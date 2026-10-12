package com.igot.cios.lti.service;

import com.igot.cios.lti.LtiClaims;
import com.igot.cios.lti.config.LtiPlatformProperties;
import com.igot.cios.lti.entity.LtiProviderRegistrationEntity;
import com.igot.cios.lti.repository.LtiProviderRegistrationRepository;
import com.nimbusds.jose.crypto.RSASSAVerifier;
import com.nimbusds.jose.jwk.JWK;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Service-to-service authorisation for AGS calls.
 *
 * <p>Mirror image of the launch: there WE sign and Moodle verifies. Here
 * MOODLE signs an assertion with its own key (private_key_jwt) and we verify
 * it against the jwksUri on its registration.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class LtiAccessTokenService {

    private final LtiProviderRegistrationRepository registrationRepository;
    private final LtiPlatformProperties properties;
    private final RedisTemplate<String, String> redisTemplate;

    private final HttpClient httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10)).build();

    public static class TokenException extends RuntimeException {
        public TokenException(String message) {
            super(message);
        }
    }

    /** Validates a client assertion and issues a scoped access token. */
    public Map<String, Object> issue(String clientAssertion, String requestedScope) {
        SignedJWT jwt;
        JWTClaimsSet claims;
        try {
            jwt = SignedJWT.parse(clientAssertion);
            claims = jwt.getJWTClaimsSet();
        } catch (Exception e) {
            throw new TokenException("client_assertion is not a parseable JWT");
        }

        String iss = claims.getIssuer();
        String sub = claims.getSubject();

        if (sub == null || !sub.equals(iss)) {
            throw new TokenException("client_assertion iss and sub must match");
        }

        LtiProviderRegistrationEntity reg = registrationRepository.findByClientId(sub)
                .orElseThrow(() -> new TokenException("Unknown client_id: " + sub));

        var audiences = claims.getAudience();
        boolean audOk = audiences != null && audiences.stream().anyMatch(a ->
                a.equals(properties.tokenUrl()) || a.equals(properties.getIssuer()));
        if (!audOk) {
            throw new TokenException("client_assertion aud does not name this token endpoint");
        }

        var exp = claims.getExpirationTime();
        if (exp == null || exp.toInstant().isBefore(java.time.Instant.now())) {
            throw new TokenException("client_assertion has expired");
        }

        String jti = claims.getJWTID();
        if (jti != null) {
            Boolean fresh = redisTemplate.opsForValue().setIfAbsent(
                    "lti:jti:" + jti, "1", properties.getLaunchStateTtlSeconds(), TimeUnit.SECONDS);
            if (!Boolean.TRUE.equals(fresh)) {
                throw new TokenException("client_assertion jti replayed");
            }
        }

        if (!verifySignature(jwt, reg)) {
            throw new TokenException("client_assertion signature did not verify against the tool's JWKS");
        }

        String token = UUID.randomUUID().toString().replace("-", "");
        String scope = requestedScope == null ? "" : requestedScope;
        redisTemplate.opsForValue().set(
                LtiClaims.REDIS_ACCESS_TOKEN + token,
                reg.getClientId() + "|" + scope,
                properties.getAccessTokenTtlSeconds(), TimeUnit.SECONDS);

        log.info("LTI access token issued to clientId={} scope={}", reg.getClientId(), scope);

        return Map.of(
                "access_token", token,
                "token_type", "Bearer",
                "expires_in", properties.getAccessTokenTtlSeconds(),
                "scope", scope);
    }

    /** Validates a Bearer token presented on an AGS call. */
    public Optional<String> validate(String authorizationHeader) {
        if (authorizationHeader == null) {
            return Optional.empty();
        }
        String token = authorizationHeader.startsWith("Bearer ")
                ? authorizationHeader.substring(7).trim()
                : authorizationHeader.trim();
        String value = redisTemplate.opsForValue().get(LtiClaims.REDIS_ACCESS_TOKEN + token);
        return Optional.ofNullable(value);
    }

    /** Verifies a JWT Moodle signed, against its registered JWKS. */
    public boolean verifySignature(SignedJWT jwt, LtiProviderRegistrationEntity reg) {
        try {
            JWKSet set = toolJwks(reg.getJwksUri(), false);
            String kid = jwt.getHeader().getKeyID();

            JWK jwk = selectKey(set, kid);
            if (jwk == null) {
                set = toolJwks(reg.getJwksUri(), true);
                jwk = selectKey(set, kid);
            }
            if (!(jwk instanceof RSAKey rsa)) {
                log.warn("No usable RSA key for kid={} at {}", kid, reg.getJwksUri());
                return false;
            }
            return jwt.verify(new RSASSAVerifier(rsa.toRSAPublicKey()));
        } catch (Exception e) {
            log.warn("Could not verify tool signature: {}", e.toString());
            return false;
        }
    }

    private JWK selectKey(JWKSet set, String kid) {
        if (kid != null) {
            return set.getKeyByKeyId(kid);
        }
        return set.getKeys().isEmpty() ? null : set.getKeys().get(0);
    }

    /**
     * Fetches the tool's JWK Set, cached in Redis. A failure here against a
     * local or UAT Moodle is usually TLS trust or a blocked outbound host,
     * rather than a problem with the key set itself.
     */
    private JWKSet toolJwks(String jwksUri, boolean forceRefresh) throws Exception {
        String cacheKey = "lti:jwks:" + jwksUri;
        if (!forceRefresh) {
            String cached = redisTemplate.opsForValue().get(cacheKey);
            if (cached != null) {
                return JWKSet.parse(cached);
            }
        }
        HttpRequest request = HttpRequest.newBuilder(URI.create(jwksUri))
                .timeout(Duration.ofSeconds(10)).GET().build();
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200) {
            throw new IllegalStateException("JWKS fetch returned HTTP " + response.statusCode()
                    + " for " + jwksUri);
        }
        redisTemplate.opsForValue().set(cacheKey, response.body(), 1, TimeUnit.HOURS);
        return JWKSet.parse(response.body());
    }
}
