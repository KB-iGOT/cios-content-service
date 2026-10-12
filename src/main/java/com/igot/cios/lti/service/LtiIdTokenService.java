package com.igot.cios.lti.service;

import com.igot.cios.lti.LtiClaims;
import com.igot.cios.lti.config.LtiPlatformProperties;
import com.igot.cios.lti.entity.LtiProviderRegistrationEntity;
import com.igot.cios.lti.entity.LtiResourceLinkEntity;
import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JOSEObjectType;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * Builds and signs the id_token: the Platform's core responsibility.
 *
 * <p>Every claim URI here is spec-fixed, from LtiClaims. The token is signed
 * RS256 with iGOT's platform key; Moodle verifies it against our published
 * JWKS.
 */
@Service
@RequiredArgsConstructor
public class LtiIdTokenService {

    private final LtiPlatformKeyService keyService;
    private final LtiPlatformProperties properties;

    /** A resource-link launch: a learner is opening a specific piece of content. */
    public String resourceLinkToken(LtiProviderRegistrationEntity reg,
                                    LtiResourceLinkEntity link,
                                    String userId,
                                    String nonce) throws JOSEException {

        String contextId = "ctx-" + link.getId();
        String agsBase = properties.agsContextBase(contextId);

        JWTClaimsSet claims = baseClaims(reg, userId, nonce)
                .claim(LtiClaims.MESSAGE_TYPE, LtiClaims.MSG_RESOURCE_LINK_REQUEST)
                .claim(LtiClaims.TARGET_LINK_URI, link.getTargetLinkUri())
                .claim(LtiClaims.RESOURCE_LINK, Map.of(
                        "id", link.getId(),
                        "title", nullSafe(link.getTitle())))
                .claim(LtiClaims.CONTEXT, Map.of(
                        "id", contextId,
                        "label", nullSafe(link.getTitle()),
                        "type", List.of("CourseOffering")))
                .claim(LtiClaims.AGS_ENDPOINT, Map.of(
                        "scope", List.of(LtiClaims.SCOPE_LINEITEM, LtiClaims.SCOPE_SCORE),
                        "lineitems", agsBase + "/lineitems",
                        "lineitem", agsBase + "/lineitems/" + link.getId()))
                .build();

        return sign(claims);
    }

    /** A Deep Linking request: an author is opening the tool's content picker. */
    public String deepLinkingToken(LtiProviderRegistrationEntity reg,
                                   String userId,
                                   String nonce,
                                   String dataToken) throws JOSEException {

        JWTClaimsSet claims = baseClaims(reg, userId, nonce)
                .claim(LtiClaims.MESSAGE_TYPE, LtiClaims.MSG_DEEP_LINKING_REQUEST)
                .claim(LtiClaims.TARGET_LINK_URI, reg.getDeepLinkingUri())
                .claim(LtiClaims.DL_SETTINGS, Map.of(
                        "deep_link_return_url", properties.deepLinkReturnUrl(),
                        "accept_types", List.of("ltiResourceLink"),
                        "accept_presentation_document_targets", List.of("iframe", "window"),
                        "accept_multiple", true,
                        "auto_create", false,
                        "data", dataToken))
                .build();

        return sign(claims);
    }

    private JWTClaimsSet.Builder baseClaims(LtiProviderRegistrationEntity reg, String userId, String nonce) {
        Instant now = Instant.now();
        return new JWTClaimsSet.Builder()
                .issuer(properties.getIssuer())
                .audience(reg.getClientId())
                .subject(userId)
                .issueTime(Date.from(now))
                .expirationTime(Date.from(now.plusSeconds(properties.getIdTokenTtlSeconds())))
                .claim("nonce", nonce)
                .claim("azp", reg.getClientId())
                .claim(LtiClaims.VERSION, LtiClaims.LTI_VERSION_1_3)
                .claim(LtiClaims.DEPLOYMENT_ID, reg.getDeploymentId())
                .claim(LtiClaims.ROLES, List.of(LtiClaims.ROLE_LEARNER))
                .claim(LtiClaims.LAUNCH_PRESENTATION, Map.of(
                        "document_target", "iframe",
                        "return_url", properties.getIssuer()));
    }

    private String sign(JWTClaimsSet claims) throws JOSEException {
        JWSHeader header = new JWSHeader.Builder(JWSAlgorithm.RS256)
                .keyID(keyService.activeKid())
                .type(JOSEObjectType.JWT)
                .build();

        SignedJWT jwt = new SignedJWT(header, claims);
        jwt.sign(new RSASSASigner(keyService.activeSigningKey().toPrivateKey()));
        return jwt.serialize();
    }

    private static String nullSafe(String s) {
        return s == null ? "" : s;
    }
}
