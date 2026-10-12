package com.igot.cios.lti.service;

import com.igot.cios.lti.entity.LtiPlatformKeyEntity;
import com.igot.cios.lti.repository.LtiPlatformKeyRepository;
import com.nimbusds.jose.jwk.JWK;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jose.jwk.gen.RSAKeyGenerator;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Manages iGOT's platform signing keys.
 *
 * <p>One active key signs all id_tokens regardless of how many providers are
 * registered. Rotation publishes the incoming key alongside the outgoing one so
 * tokens already in flight continue to verify.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class LtiPlatformKeyService {

    public static final String STATUS_ACTIVE = "active";
    public static final String STATUS_ROTATING = "rotating";
    public static final String STATUS_RETIRED = "retired";

    private final LtiPlatformKeyRepository keyRepository;

    /** Generates the first key on startup if none exists, so the service is usable immediately. */
    @PostConstruct
    void ensureActiveKey() {
        try {
            if (keyRepository.findFirstByStatus(STATUS_ACTIVE).isEmpty()) {
                LtiPlatformKeyEntity key = generate(STATUS_ACTIVE);
                log.info("LtiPlatformKeyService: generated initial signing key, kid={}", key.getKid());
            }
        } catch (Exception e) {
            // Do not block application startup; LTI endpoints report the
            // problem when first used.
            log.error("LtiPlatformKeyService: could not initialise signing key: {}", e.getMessage());
        }
    }

    public RSAKey activeSigningKey() {
        LtiPlatformKeyEntity entity = keyRepository.findFirstByStatus(STATUS_ACTIVE)
                .orElseThrow(() -> new IllegalStateException(
                        "No active LTI platform signing key. Generate one before launching."));
        try {
            return RSAKey.parse(entity.getPrivateKeyJwk());
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Stored LTI signing key could not be parsed, kid=" + entity.getKid(), e);
        }
    }

    public String activeKid() {
        return keyRepository.findFirstByStatus(STATUS_ACTIVE)
                .map(LtiPlatformKeyEntity::getKid)
                .orElseThrow(() -> new IllegalStateException("No active LTI platform signing key"));
    }

    /** Public JWK Set served at the well-known endpoint. Private members are stripped. */
    public Map<String, Object> publicJwks() {
        List<JWK> keys = new ArrayList<>();
        for (LtiPlatformKeyEntity e : keyRepository.findByStatusIn(List.of(STATUS_ACTIVE, STATUS_ROTATING))) {
            try {
                keys.add(RSAKey.parse(e.getPublicKeyJwk()));
            } catch (Exception ex) {
                log.error("Skipping unparseable public key kid={}: {}", e.getKid(), ex.getMessage());
            }
        }
        return new JWKSet(keys).toJSONObject();
    }

    /**
     * Rotates the signing key. The outgoing key moves to "rotating" rather than
     * being removed, so it stays in the JWKS and recently signed tokens still verify.
     */
    public LtiPlatformKeyEntity rotate() {
        keyRepository.findFirstByStatus(STATUS_ACTIVE).ifPresent(current -> {
            current.setStatus(STATUS_ROTATING);
            keyRepository.save(current);
            log.info("LtiPlatformKeyService: key {} moved to rotating", current.getKid());
        });
        LtiPlatformKeyEntity fresh = generate(STATUS_ACTIVE);
        log.info("LtiPlatformKeyService: new active key {}", fresh.getKid());
        return fresh;
    }

    public void retire(String kid) {
        keyRepository.findById(kid).ifPresent(k -> {
            k.setStatus(STATUS_RETIRED);
            k.setRetiredDate(Timestamp.from(Instant.now()));
            keyRepository.save(k);
        });
    }

    public List<LtiPlatformKeyEntity> all() {
        return keyRepository.findAll();
    }

    private LtiPlatformKeyEntity generate(String status) {
        try {
            RSAKey key = new RSAKeyGenerator(2048).keyID(UUID.randomUUID().toString()).generate();
            Timestamp now = Timestamp.from(Instant.now());

            LtiPlatformKeyEntity entity = new LtiPlatformKeyEntity();
            entity.setKid(key.getKeyID());
            entity.setAlgorithm("RS256");
            entity.setPublicKeyJwk(key.toPublicJWK().toJSONString());
            entity.setPrivateKeyJwk(key.toJSONString());
            entity.setStatus(status);
            entity.setCreatedDate(now);
            entity.setActivatedDate(STATUS_ACTIVE.equals(status) ? now : null);
            return keyRepository.save(entity);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to generate LTI signing key", e);
        }
    }
}
