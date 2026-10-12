package com.igot.cios.lti.controller;

import com.igot.cios.lti.config.LtiPlatformProperties;
import com.igot.cios.lti.entity.LtiPlatformKeyEntity;
import com.igot.cios.lti.entity.LtiProviderRegistrationEntity;
import com.igot.cios.lti.entity.LtiResourceLinkEntity;
import com.igot.cios.lti.repository.LtiProviderRegistrationRepository;
import com.igot.cios.lti.repository.LtiResourceLinkRepository;
import com.igot.cios.lti.service.LtiPlatformKeyService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Administrative endpoints: provider registry, resource links, key lifecycle,
 * and the platform configuration block an operator copies into Moodle.
 *
 * <p>The registry lives here for now so the module is self-contained. The HLD
 * places lti_provider_registration in cb-pores-service beside
 * ContentPartnerEntity; moving it later only changes the repository
 * implementation, not these endpoints.
 *
 * <p><b>Pending:</b> these endpoints carry no authorisation. Gate them behind
 * an admin role before any shared environment.
 */
@RestController
@RequestMapping("/ciosIntegration/v1/lti/admin")
@RequiredArgsConstructor
public class LtiAdminController {

    private final LtiProviderRegistrationRepository registrationRepository;
    private final LtiResourceLinkRepository resourceLinkRepository;
    private final LtiPlatformKeyService keyService;
    private final LtiPlatformProperties properties;

    /** The values an operator pastes into Moodle. All four are global; only clientId differs. */
    @GetMapping("/platform-config")
    public Map<String, Object> platformConfig() {
        return Map.of(
                "issuer", properties.getIssuer(),
                "authenticationRequestUrl", properties.authorizeUrl(),
                "accessTokenUrl", properties.tokenUrl(),
                "publicKeysetUrl", properties.jwksUrl(),
                "activeKid", keyService.activeKid());
    }

    @PostMapping("/providers")
    public ResponseEntity<Map<String, Object>> create(@RequestBody LtiProviderRegistrationEntity request) {
        Timestamp now = Timestamp.from(Instant.now());

        request.setId(UUID.randomUUID().toString());
        if (request.getClientId() == null || request.getClientId().isBlank()) {
            request.setClientId("igot-" + UUID.randomUUID().toString().substring(0, 8));
        }
        if (request.getDeploymentId() == null || request.getDeploymentId().isBlank()) {
            request.setDeploymentId("1");
        }
        if (request.getEnvironment() == null) {
            request.setEnvironment("sandbox");
        }
        request.setStatus("active");
        request.setIsActive(true);
        request.setCreatedDate(now);
        request.setUpdatedDate(now);

        LtiProviderRegistrationEntity saved = registrationRepository.save(request);

        return ResponseEntity.ok(Map.of(
                "registration", saved,
                "platformConfigForProvider", platformConfig()));
    }

    @GetMapping("/providers")
    public List<LtiProviderRegistrationEntity> list() {
        return registrationRepository.findAll();
    }

    @GetMapping("/providers/{id}")
    public ResponseEntity<LtiProviderRegistrationEntity> read(@PathVariable String id) {
        return registrationRepository.findById(id)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @DeleteMapping("/providers/{id}")
    public ResponseEntity<Void> delete(@PathVariable String id) {
        registrationRepository.deleteById(id);
        return ResponseEntity.noContent().build();
    }

    /** Manual catalog entry: the fallback when a provider exposes no content picker. */
    @PostMapping("/resource-links")
    public LtiResourceLinkEntity createResourceLink(@RequestBody LtiResourceLinkEntity request) {
        Timestamp now = Timestamp.from(Instant.now());
        request.setId(UUID.randomUUID().toString());
        request.setCreatedDate(now);
        request.setUpdatedDate(now);
        return resourceLinkRepository.save(request);
    }

    @GetMapping("/resource-links")
    public List<LtiResourceLinkEntity> listResourceLinks() {
        return resourceLinkRepository.findAll();
    }

    @DeleteMapping("/resource-links/{id}")
    public ResponseEntity<Void> deleteResourceLink(@PathVariable String id) {
        resourceLinkRepository.deleteById(id);
        return ResponseEntity.noContent().build();
    }

    @GetMapping("/keys")
    public List<Map<String, Object>> keys() {
        return keyService.all().stream().map(k -> Map.<String, Object>of(
                "kid", k.getKid(),
                "algorithm", k.getAlgorithm(),
                "status", k.getStatus(),
                "createdDate", String.valueOf(k.getCreatedDate()))).toList();
    }

    @PostMapping("/keys/rotate")
    public Map<String, Object> rotate() {
        LtiPlatformKeyEntity fresh = keyService.rotate();
        return Map.of(
                "activeKid", fresh.getKid(),
                "note", "The previous key is now 'rotating' and still published in the JWKS "
                        + "so tokens signed before the switch continue to verify.");
    }
}
