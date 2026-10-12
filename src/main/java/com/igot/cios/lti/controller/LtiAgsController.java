package com.igot.cios.lti.controller;

import com.igot.cios.lti.LtiClaims;
import com.igot.cios.lti.config.LtiPlatformProperties;
import com.igot.cios.lti.entity.LtiProviderRegistrationEntity;
import com.igot.cios.lti.entity.LtiResourceLinkEntity;
import com.igot.cios.lti.repository.LtiProviderRegistrationRepository;
import com.igot.cios.lti.repository.LtiResourceLinkRepository;
import com.igot.cios.lti.service.LtiAccessTokenService;
import com.igot.cios.lti.service.LtiScorePublisher;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Assignment and Grade Services: the Platform hosts these, Moodle calls them.
 *
 * <p>Media types are part of the contract, not decoration. Note the version
 * skew: score is v1, line items are v2 - correct per spec.
 *
 * <p>Scope note: line items are not a table. Each resource link has one
 * implicit gradebook column, "course completion", which is what the downstream
 * progress schema represents. Add a line-item table only if a provider needs
 * several graded columns per course.
 */
@Slf4j
@RestController
@RequestMapping("/ciosIntegration/v1/lti/ags/contexts/{contextId}")
@RequiredArgsConstructor
public class LtiAgsController {

    private final LtiAccessTokenService tokenService;
    private final LtiResourceLinkRepository resourceLinkRepository;
    private final LtiProviderRegistrationRepository registrationRepository;
    private final LtiScorePublisher scorePublisher;
    private final LtiPlatformProperties properties;
    private final RedisTemplate<String, String> redisTemplate;

    @GetMapping(value = "/lineitems", produces = LtiClaims.MT_LINEITEM_LIST)
    public ResponseEntity<?> listLineItems(
            @PathVariable String contextId,
            @RequestHeader(value = "Authorization", required = false) String authorization) {

        if (tokenService.validate(authorization).isEmpty()) {
            return unauthorized();
        }
        String linkId = resourceLinkIdFrom(contextId);
        return resourceLinkRepository.findById(linkId)
                .<ResponseEntity<?>>map(link -> ResponseEntity.ok(List.of(lineItemJson(contextId, link))))
                .orElseGet(() -> ResponseEntity.ok(List.of()));
    }

    @GetMapping(value = "/lineitems/{lineItemId}", produces = LtiClaims.MT_LINEITEM)
    public ResponseEntity<?> getLineItem(
            @PathVariable String contextId,
            @PathVariable String lineItemId,
            @RequestHeader(value = "Authorization", required = false) String authorization) {

        if (tokenService.validate(authorization).isEmpty()) {
            return unauthorized();
        }
        return resourceLinkRepository.findById(lineItemId)
                .<ResponseEntity<?>>map(link -> ResponseEntity.ok(lineItemJson(contextId, link)))
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    /**
     * Where completion arrives. Normalises the AGS payload and publishes it to
     * the existing progress topic, which cb_external_enrollment_service
     * already consumes.
     */
    @PostMapping(value = "/lineitems/{lineItemId}/scores", consumes = LtiClaims.MT_SCORE)
    public ResponseEntity<?> postScore(
            @PathVariable String contextId,
            @PathVariable String lineItemId,
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @RequestBody Map<String, Object> body) {

        if (tokenService.validate(authorization).isEmpty()) {
            return unauthorized();
        }

        String userId = asString(body.get("userId"));
        if (userId == null || userId.isBlank()) {
            return ResponseEntity.badRequest().body(Map.of("error", "userId is required"));
        }

        Instant timestamp;
        try {
            String raw = asString(body.get("timestamp"));
            timestamp = raw == null ? Instant.now() : Instant.parse(raw);
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(Map.of("error", "timestamp must be ISO-8601"));
        }

        // Spec rule: reject a score older than one already accepted. Moodle's
        // timestamp drives this, not the server clock.
        String seenKey = "lti:score:" + lineItemId + ":" + userId;
        String previous = redisTemplate.opsForValue().get(seenKey);
        if (previous != null && Instant.parse(previous).isAfter(timestamp)) {
            log.warn("Rejecting out-of-order AGS score user={} incoming={} stored={}",
                    userId, timestamp, previous);
            return ResponseEntity.status(HttpStatus.CONFLICT)
                    .body(Map.of("error", "A newer score already exists for this user"));
        }

        LtiResourceLinkEntity link = resourceLinkRepository.findById(lineItemId).orElse(null);
        if (link == null) {
            return ResponseEntity.notFound().build();
        }
        LtiProviderRegistrationEntity registration =
                registrationRepository.findById(link.getRegistrationId()).orElse(null);
        if (registration == null) {
            return ResponseEntity.status(HttpStatus.CONFLICT)
                    .body(Map.of("error", "Registration for this line item no longer exists"));
        }

        scorePublisher.publishIfComplete(
                link, registration, userId,
                asDouble(body.get("scoreGiven")),
                asDouble(body.get("scoreMaximum")),
                asString(body.get("activityProgress")),
                timestamp);

        redisTemplate.opsForValue().set(seenKey, timestamp.toString(), 90, TimeUnit.DAYS);

        return ResponseEntity.noContent().build();
    }

    private Map<String, Object> lineItemJson(String contextId, LtiResourceLinkEntity link) {
        return Map.of(
                "id", properties.agsContextBase(contextId) + "/lineitems/" + link.getId(),
                "label", link.getTitle() == null ? "Course completion" : link.getTitle(),
                "scoreMaximum", link.getScoreMaximum() == null ? 100d : link.getScoreMaximum(),
                "resourceId", link.getExternalId() == null ? "" : link.getExternalId());
    }

    private String resourceLinkIdFrom(String contextId) {
        return contextId != null && contextId.startsWith("ctx-")
                ? contextId.substring(4)
                : contextId;
    }

    private ResponseEntity<?> unauthorized() {
        return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(Map.of(
                "error", "invalid_token",
                "error_description",
                "Missing or invalid Bearer token. Obtain one from the LTI token endpoint first."));
    }

    private static String asString(Object value) {
        return value == null ? null : String.valueOf(value);
    }

    private static Double asDouble(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Number number) {
            return number.doubleValue();
        }
        try {
            return Double.valueOf(String.valueOf(value));
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
