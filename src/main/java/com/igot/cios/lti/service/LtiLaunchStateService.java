package com.igot.cios.lti.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cios.lti.LtiClaims;
import com.igot.cios.lti.config.LtiPlatformProperties;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Short-lived launch state, held in Redis with a TTL.
 *
 * <p>Redis rather than a table: this data is high-churn and self-expiring, so a
 * relational row would only create a cleanup job.
 *
 * <p>Direction of the values is easy to invert:
 * <ul>
 *   <li>lti_message_hint: WE generate it in leg 1; Moodle echoes it back in leg
 *       2. It is how we recall what was being launched.</li>
 *   <li>state and nonce: MOODLE generates these in leg 2. We echo state back
 *       and embed nonce in the id_token.</li>
 * </ul>
 *
 * <p>Both the message hint and the nonce are single-use: consuming one deletes
 * it, so a replayed request finds nothing and is rejected.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class LtiLaunchStateService {

    private final RedisTemplate<String, String> redisTemplate;
    private final ObjectMapper objectMapper;
    private final LtiPlatformProperties properties;

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class LaunchState {
        private String registrationId;
        /** Null for a Deep Linking request: nothing specific is being launched. */
        private String resourceLinkId;
        private String messageType;
        private String userId;
    }

    public void put(String messageHint, LaunchState state) {
        try {
            redisTemplate.opsForValue().set(
                    LtiClaims.REDIS_MESSAGE_HINT + messageHint,
                    objectMapper.writeValueAsString(state),
                    properties.getLaunchStateTtlSeconds(), TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new IllegalStateException("Could not persist LTI launch state", e);
        }
    }

    /** Consumes the state. Empty means unknown, expired, or already used. */
    public Optional<LaunchState> consume(String messageHint) {
        if (messageHint == null || messageHint.isBlank()) {
            return Optional.empty();
        }
        String key = LtiClaims.REDIS_MESSAGE_HINT + messageHint;
        try {
            String json = redisTemplate.opsForValue().get(key);
            if (json == null) {
                return Optional.empty();
            }
            redisTemplate.delete(key);
            return Optional.of(objectMapper.readValue(json, LaunchState.class));
        } catch (Exception e) {
            log.error("Could not read LTI launch state: {}", e.getMessage());
            return Optional.empty();
        }
    }

    /** @return true if the nonce is fresh; false if it has been seen before. */
    public boolean claimNonce(String nonce) {
        if (nonce == null || nonce.isBlank()) {
            return false;
        }
        Boolean fresh = redisTemplate.opsForValue().setIfAbsent(
                LtiClaims.REDIS_NONCE + nonce, "1",
                properties.getLaunchStateTtlSeconds(), TimeUnit.SECONDS);
        return Boolean.TRUE.equals(fresh);
    }

    public void putDeepLinkData(String data) {
        redisTemplate.opsForValue().set(LtiClaims.REDIS_DL_DATA + data, "1",
                properties.getLaunchStateTtlSeconds(), TimeUnit.SECONDS);
    }

    /** @return true if this data value was one we issued and had not yet been consumed. */
    public boolean consumeDeepLinkData(String data) {
        if (data == null || data.isBlank()) {
            return false;
        }
        String key = LtiClaims.REDIS_DL_DATA + data;
        String found = redisTemplate.opsForValue().get(key);
        if (found == null) {
            return false;
        }
        redisTemplate.delete(key);
        return true;
    }
}
