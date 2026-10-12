package com.igot.cios.lti.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.igot.cios.kafka.KafkaProducer;
import com.igot.cios.lti.entity.LtiProviderRegistrationEntity;
import com.igot.cios.lti.entity.LtiResourceLinkEntity;
import com.igot.cios.util.CbServerProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * Normalises an AGS score into the existing internal progress schema and
 * publishes it to the Kafka topic the platform already consumes.
 *
 * <p>This is the seam that lets LTI reuse everything downstream. The payload
 * shape matches PayloadValidation/ProgressFileValidation.json, and the topic is
 * the same spring.kafka.cornell.topic.name that the Cornell, Coursera-batch,
 * CDAC and Harvard integrations publish to. From there
 * cb_external_enrollment_service's enrollUpdateConsumer writes completion
 * status and triggers certificate generation, with no changes required there.
 *
 * <p>LTI replaces the scheduled pull and the JOLT transform, not the contract.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class LtiScorePublisher {

    private static final DateTimeFormatter COMPLETED_ON_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.systemDefault());

    /** Closed enum from the AGS spec; only this value means the activity is done. */
    private static final String ACTIVITY_COMPLETED = "Completed";

    private final KafkaProducer kafkaProducer;
    private final CbServerProperties cbServerProperties;
    private final ObjectMapper objectMapper;

    /**
     * Publishes a completion event derived from an AGS score. Only fully
     * completed activities are published; partial progress has no
     * representation in the downstream schema, which models completion rather
     * than a percentage.
     *
     * @return true if an event was published, false if the score was ignored
     */
    public boolean publishIfComplete(LtiResourceLinkEntity link,
                                     LtiProviderRegistrationEntity registration,
                                     String userId,
                                     Double scoreGiven,
                                     Double scoreMaximum,
                                     String activityProgress,
                                     Instant timestamp) {

        if (!ACTIVITY_COMPLETED.equalsIgnoreCase(activityProgress)) {
            log.debug("LTI score for user={} ignored, activityProgress={}", userId, activityProgress);
            return false;
        }

        ObjectNode payload = objectMapper.createObjectNode();
        payload.put("userid", userId);
        payload.put("courseid", link.getExternalId());
        payload.put("completedon",
                COMPLETED_ON_FORMAT.format(timestamp == null ? Instant.now() : timestamp));
        payload.put("status", ACTIVITY_COMPLETED);
        payload.put("partnerCode", registration.getPartnerCode());
        payload.put("partnerId", registration.getPartnerId());
        if (scoreGiven != null) {
            payload.put("scoreGiven", scoreGiven);
        }
        if (scoreMaximum != null) {
            payload.put("scoreMaximum", scoreMaximum);
        }

        kafkaProducer.push(cbServerProperties.getTopic(), payload);
        log.info("LTI completion published to topic={} for user={} course={} partner={}",
                cbServerProperties.getTopic(), userId, link.getExternalId(),
                registration.getPartnerCode());
        return true;
    }
}
