package com.igot.cios.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ResponseStatusException;

@Component
@Slf4j
public class KafkaProducer {
    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;
    @Autowired
    private ObjectMapper objectMapper;

    public void push(String topic, Object data) {
        try {
            String message = objectMapper.writeValueAsString(data);
            log.info("KafkaProducer::sendCornellData: topic: {}", topic);
            this.kafkaTemplate.send(topic, message);
            log.info("Data sent to kafka topic {} and message is {}", topic, message);
        } catch (Exception e) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, e.getMessage());
        }

    }

}
