package com.igot.cios.consumer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cios.exception.CiosContentException;
import com.igot.cios.plugins.ContentPartnerPluginService;
import com.igot.cios.plugins.ContentSource;
import com.igot.cios.plugins.DataTransformUtility;
import com.igot.cios.plugins.config.ContentPartnerServiceFactory;
import com.igot.cios.service.impl.CiosContentServiceImpl;
import com.igot.cios.util.Constants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
@Component
@Slf4j
public class OnboardContentConsumer {
    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    DataTransformUtility dataTransformUtility;

    @Autowired
    private ContentPartnerServiceFactory contentPartnerServiceFactory;

    @Autowired
    private CiosContentServiceImpl ciosContentServiceimpl;

    @KafkaListener(topics = "${kafka.topic.content.onboarding}", groupId = "enrollment-progress")
    public void consumeMessage(String message) {
        try {
            log.info("Consuming the content to onboard in cios");
            Map<String, Object> receivedMessage = objectMapper.readValue(message, new TypeReference<Map<String, Object>>() {});

            String partnerCode = (String) receivedMessage.get(Constants.PARTNER_CODE);
            String fileName = (String) receivedMessage.get(Constants.FILE_NAME);
            Timestamp initiatedOn = objectMapper.convertValue(receivedMessage.get(Constants.INITIATED_ON), Timestamp.class);
            String fileId = (String) receivedMessage.get(Constants.FILE_ID);

            log.info("Received {} records from Kafka", receivedMessage.size());
            List<Map<String, String>> processedData = objectMapper.convertValue(receivedMessage.get("data"), new TypeReference<List<Map<String, String>>>() {});
            processReceivedData(partnerCode, processedData, fileName, fileId, initiatedOn);
        } catch (Exception e) {
            log.error("Error while consuming message from Kafka", e);
        }
    }


    private void processReceivedData(String partnerCode, List<Map<String, String>> processedData, String fileName, String fileId, Timestamp initiatedOn) throws IOException {
            log.info("Processing {} records for partner code {}", processedData.size(), partnerCode);
            JsonNode jsonData = objectMapper.valueToTree(processedData);

            JsonNode entity = dataTransformUtility.fetchPartnerInfoUsingApi(partnerCode);
            List<Object> contentJson = objectMapper.convertValue(entity.path("result").path("trasformContentJson"), new TypeReference<List<Object>>() {
            });
            if (contentJson == null || contentJson.isEmpty()) {
                throw new CiosContentException("Transformation data not present in content partner db", HttpStatus.INTERNAL_SERVER_ERROR);
            }
            ContentPartnerPluginService service = contentPartnerServiceFactory.getContentPartnerPluginService(ContentSource.fromPartnerCode(partnerCode));
            service.loadContentFromExcel(jsonData, partnerCode, fileName, fileId, contentJson);
            ciosContentServiceimpl.processRowsAndCreateLogs(processedData, entity, fileId, fileName, initiatedOn,partnerCode);
            log.info("Data successfully processed for partner: {}", partnerCode);
        }

}
