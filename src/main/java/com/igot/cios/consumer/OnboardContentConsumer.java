package com.igot.cios.consumer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cios.exception.CiosContentException;
import com.igot.cios.plugins.DataTransformUtility;
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
    private CiosContentServiceImpl ciosContentServiceimpl;

    @KafkaListener(topics = "${kafka.topic.content.onboarding}", groupId = "${content.onboarding.consumer.group}")
    public void consumeMessage(String message) {
        String partnerCode = null;
        String partnerId = null;
        String fileName = null;
        Timestamp initiatedOn = null;
        String fileId = null;
        String loadContentErrorMessage = null;
        try {
            log.info("Consuming the content to onboard in cios");
            Map<String, Object> receivedMessage = objectMapper.readValue(message, new TypeReference<Map<String, Object>>() {});

            partnerCode = (String) receivedMessage.get(Constants.PARTNER_CODE);
            fileName = (String) receivedMessage.get(Constants.FILE_NAME);
            initiatedOn = objectMapper.convertValue(receivedMessage.get(Constants.INITIATED_ON), Timestamp.class);
            fileId = (String) receivedMessage.get(Constants.FILE_ID);
            partnerId = (String) receivedMessage.get(Constants.PARTNER_ID);

            log.info("Received {} records from Kafka", receivedMessage.size());
            List<Map<String, String>> processedData = objectMapper.convertValue(receivedMessage.get("data"), new TypeReference<List<Map<String, String>>>() {});
            try {
                //ciosContentServiceimpl.processRowsAndCreateLogs(processedData, partnerId, fileId, fileName, initiatedOn,partnerCode, loadContentErrorMessage);
                processReceivedData(partnerCode, processedData, fileName, fileId, initiatedOn,partnerId);
            }catch (Exception e){
                loadContentErrorMessage = "Error in processReceivedData: " + e.getMessage();
                log.error(loadContentErrorMessage);
            }
            ciosContentServiceimpl.processRowsAndCreateLogs(processedData, partnerId, fileId, fileName, initiatedOn,partnerCode, loadContentErrorMessage);
            log.info("Data successfully processed for partner: {}", partnerCode);
        } catch (Exception e) {
            dataTransformUtility.createFileInfo(partnerId, fileId, fileName, initiatedOn, new Timestamp(System.currentTimeMillis()), Constants.CONTENT_UPLOAD_FAILED, null);
            log.error("Error while consuming message from Kafka", e);
        }
    }


    private void processReceivedData(String partnerCode, List<Map<String, String>> processedData, String fileName, String fileId, Timestamp initiatedOn,String partnerId) throws IOException {
        log.info("Processing {} records for partner code {}", processedData.size(), partnerCode);
        JsonNode jsonData = objectMapper.valueToTree(processedData);

        JsonNode entity = dataTransformUtility.fetchPartnerInfoUsingApi(partnerCode);
        List<Object> contentJson = objectMapper.convertValue(entity.path("result").path("trasformContentJson"), new TypeReference<List<Object>>() {
        });
        if(contentJson == null || contentJson.isEmpty()){
            log.error("trasformContentJson is missing, please update in contentPartner");
            throw new CiosContentException("ERROR","trasformContentJson is missing, please update in contentPartner", HttpStatus.INTERNAL_SERVER_ERROR);
        }
        dataTransformUtility.updateProcessedDataInDb(jsonData, partnerCode, fileName, fileId, contentJson,partnerId);
    }

}
