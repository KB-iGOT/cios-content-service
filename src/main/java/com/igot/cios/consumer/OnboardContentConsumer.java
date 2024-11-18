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
import org.springframework.util.StopWatch;

import java.io.File;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
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
        Map<String, List<Map<String, String>>> fileDataMap = new HashMap<>();
        String partnerCode = null;
        String partnerId = null;
        String fileName = null;
        Timestamp initiatedOn = null;
        String fileId = null;
        String loadContentErrorMessage = null;
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        try {
            log.info("Consuming the content to onboard in cios");
            Map<String, Object> receivedMessage = objectMapper.readValue(message, new TypeReference<Map<String, Object>>() {
            });
            partnerCode = (String) receivedMessage.get(Constants.PARTNER_CODE);
            fileName = (String) receivedMessage.get(Constants.FILE_NAME);
            initiatedOn = objectMapper.convertValue(receivedMessage.get(Constants.INITIATED_ON), Timestamp.class);
            fileId = (String) receivedMessage.get(Constants.FILE_ID);
            partnerId = (String) receivedMessage.get(Constants.PARTNER_ID);
            boolean isLastBatch = (boolean) receivedMessage.getOrDefault("isLastBatch", false);

            log.info("Received {} records from Kafka", receivedMessage.size());
            List<Map<String, String>> processedData = objectMapper.convertValue(receivedMessage.get("data"), new TypeReference<List<Map<String, String>>>() {
            });

            List<Map<String, String>> accumulatedData = fileDataMap.getOrDefault(fileId, new ArrayList<>());
            accumulatedData.addAll(processedData);
            fileDataMap.put(fileId, accumulatedData);

            if (isLastBatch) {
                List<Map<String, String>> finalData = fileDataMap.get(fileId);
                if (finalData == null || finalData.isEmpty()) {
                    log.error("No accumulated data found for fileId: {}. Throwing exception to trigger error handling.", fileId);
                    throw new IllegalStateException("No accumulated data found for fileId: " + fileId);
                }
                Map<String, Object> result = ciosContentServiceimpl.processRowsAndCreateLogs(
                        finalData, fileId, fileName, partnerCode, null);

                List<Map<String, String>> successProcessedData = (List<Map<String, String>>) result.get("successProcessedData");
                File logFile = (File) result.get("logFile");
                boolean hasFailures = (boolean) result.get("hasFailures");

                //processing the successful data for saving in db
                if (successProcessedData != null && !successProcessedData.isEmpty()) {
                    processReceivedData(partnerCode, successProcessedData, fileName, fileId, initiatedOn, partnerId);
                } else {
                    log.info("No successful data to process for partner: {}", partnerCode);
                }
                // Uploading logs to GCP
                ciosContentServiceimpl.uploadLogFileToGCP(logFile, partnerId, fileId, fileName, initiatedOn, hasFailures);
                log.info("Log file  successful uploaded to GCP for partner: {}", partnerCode);
                fileDataMap.remove(fileId);
            }
        } catch (Exception e) {
            loadContentErrorMessage = "Error in processReceivedData: " + e.getMessage();
            log.error(loadContentErrorMessage, e);
            try {
                Map<String, Object> errorResult = ciosContentServiceimpl.processRowsAndCreateLogs(
                        null, fileId, fileName, partnerCode, loadContentErrorMessage);

                File errorLogFile = (File) errorResult.get("logFile");
                boolean hasFailures = true;
                ciosContentServiceimpl.uploadLogFileToGCP(errorLogFile, partnerId, fileId, fileName, initiatedOn, hasFailures);
                log.info("Log file uploaded to GCP with failure status for partner: {}", partnerCode);
            } catch (Exception logException) {
                log.error("Error while generating or uploading error logs for partner: {}", partnerCode, logException);
            }
        } finally {
            stopWatch.stop(); // Stop the stopwatch
            log.info("Total time taken to process the message: {} ms", stopWatch.getTotalTimeMillis());
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
