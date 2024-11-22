package com.igot.cios.consumer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cios.entity.FileInfoEntity;
import com.igot.cios.entity.FileLogInfoEntity;
import com.igot.cios.exception.CiosContentException;
import com.igot.cios.plugins.DataTransformUtility;
import com.igot.cios.repository.FileInfoRepository;
import com.igot.cios.repository.FileLogInfoRepository;
import com.igot.cios.service.impl.CiosContentServiceImpl;
import com.igot.cios.util.Constants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.*;

@Component
@Slf4j
public class OnboardContentConsumer {
    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    DataTransformUtility dataTransformUtility;

    @Autowired
    private CiosContentServiceImpl ciosContentServiceimpl;

    @Autowired
    private FileInfoRepository fileInfoRepository;

    @Autowired
    private FileLogInfoRepository fileLogInfoRepository;

    @KafkaListener(topics = "${kafka.topic.content.onboarding}", groupId = "${content.onboarding.consumer.group}")
    public void consumeMessage(String message) {
        String partnerCode = null;
        String partnerId = null;
        String fileName = null;
        Timestamp initiatedOn = null;
        String fileId = null;
        String loadContentErrorMessage = null;
        int totalNumberOfContent = 0;
        int processedNoOfContent = 0;
        try {
            log.info("Consuming the content to onboard in cios");
            Map<String, Object> receivedMessage = objectMapper.readValue(message, new TypeReference<Map<String, Object>>() {
            });

            partnerCode = (String) receivedMessage.get(Constants.PARTNER_CODE);
            fileName = (String) receivedMessage.get(Constants.FILE_NAME);
            initiatedOn = objectMapper.convertValue(receivedMessage.get(Constants.INITIATED_ON), Timestamp.class);
            fileId = (String) receivedMessage.get(Constants.FILE_ID);
            partnerId = (String) receivedMessage.get(Constants.PARTNER_ID);

            Map<String, String> processedData = objectMapper.convertValue(receivedMessage.get("data"), new TypeReference<Map<String, String>>() {
            });
            Map<String, Object> result = ciosContentServiceimpl.processRowsAndCreateLogs(processedData, fileId, fileName, partnerCode, null);
            List<Map<String, String>> successProcessedData = (List<Map<String, String>>) result.get(Constants.SUCCESS_PROCESS_DATA);

            //processing the successful data for saving in db
            if (successProcessedData != null && !successProcessedData.isEmpty()) {
                processReceivedData(partnerCode, successProcessedData, fileName, fileId, initiatedOn, partnerId);
            } else {
                log.info("No successful data to process for partner: {}", partnerCode);
            }
            Optional<FileInfoEntity> fileInfoOptional = fileInfoRepository.findById(fileId);
            if (fileInfoOptional.isPresent()) {
                FileInfoEntity fileInfoEntity = fileInfoOptional.get();
                processedNoOfContent = fileInfoEntity.getProcessedNoOfContent();
                totalNumberOfContent = fileInfoEntity.getTotalNoOfContent();
                processedNoOfContent = processedNoOfContent + 1;
            }
            dataTransformUtility.createFileInfo(partnerId, fileId, fileName, initiatedOn, null, Constants.CONTENT_UPLOAD_IN_PROGRESS, null, totalNumberOfContent, processedNoOfContent);
            if (processedNoOfContent == totalNumberOfContent) {
                log.info("Data all process done{} ", processedNoOfContent);
                handleLogFile(fileId, fileName, partnerId, initiatedOn, totalNumberOfContent, processedNoOfContent);
            }
        } catch (Exception e) {
            loadContentErrorMessage = "Error in processReceivedData: " + e.getMessage();
            log.error(loadContentErrorMessage, e);
            handleErrorScenario(fileId, fileName, partnerCode, partnerId, initiatedOn, loadContentErrorMessage, totalNumberOfContent, processedNoOfContent);
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

    private void handleErrorScenario(String fileId, String fileName, String partnerCode, String partnerId, Timestamp initiatedOn, String errorMessage, int totalNumberOfContent, int processedNoOfContent) {
        try {
            Map<String, Object> errorResult = ciosContentServiceimpl.processRowsAndCreateLogs(null, fileId, fileName, partnerCode, errorMessage);
            handleLogFile(fileId, fileName, partnerId, initiatedOn, totalNumberOfContent, processedNoOfContent);
            log.info("Log file uploaded to GCP with failure status.");
        } catch (Exception logException) {
            log.error("Error while generating or uploading error logs for fileId: {}", fileId, logException);
        }
    }


    private void handleLogFile(String fileId, String fileName, String partnerId, Timestamp initiatedOn, int totalNumberOfContent, int processedNoOfContent) throws IOException {
        List<FileLogInfoEntity> logEntities = fileLogInfoRepository.findByFileId(fileId);
        if (logEntities.isEmpty()) {
            log.warn("No logs found for fileId: {}", fileId);
            return;
        }
        boolean isFailed = false;
        List<LinkedHashMap<String, String>> logs = new ArrayList<>();
        for (FileLogInfoEntity entity : logEntities) {
            if (entity.isHasFailure()) {
                isFailed = entity.isHasFailure();
            }
            LinkedHashMap<String, String> logData = objectMapper.convertValue(entity.getLogData(), new TypeReference<LinkedHashMap<String, String>>() {
            });
            logs.add(logData);
        }

        File logFile = ciosContentServiceimpl.writeLogsToFile(logs, fileName);
        ciosContentServiceimpl.uploadLogFileToGCP(logFile, partnerId, fileId, fileName, initiatedOn, isFailed, totalNumberOfContent, processedNoOfContent);
        log.info("Log file  successful uploaded to GCP for partner: {}", partnerId);
    }
}
