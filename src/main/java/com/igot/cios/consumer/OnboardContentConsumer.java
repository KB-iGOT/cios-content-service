package com.igot.cios.consumer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cios.dto.SBApiResponse;
import com.igot.cios.entity.FileInfoEntity;
import com.igot.cios.exception.CiosContentException;
import com.igot.cios.plugins.DataTransformUtility;
import com.igot.cios.repository.FileInfoRepository;
import com.igot.cios.service.impl.CiosContentServiceImpl;
import com.igot.cios.storage.StoreFileToGCP;
import com.igot.cios.util.CbServerProperties;
import com.igot.cios.util.Constants;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.stereotype.Component;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

@Component
@Slf4j
public class OnboardContentConsumer {
    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private CiosContentServiceImpl ciosContentServiceimpl;

    @Autowired
    private FileInfoRepository fileInfoRepository;

    @Autowired
    private StoreFileToGCP storeFileToGCP;

    @Autowired
    private CbServerProperties cbServerProperties;

    @Autowired
    DataTransformUtility dataTransformUtility;

    private String cachedPartnerCode = null;
    private List<Object> cachedContentJson = null;
    private long cacheTimestamp = 0;

    @KafkaListener(topics = "${kafka.topic.content.onboarding}", groupId = "${content.onboarding.consumer.group}")
    public void consumeMessage(String message) {
        CompletableFuture.runAsync(() -> {
            try {
                log.info("Received message to process: {}", message);
                if (StringUtils.isNoneBlank(message)) {
                    // Call the asynchronous processing method
                    processOnboardingContent(message);
                } else {
                    log.error("Error in consuming message: Invalid content");
                }
            } catch (Exception e) {
                log.error("Error in consumeMessage: {}", e.getMessage(), e);
            }
        });
    }

    public void processOnboardingContent(String message) {
        String contentUploadedGCPFileName = null;
        Path tmpPath = null;
        String loadContentErrorMessage = null;
        String fileId = null;
        String fileName = null;
        String partnerCode = null;
        String partnerId = null;
        Timestamp initiatedOn = null;

        try {
            log.info("Consuming the content to onboard in cios");
            Map<String, Object> receivedMessage = objectMapper.readValue(message, new TypeReference<Map<String, Object>>() {
            });
            log.info("Received {} records from Kafka", receivedMessage.size());

            partnerCode = (String) receivedMessage.get(Constants.PARTNER_CODE);
            fileName = (String) receivedMessage.get(Constants.FILE_NAME);
            initiatedOn = objectMapper.convertValue(receivedMessage.get(Constants.INITIATED_ON), Timestamp.class);
            fileId = (String) receivedMessage.get(Constants.FILE_ID);
            partnerId = (String) receivedMessage.get(Constants.PARTNER_ID);

            Optional<FileInfoEntity> fileInfoOptional = fileInfoRepository.findById(fileId);
            if (fileInfoOptional.isPresent()) {
                FileInfoEntity fileInfoEntity = fileInfoOptional.get();
                contentUploadedGCPFileName = fileInfoEntity.getContentUploadedGCPFileName();
            }

            ResponseEntity<?> response = storeFileToGCP.downloadCiosContentFile(contentUploadedGCPFileName);
            if (!response.getStatusCode().is2xxSuccessful() || !(response.getBody() instanceof ByteArrayResource)) {
                log.error("Failed to download file: {}", contentUploadedGCPFileName);
                return;
            }

            tmpPath = Paths.get(Constants.LOCAL_BASE_PATH + contentUploadedGCPFileName);
            Files.write(tmpPath, ((ByteArrayResource) response.getBody()).getByteArray());
            MultipartFile tempFile = new MockMultipartFile(
                    contentUploadedGCPFileName,
                    contentUploadedGCPFileName,
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    Files.readAllBytes(tmpPath)
            );

            List<Map<String, String>> processedData = dataTransformUtility.processExcelFile(tempFile);
            if (processedData == null || processedData.isEmpty()) {
                log.warn("The provided file {} contains no valid data or has an unsupported format.", contentUploadedGCPFileName);
                throw new IllegalArgumentException("The file contains no valid data or has an unsupported format.");
            }

            Map<String, Object> result = ciosContentServiceimpl.processRowsAndCreateLogs(
                    processedData, fileId, fileName, partnerCode, null, partnerId);
            File logFile = (File) result.get(Constants.LOG_FILE);
            boolean hasFailures = (boolean) result.get(Constants.HAS_FAILURES);

            uploadLogFileToGCP(logFile, partnerId, fileId, fileName, initiatedOn, hasFailures, contentUploadedGCPFileName);
            log.info("Log file successfully uploaded to GCP for partner: {}", partnerCode);
        } catch (Exception e) {
            loadContentErrorMessage = "Error in processReceivedData: " + e.getMessage();
            log.error(loadContentErrorMessage, e);
            try {
                Map<String, Object> errorResult = ciosContentServiceimpl.processRowsAndCreateLogs(
                        null, fileId, fileName, partnerCode, loadContentErrorMessage, partnerId);

                File errorLogFile = (File) errorResult.get(Constants.LOG_FILE);
                boolean hasFailures = true;
                uploadLogFileToGCP(errorLogFile, partnerId, fileId, fileName, initiatedOn, hasFailures, contentUploadedGCPFileName);
                log.info("Log file uploaded to GCP with failure status for partner: {}", partnerCode);
            } catch (Exception logException) {
                log.error("Error while generating or uploading error logs for partner: {}", partnerCode, logException);
            }
        } finally {
            if (tmpPath != null && Files.exists(tmpPath)) {
                try {
                    Files.delete(tmpPath);
                } catch (IOException e) {
                    log.error("Failed to delete temporary file: {}", tmpPath, e);
                }
            }
        }
    }


    public void processReceivedData(String partnerCode, List<Map<String, String>> processedData, String fileName, String fileId, String partnerId) throws IOException {
        log.info("Processing {} records for partner code {}", processedData.size(), partnerCode);
        JsonNode jsonData = objectMapper.valueToTree(processedData);
        boolean isCacheExpired = System.currentTimeMillis() - cacheTimestamp > Constants.CACHE_EXPIRY_DURATION;
        if (cachedPartnerCode == null || !cachedPartnerCode.equals(partnerCode) || isCacheExpired) {
            JsonNode partnerInfo = dataTransformUtility.fetchPartnerInfoUsingApi(partnerCode);
            // Extract and cache contentJson
            cachedContentJson = objectMapper.convertValue(
                    partnerInfo.path("trasformContentJson"),
                    new TypeReference<List<Object>>() {
                    }
            );
            if (cachedContentJson == null || cachedContentJson.isEmpty()) {
                log.error("trasformContentJson is missing, please update in contentPartner for partner {}",partnerCode);
                throw new CiosContentException(
                        "ERROR",
                        "trasformContentJson is missing, please update in contentPartner",
                        HttpStatus.INTERNAL_SERVER_ERROR
                );
            }
            cachedPartnerCode = partnerCode;
        }
        dataTransformUtility.updateProcessedDataInDb(jsonData, partnerCode, fileName, fileId, cachedContentJson, partnerId);
    }

    public void uploadLogFileToGCP(File logFile, String partnerId, String fileId, String fileName, Timestamp initiatedOn, boolean hasFailures, String contentUploadedGCPFileName) throws IOException {
        log.info("consumeMessage::uploadLogFileToGCP:uploading file to GCP");
        SBApiResponse uploadedGCPFileResponse = storeFileToGCP.uploadCiosLogsFile(
                logFile,
                cbServerProperties.getCiosCloudContainerName(),
                cbServerProperties.getCiosFileLogsCloudFolderName());
        String uploadedGCPFileName = "";
        if (uploadedGCPFileResponse.getParams().getStatus().equals(Constants.SUCCESS)) {
            uploadedGCPFileName = uploadedGCPFileResponse.getResult().get(Constants.NAME).toString();
        } else {
            log.error("Failed to upload log file. Error message: {}", uploadedGCPFileResponse.getParams().getErrmsg());
        }
        Timestamp completedOn = new Timestamp(System.currentTimeMillis());
        String status = hasFailures ? Constants.CONTENT_UPLOAD_FAILED : Constants.CONTENT_UPLOAD_SUCCESSFULLY;
        dataTransformUtility.createFileInfo(partnerId, fileId, fileName, initiatedOn, completedOn, status, uploadedGCPFileName, contentUploadedGCPFileName);
    }
}
