package com.igot.cios.service.impl;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.igot.cios.dto.DeleteContentRequestDto;
import com.igot.cios.dto.PaginatedResponse;
import com.igot.cios.dto.RequestDto;
import com.igot.cios.dto.SBApiResponse;
import com.igot.cios.entity.FileInfoEntity;
import com.igot.cios.exception.CiosContentException;
import com.igot.cios.kafka.KafkaProducer;
import com.igot.cios.plugins.ContentPartnerPluginService;
import com.igot.cios.plugins.ContentSource;
import com.igot.cios.plugins.DataTransformUtility;
import com.igot.cios.plugins.config.ContentPartnerServiceFactory;
import com.igot.cios.repository.FileInfoRepository;
import com.igot.cios.service.CiosContentService;
import com.igot.cios.storage.StoreFileToGCP;
import com.igot.cios.util.CbServerProperties;
import com.igot.cios.util.Constants;
import com.igot.cios.util.PayloadValidation;
import com.igot.cios.util.elasticsearch.dto.SearchCriteria;
import com.igot.cios.util.elasticsearch.dto.SearchResult;
import com.igot.cios.util.elasticsearch.service.EsUtilService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.data.domain.Page;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.*;


@Service
@Slf4j
public class CiosContentServiceImpl implements CiosContentService {

    @Autowired
    ObjectMapper objectMapper;
    @Autowired
    KafkaProducer kafkaProducer;
    @Autowired
    PayloadValidation payloadValidation;
    @Autowired
    DataTransformUtility dataTransformUtility;
    @Value("${spring.kafka.cornell.topic.name}")
    private String topic;
    @Autowired
    private ContentPartnerServiceFactory contentPartnerServiceFactory;
    @Autowired
    private FileInfoRepository fileInfoRepository;
    @Autowired
    EsUtilService esUtilService;
    @Value("${search.result.redis.ttl}")
    private long searchResultRedisTtl;
    @Autowired
    private RedisTemplate<String, SearchResult> redisTemplate;
    @Autowired
    private StoreFileToGCP storeFileToGCP;
    @Autowired
    private CbServerProperties cbServerProperties;

    @Override
    public void loadContentFromExcel(MultipartFile file, String partnerCode) {
        log.info("CiosContentServiceImpl::loadContentFromExcel");
        ContentSource contentSource = ContentSource.fromPartnerCode(partnerCode);
        if (contentSource == null) {
            log.warn("Unknown provider name: " + partnerCode);
            throw new CiosContentException("Unknown provider name:" + partnerCode, HttpStatus.BAD_REQUEST);
        }
        String fileName = file.getOriginalFilename();
        Timestamp initiatedOn = new Timestamp(System.currentTimeMillis());
        String fileId = dataTransformUtility.createFileInfo(null, null, fileName, initiatedOn, null, null, null);
        try {
            List<Map<String, String>> processedData = dataTransformUtility.processExcelFile(file);
            log.info("No.of processedData from excel: " + processedData.size());
            int batchSize = 500;
            List<List<Map<String, String>>> batches = splitIntoBatches(processedData, batchSize);
            for (List<Map<String, String>> batch : batches) {
                Map<String, Object> batchDataMap = new HashMap<>();
                batchDataMap.put(Constants.PARTNER_CODE, partnerCode);
                batchDataMap.put(Constants.FILE_NAME, fileName);
                batchDataMap.put(Constants.INITIATED_ON, initiatedOn);
                batchDataMap.put(Constants.FILE_ID, fileId);
                batchDataMap.put("data", batch);

                kafkaProducer.push(cbServerProperties.getCiosContentOnboardTopic(), batchDataMap);
                log.info("Batch of size {} sent to Kafka", batch.size());
            }
        } catch (Exception e) {
            JsonNode entity = dataTransformUtility.fetchPartnerInfoUsingApi(partnerCode);
            dataTransformUtility.createFileInfo(entity.path(Constants.RESULT).get(Constants.ID).asText(), fileId, fileName, initiatedOn, new Timestamp(System.currentTimeMillis()), Constants.CONTENT_UPLOAD_FAILED, null);
            throw new RuntimeException(e.getMessage());
        }
    }

    @Override
    public PaginatedResponse<?> fetchAllContentFromSecondaryDb(RequestDto dto) {
        log.info("CiosContentServiceImpl::fetchAllCornellContentFromDb");
        ContentSource contentSource = ContentSource.fromPartnerCode(dto.getPartnerCode());
        if (contentSource == null) {
            log.warn("Unknown provider name: " + dto.getPartnerCode());
            throw new CiosContentException("Unknown provider name:" + dto.getPartnerCode(), HttpStatus.BAD_REQUEST);
        }
        try {
            ContentPartnerPluginService service = contentPartnerServiceFactory.getContentPartnerPluginService(contentSource);
            Page<?> pageData = service.fetchAllContentFromSecondaryDb(dto);
            if (pageData != null) {
                return new PaginatedResponse<>(
                        pageData.getContent(),
                        pageData.getTotalPages(),
                        pageData.getTotalElements(),
                        pageData.getNumberOfElements(),
                        pageData.getSize(),
                        pageData.getNumber()
                );
            } else {
                return new PaginatedResponse<>(
                        Collections.emptyList(),
                        0,
                        0,
                        0,
                        0,
                        0
                );
            }
        } catch (DataAccessException dae) {
            log.error("Database access error while fetching content", dae.getMessage());
            throw new CiosContentException(Constants.ERROR, "Database access error: " + dae.getMessage());
        } catch (Exception e) {
            throw new CiosContentException(Constants.ERROR, e.getMessage());
        }

    }

    @Override
    public void loadContentProgressFromExcel(MultipartFile file, String partnerCode) {
        try {
            List<Map<String, String>> processedData = dataTransformUtility.processExcelFile(file);
            log.info("No.of processedData from excel: " + processedData.size());
            JsonNode jsonData = objectMapper.valueToTree(processedData);
            jsonData.forEach(
                    eachContentData -> {
                        callEnrollmentAPI(eachContentData, partnerCode);
                    });
        } catch (Exception e) {
            throw new CiosContentException(e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    private void callEnrollmentAPI(JsonNode rawContentData, String partnerCode) {
        try {
            log.info("CiosContentServiceImpl::saveOrUpdateContentFromProvider");
            JsonNode entity = dataTransformUtility.fetchPartnerInfoUsingApi(partnerCode);
            List<Object> contentJson = objectMapper.convertValue(entity.path("result").path("transformProgressJson"), new TypeReference<List<Object>>() {
            });
            JsonNode transformData = dataTransformUtility.transformData(rawContentData, contentJson);
            payloadValidation.validatePayload(Constants.PROGRESS_DATA_VALIDATION_FILE, transformData);
            ((ObjectNode) transformData).put("partnerCode", partnerCode);
            ((ObjectNode) transformData).put("partnerId", entity.get("id").asText());
            kafkaProducer.push(topic, transformData);
            log.info("callCornellEnrollmentAPI {} ", transformData.asText());
        } catch (Exception e) {
            log.error("error while processing", e);
            throw new CiosContentException(e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @Override
    public List<FileInfoEntity> getAllFileInfos(String partnerId) {
        log.info("CiosContentService:: getAllFileInfos: fetching all information about file");
        try {
            List<FileInfoEntity> fileInfo = fileInfoRepository.findByPartnerId(partnerId);

            if (fileInfo.isEmpty()) {
                log.warn("No file information found for partnerId: {}", partnerId);
            } else {
                log.info("File information found for partnerId: {}", partnerId);
            }
            return fileInfo;
        } catch (DataAccessException dae) {
            log.error("Database access error while fetching info", dae.getMessage());
            throw new CiosContentException(Constants.ERROR, "Database access error: " + dae.getMessage());
        } catch (Exception e) {
            throw new CiosContentException(Constants.ERROR, e.getMessage());
        }
    }

    @Override
    public ResponseEntity<?> deleteNotPublishContent(DeleteContentRequestDto deleteContentRequestDto) {
        log.info("Deleting non-published content");
        String partnerCode = deleteContentRequestDto.getPartnerCode();
        ContentSource contentSource = ContentSource.fromPartnerCode(partnerCode);
        if (contentSource == null) {
            log.warn("Unknown provider name: " + deleteContentRequestDto.getPartnerCode());
            throw new CiosContentException("Unknown provider name:" + deleteContentRequestDto.getPartnerCode(), HttpStatus.BAD_REQUEST);
        }
        ContentPartnerPluginService service = contentPartnerServiceFactory.getContentPartnerPluginService(contentSource);
        return service.deleteNotPublishContent(deleteContentRequestDto);
    }

    @Override
    public Object readContentByExternalId(String partnercode, String externalid) {
        ContentSource contentSource = ContentSource.fromPartnerCode(partnercode);
        if (contentSource == null) {
            log.warn("Unknown provider name: " + partnercode);
            throw new CiosContentException("Unknown provider name:" + partnercode, HttpStatus.BAD_REQUEST);
        }
        ContentPartnerPluginService service = contentPartnerServiceFactory.getContentPartnerPluginService(contentSource);
        return service.readContentByExternalId(externalid);
    }

    @Override
    public SearchResult searchContent(SearchCriteria searchCriteria) {
        log.info("CiosContentServiceImpl::searchCotent");
        try {
            SearchResult searchResult = esUtilService.searchDocuments(Constants.CIOS_CONTENT_INDEX_NAME, searchCriteria);
            return searchResult;
        } catch (Exception e) {
            throw new CiosContentException("ERROR", e.getMessage(), HttpStatus.BAD_REQUEST);
        }
    }

    @Override
    public Object updateContent(JsonNode jsonNode) {
        log.info("CiosContentServiceImpl::updateContent");
        String partnerCode = jsonNode.path("content").get("contentPartner").get("partnerCode").asText();
        ContentSource contentSource = ContentSource.fromPartnerCode(partnerCode);
        if (contentSource == null) {
            log.warn("Unknown provider name: " + partnerCode);
            throw new CiosContentException("Unknown provider name:" + partnerCode, HttpStatus.BAD_REQUEST);
        }
        ContentPartnerPluginService service = contentPartnerServiceFactory.getContentPartnerPluginService(contentSource);
        dataTransformUtility.validatePayload(Constants.UPDATED_DATA_PAYLOAD_VALIDATION_FILE, jsonNode);
        return service.updateContent(jsonNode, partnerCode);
    }

    public void processRowsAndCreateLogs(List<Map<String, String>> processedData, JsonNode entity, String fileId, String fileName, Timestamp initiatedOn, String partnerCode) throws IOException {
        log.info("Starting row validation and log generation for file: {}", fileName);
        List<LinkedHashMap<String, String>> successLogs = new ArrayList<>();
        List<LinkedHashMap<String, String>> errorLogs = new ArrayList<>();
        boolean hasFailures = false;
        String schemaFilePath = dataTransformUtility.getSchemaFilePathForPartner(partnerCode);
        for (Map<String, String> row : processedData) {
            LinkedHashMap<String, String> linkedRow = new LinkedHashMap<>(row);
            List<String> validationErrors = dataTransformUtility.validateRowData(linkedRow, schemaFilePath);
            if (validationErrors.isEmpty()) {
                linkedRow.put(Constants.STATUS, Constants.SUCCESS);
                linkedRow.put("error", "");
                successLogs.add(linkedRow);
            } else {
                linkedRow.put(Constants.STATUS, Constants.FAILED);
                linkedRow.put("error", String.join(", ", validationErrors));
                errorLogs.add(linkedRow);
                hasFailures = true;
                log.warn("Validation failed for row: {}. Errors: {}", row, validationErrors);
            }
        }
        List<LinkedHashMap<String, String>> combinedLogs = new ArrayList<>();
        combinedLogs.addAll(successLogs);
        combinedLogs.addAll(errorLogs);
        String logFileName = fileName + "_" + partnerCode;
        File logFile= writeLogsToFile(combinedLogs, logFileName);
        SBApiResponse uploadedGCPFileResponse = storeFileToGCP.uploadCiosLogsFile(logFile,cbServerProperties.getCiosCloudContainerName(), cbServerProperties.getCiosFileLogsCloudFolderName());
        String uploadedGCPFileName = "";
        if (uploadedGCPFileResponse.getParams().getStatus().equals(Constants.SUCCESS)) {
            uploadedGCPFileName = uploadedGCPFileResponse.getResult().get(Constants.NAME).toString();
            log.info("Log file uploaded successfully. File URL: {}", uploadedGCPFileName);
        } else {
            log.error("Failed to upload log file. Error message: {}", uploadedGCPFileResponse.getParams().getErrmsg());
        }
        Timestamp completedOn = new Timestamp(System.currentTimeMillis());
        if (hasFailures) {
            log.info("Marking file: {} as failed due to validation errors", fileName);
            dataTransformUtility.createFileInfo(entity.path(Constants.RESULT).get(Constants.ID).asText(), fileId, fileName, initiatedOn, completedOn, Constants.CONTENT_UPLOAD_FAILED, uploadedGCPFileName);
        } else {
            log.info("Marking file: {} as successful, no validation errors found", fileName);
            dataTransformUtility.createFileInfo(entity.path(Constants.RESULT).get(Constants.ID).asText(), fileId, fileName, initiatedOn, completedOn, Constants.CONTENT_UPLOAD_SUCCESSFULLY, uploadedGCPFileName);
        }
    }

    private File writeLogsToFile(List<LinkedHashMap<String, String>> logs, String originalFileName) throws IOException {
        log.info("Logs written to file: {}", originalFileName);
        String csvFileName = originalFileName + "_log.csv";
        String tempDir = System.getProperty("java.io.tmpdir");
        String csvFilePath = tempDir + File.separator + csvFileName;
        File logFile = new File(csvFilePath);
        if (!logFile.exists()) {
            logFile.getParentFile().mkdirs();
            logFile.createNewFile();
        }
        try (FileWriter writer = new FileWriter(csvFilePath)) {
            if (!logs.isEmpty()) {
                LinkedHashMap<String, String> firstLog = logs.get(0);
                StringBuilder header = new StringBuilder();
                for (String key : firstLog.keySet()) {
                    header.append(escapeSpecialCharacters(key)).append(" | ");
                }
                header.append(Constants.TIME);
                writer.write(header.toString());
                writer.write(System.lineSeparator());
                for (LinkedHashMap<String, String> logEntry : logs) {
                    StringBuilder row = new StringBuilder();
                    for (String key : firstLog.keySet()) {
                        row.append(escapeSpecialCharacters(logEntry.getOrDefault(key, ""))).append(" | ");
                    }
                    String timestamp = new Timestamp(System.currentTimeMillis()).toString();
                    row.append(timestamp);
                    writer.write(row.toString());
                    writer.write(System.lineSeparator());
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return logFile;
    }

    private String escapeSpecialCharacters(String value) {
        String escapedValue = value;
        if (value.contains(",") || value.contains("\"") || value.contains("\n")) {
            escapedValue = "\"" + value.replace("\"", "\"\"") + "\"";
        }
        return escapedValue;
    }

    private List<List<Map<String, String>>> splitIntoBatches(List<Map<String, String>> data, int batchSize) {
        List<List<Map<String, String>>> batches = new ArrayList<>();
        for (int i = 0; i < data.size(); i += batchSize) {
            int end = Math.min(data.size(), i + batchSize);
            batches.add(data.subList(i, end));
        }
        return batches;
    }

}
