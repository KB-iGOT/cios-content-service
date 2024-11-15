package com.igot.cios.service.impl;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.igot.cios.dto.DeleteContentRequestDto;
import com.igot.cios.dto.PaginatedResponse;
import com.igot.cios.dto.RequestDto;
import com.igot.cios.dto.SBApiResponse;
import com.igot.cios.entity.CornellContentEntity;
import com.igot.cios.entity.FileInfoEntity;
import com.igot.cios.exception.CiosContentException;
import com.igot.cios.kafka.KafkaProducer;
import com.igot.cios.plugins.ContentPartnerPluginService;
import com.igot.cios.plugins.DataTransformUtility;
import com.igot.cios.repository.CornellContentRepository;
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
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
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
    private CornellContentRepository repository;
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
    public void loadContentFromExcel(MultipartFile file, String partnerCode, String partnerId) {
        log.info("CiosContentServiceImpl::loadContentFromExcel");
        String fileName = file.getOriginalFilename();
        Timestamp initiatedOn = new Timestamp(System.currentTimeMillis());
        String fileId = dataTransformUtility.createFileInfo(partnerId, null, fileName, initiatedOn, null, Constants.CONTENT_UPLOAD_IN_PROGRESS , null);
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
                batchDataMap.put(Constants.PARTNER_ID, partnerId);
                batchDataMap.put("data", batch);

                kafkaProducer.push(cbServerProperties.getCiosContentOnboardTopic(), batchDataMap);
                log.info("Batch of size {} sent to Kafka", batch.size());
            }
        } catch (Exception e) {
            dataTransformUtility.createFileInfo(partnerId, fileId, fileName, initiatedOn, new Timestamp(System.currentTimeMillis()), Constants.CONTENT_UPLOAD_FAILED, null);
            throw new RuntimeException(e.getMessage());
        }
    }

    @Override
    public PaginatedResponse<?> fetchAllContentFromSecondaryDb(RequestDto dto) {
        log.info("CiosContentServiceImpl::fetchAllCornellContentFromDb");
        try {
            Pageable pageable = PageRequest.of(dto.getPage(), dto.getSize());
            Page<?> pageData = repository.findAllCiosDataAndIsActive(dto.getIsActive(), pageable, dto.getKeyword());
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
        SBApiResponse response = SBApiResponse.createDefaultResponse(Constants.API_CB_PLAN_PUBLISH);
        String partnerCode=deleteContentRequestDto.getPartnerCode();
        List<String> externalIds = deleteContentRequestDto.getExternalId();
        List<CornellContentEntity> entities = repository.findByExternalIdInAndPartnerCode(externalIds,partnerCode);
        List<String> errors = new ArrayList<>();
        for (String id : externalIds) {
            Optional<CornellContentEntity> entityOpt = entities.stream()
                    .filter(entity -> entity.getExternalId().equals(id))
                    .findFirst();
            if (entityOpt.isPresent()) {
                CornellContentEntity entity = entityOpt.get();
                if (entity.getIsActive()) {
                    errors.add("External ID: " + id + " is live, cannot delete.");
                } else {
                    JsonNode ciosData = entity.getCiosData(); // Assuming this getter method exists
                    if (ciosData != null && ciosData.path("content").has("status")) {
                        String status = ciosData.path("content").get("status").asText();
                        if ("notInitiated".equalsIgnoreCase(status)) {
                            repository.delete(entity);
                            String uniqueId = deleteContentRequestDto.getPartnerCode() + "_" + entity.getExternalId();
                            esUtilService.deleteDocument(uniqueId, Constants.CIOS_CONTENT_INDEX_NAME);
                        } else {
                            errors.add("External ID: " + id + " cannot be deleted because its status is not 'notInitiated'.");
                        }
                    } else {
                        errors.add("External ID: " + id + " does not have a valid status in ciosData.");
                    }
                }
            } else {
                errors.add("External ID: " + id + " does not exist.");
            }
        }
        if (!errors.isEmpty()) {
            log.error("Validation errors: {}", errors);
            return buildErrorResponse(response, HttpStatus.BAD_REQUEST, String.join("\n", errors));
        }
        response.getResult().put(Constants.STATUS, Constants.SUCCESS);
        response.getResult().put(Constants.MESSAGE, "Content deleted successfully.");
        return ResponseEntity.ok(response);
    }

    private ResponseEntity<?> buildErrorResponse(SBApiResponse response, HttpStatus status, String errorMessage) {
        response.getParams().setStatus(Constants.FAILED);
        response.getParams().setErr(errorMessage);
        response.setResponseCode(status);
        return ResponseEntity.status(status).body(response);
    }
    @Override
    public Object readContentByExternalId(String partnercode, String externalid) {
        Optional<CornellContentEntity> entity = repository.findByExternalId(externalid);
        if (entity.isPresent()) {
            return entity.get().getCiosData();
        } else {
            throw new CiosContentException("No data found for given id", externalid, HttpStatus.BAD_REQUEST);
        }
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
        String partnerId = jsonNode.path("content").get("contentPartner").get("id").asText();
        Timestamp currentTime = new Timestamp(System.currentTimeMillis());
        String externalId = jsonNode.path("content").get("externalId").asText();
        boolean isActive = jsonNode.path("content").get("isActive").asBoolean(false);
        return saveOrUpdateCornellContent(externalId,jsonNode,currentTime,isActive,partnerCode,partnerId);
    }

    private CornellContentEntity saveOrUpdateCornellContent(String externalId, JsonNode transformData, Timestamp currentTime, boolean isActive, String partnerCode,String partnerId) {
        ((ObjectNode) transformData.path(Constants.CONTENT)).put(Constants.PARTNER_CODE, partnerCode).asText();
        addSearchTags(transformData);
        CornellContentEntity externalContent;
        Optional<CornellContentEntity> optExternalContent = repository.findByExternalId(externalId);
        if (optExternalContent.isPresent()) {
            externalContent = optExternalContent.get();
            externalContent.setExternalId(externalId);
            externalContent.setCiosData(transformData);
            externalContent.setIsActive(isActive);
            externalContent.setCreatedDate(externalContent.getCreatedDate());
            externalContent.setUpdatedDate(currentTime);
            externalContent.setPartnerCode(partnerCode);
            externalContent.setPartnerId(partnerId);
        } else {
            externalContent = new CornellContentEntity();
            externalContent.setExternalId(externalId);
            externalContent.setCiosData(transformData);
            externalContent.setIsActive(isActive);
            externalContent.setCreatedDate(currentTime);
            externalContent.setUpdatedDate(currentTime);
            externalContent.setPartnerCode(partnerCode);
            externalContent.setPartnerId(partnerId);
        }
        repository.save(externalContent);
        Map<String, Object> entityMap = objectMapper.convertValue(externalContent, Map.class);
        dataTransformUtility.flattenContentData(entityMap);
        String uniqueId = partnerCode + "_" + externalContent.getExternalId();
        esUtilService.updateDocument(Constants.CIOS_CONTENT_INDEX_NAME, Constants.INDEX_TYPE,
                uniqueId,
                entityMap,
                cbServerProperties.getElasticCiosContentJsonPath()
        );
        return externalContent;
    }

    private JsonNode addSearchTags(JsonNode transformData) {
        List<String> searchTags = new ArrayList<>();
        searchTags.add(transformData.path(Constants.CONTENT).get(Constants.NAME).textValue().toLowerCase());
        ArrayNode searchTagsArray = objectMapper.valueToTree(searchTags);
        ((ObjectNode) transformData.path(Constants.CONTENT)).put(Constants.CONTENT_SEARCH_TAGS, searchTagsArray);
        return transformData;
    }

    public void processRowsAndCreateLogs(List<Map<String, String>> processedData, String partnerId, String fileId, String fileName, Timestamp initiatedOn, String partnerCode, String loadContentErrorMessage) throws IOException {
        log.info("Starting row validation and log generation for file: {}", fileName);
        List<LinkedHashMap<String, String>> successLogs = new ArrayList<>();
        List<LinkedHashMap<String, String>> errorLogs = new ArrayList<>();
        boolean hasFailures = false;
        if (loadContentErrorMessage != null) {
            LinkedHashMap<String, String> loadContentErrorLog = new LinkedHashMap<>();
            loadContentErrorLog.put(Constants.FILE_ID, fileId);
            loadContentErrorLog.put(Constants.FILE_NAME, fileName);
            loadContentErrorLog.put(Constants.STATUS, Constants.FAILED);
            loadContentErrorLog.put("error", loadContentErrorMessage);
            errorLogs.add(loadContentErrorLog);
            hasFailures = true;
        } else {
            JsonNode response = dataTransformUtility.fetchPartnerInfoUsingApi(partnerCode);
            JsonNode fileValidation = response.path(Constants.RESULT).path("contentFileValidation");
            if(fileValidation.isMissingNode()){
                log.error("contentFileValidation is missing, please update in contentPartner");
                throw new CiosContentException("ERROR","contentFileValidation is missing, please update in contentPartner",HttpStatus.INTERNAL_SERVER_ERROR);
            }
            for (Map<String, String> row : processedData) {
                LinkedHashMap<String, String> linkedRow = new LinkedHashMap<>(row);
                List<String> validationErrors = dataTransformUtility.validateRowData(linkedRow, fileValidation);
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
        }
        List<LinkedHashMap<String, String>> combinedLogs = new ArrayList<>();
        combinedLogs.addAll(successLogs);
        combinedLogs.addAll(errorLogs);
        String logFileName = fileName + "_" + partnerCode;
        File logFile = writeLogsToFile(combinedLogs, logFileName);
        SBApiResponse uploadedGCPFileResponse = storeFileToGCP.uploadCiosLogsFile(logFile, cbServerProperties.getCiosCloudContainerName(), cbServerProperties.getCiosFileLogsCloudFolderName());
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
            dataTransformUtility.createFileInfo(partnerId, fileId, fileName, initiatedOn, completedOn, Constants.CONTENT_UPLOAD_FAILED, uploadedGCPFileName);
        } else {
            log.info("Marking file: {} as successful, no validation errors found", fileName);
            dataTransformUtility.createFileInfo(partnerId, fileId, fileName, initiatedOn, completedOn, Constants.CONTENT_UPLOAD_SUCCESSFULLY, uploadedGCPFileName);
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
