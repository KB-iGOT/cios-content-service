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
import com.igot.cios.entity.FileLogInfoEntity;
import com.igot.cios.exception.CiosContentException;
import com.igot.cios.kafka.KafkaProducer;
import com.igot.cios.plugins.DataTransformUtility;
import com.igot.cios.repository.CornellContentRepository;
import com.igot.cios.repository.FileInfoRepository;
import com.igot.cios.repository.FileLogInfoRepository;
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
    @Autowired
    private FileLogInfoRepository fileLogInfoRepository;

    @Override
    public SBApiResponse loadContentFromExcel(MultipartFile file, String partnerCode, String partnerId) {
        log.info("CiosContentServiceImpl::loadContentFromExcel");
        SBApiResponse response = SBApiResponse.createDefaultResponse(Constants.API_CIOS_LOAD_EXCEL_CONTENT);
        String fileName = file.getOriginalFilename();
        Timestamp initiatedOn = new Timestamp(System.currentTimeMillis());
        int totalNumberOfContent = 0;
        int processedNoOfContent = 0;
        try {
            List<Map<String, String>> processedData = dataTransformUtility.processExcelFile(file);
            if (processedData == null || processedData.isEmpty()) {
                response.getParams().setErrmsg(Constants.FILE_FORMAT_ERROR);
                response.getParams().setStatus(Constants.FAILED);
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                return response;
            }
            log.info("No.of processedData from excel: " + processedData.size());
            totalNumberOfContent = processedData.size();
            String fileId = dataTransformUtility.createFileInfo(partnerId, null, fileName, initiatedOn, null, Constants.CONTENT_UPLOAD_IN_PROGRESS, null, totalNumberOfContent, processedNoOfContent);
            for (int i = 0; i < processedData.size(); i++) {
                Map<String, String> content = processedData.get(i);
                Map<String, Object> contentDataMap = new HashMap<>();
                contentDataMap.put(Constants.PARTNER_CODE, partnerCode);
                contentDataMap.put(Constants.FILE_NAME, fileName);
                contentDataMap.put(Constants.INITIATED_ON, initiatedOn);
                contentDataMap.put(Constants.FILE_ID, fileId);
                contentDataMap.put(Constants.PARTNER_ID, partnerId);
                contentDataMap.put(Constants.DATA, content);

                kafkaProducer.push(cbServerProperties.getCiosContentOnboardTopic(), contentDataMap);
            }
            return response;
        } catch (Exception e) {
            response.getParams().setErrmsg(e.getMessage());
            response.getParams().setStatus(Constants.FAILED);
            response.setResponseCode(HttpStatus.BAD_REQUEST);
            return response;
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
        Long totalCourseCount = repository.countByPartnerCode(partnerCode);
        JsonNode contentPartnerResponse = dataTransformUtility.fetchPartnerInfoUsingApi(partnerCode);
        JsonNode resultData = contentPartnerResponse.path(Constants.RESULT);
        JsonNode data = resultData.path(Constants.DATA);
        ((ObjectNode) data).put(Constants.TOTAL_COURSES_COUNT, totalCourseCount);
        dataTransformUtility.updatingPartnerInfo(resultData);
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
        Optional<CornellContentEntity> entity = repository.findByExternalIdAndPartnerCode(externalid,partnercode);
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
        return saveOrUpdateContent(externalId,jsonNode,currentTime,isActive,partnerCode,partnerId);
    }

    private CornellContentEntity saveOrUpdateContent(String externalId, JsonNode transformData, Timestamp currentTime, boolean isActive, String partnerCode,String partnerId) {
        ((ObjectNode) transformData.path(Constants.CONTENT)).put(Constants.PARTNER_CODE, partnerCode).asText();
        addSearchTags(transformData);
        CornellContentEntity externalContent;
        Optional<CornellContentEntity> optExternalContent = repository.findByExternalIdAndPartnerId(externalId,partnerId);
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

    public Map<String, Object> processRowsAndCreateLogs(
            Map<String, String> processedData,
            String fileId,
            String fileName,
            String partnerCode,
            String loadContentErrorMessage) throws IOException {

        log.info("Starting row validation and log generation for file: {}", fileName);
        List<Map<String, String>> successProcessedData = new ArrayList<>();
        boolean hasFailures = false;

        // Fetch partner info from API
        JsonNode response = dataTransformUtility.fetchPartnerInfoUsingApi(partnerCode);
        JsonNode fileValidation = response.path(Constants.RESULT).path(Constants.CONTENT_FILE_VALIDATION);

        if (loadContentErrorMessage != null) {
            Map<String, String> loadContentErrorLog = new HashMap<>();
            loadContentErrorLog.put(Constants.FILE_ID, fileId);
            loadContentErrorLog.put(Constants.FILE_NAME, fileName);
            loadContentErrorLog.put(Constants.PARTNER_CODE, partnerCode);
            loadContentErrorLog.put(Constants.STATUS, Constants.FAILED);
            loadContentErrorLog.put("error", loadContentErrorMessage);
            hasFailures = true;
            saveLog(loadContentErrorLog, fileId, hasFailures);
        } else {
            Map<String, String> linkedRow = new HashMap<>(processedData);
            List<String> validationErrors = dataTransformUtility.validateRowData(linkedRow, fileValidation);
            if (validationErrors.isEmpty()) {
                linkedRow.put(Constants.STATUS, Constants.SUCCESS);
                linkedRow.put("error", "");
                successProcessedData.add(processedData);
                hasFailures = false;
                saveLog(linkedRow, fileId, hasFailures);
            } else {
                linkedRow.put(Constants.STATUS, Constants.FAILED);
                linkedRow.put("error", String.join(", ", validationErrors));
                hasFailures = true;
                saveLog(linkedRow, fileId, hasFailures);
            }
        }
        Map<String, Object> result = new HashMap<>();
        result.put(Constants.SUCCESS_PROCESS_DATA, successProcessedData);
        return result;
    }

    public File writeLogsToFile(List<LinkedHashMap<String, String>> logs, String originalFileName) throws IOException {
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

    private void saveLog(Map<String, String> logData, String fileId, boolean hasFailures) {
        JsonNode logJson = objectMapper.valueToTree(logData);
        FileLogInfoEntity logInfoEntity = new FileLogInfoEntity();
        logInfoEntity.setId(UUID.randomUUID().toString());
        logInfoEntity.setFileId(fileId);
        logInfoEntity.setLogData(logJson);
        logInfoEntity.setHasFailure(hasFailures);
        fileLogInfoRepository.save(logInfoEntity);
    }

    public void uploadLogFileToGCP(File logFile, String partnerId, String fileId, String fileName, Timestamp initiatedOn, boolean hasFailures, int totalNumberOfContent, int processedNoOfContent) throws IOException {
        log.info("consumeMessage::uploadLogFileToGCP:uploading file to GCP");
        SBApiResponse uploadedGCPFileResponse = storeFileToGCP.uploadCiosLogsFile(
                logFile,
                cbServerProperties.getCiosCloudContainerName(),
                cbServerProperties.getCiosFileLogsCloudFolderName());
        String uploadedGCPFileName = "";
        if (uploadedGCPFileResponse.getParams().getStatus().equals(Constants.SUCCESS)) {
            uploadedGCPFileName = uploadedGCPFileResponse.getResult().get(Constants.NAME).toString();
            log.info("Log file  successful uploaded to GCP, File URL: {}", uploadedGCPFileName);
        } else {
            log.error("Failed to upload log file. Error message: {}", uploadedGCPFileResponse.getParams().getErrmsg());
        }
        Timestamp completedOn = new Timestamp(System.currentTimeMillis());
        String status = hasFailures ? Constants.CONTENT_UPLOAD_FAILED : Constants.CONTENT_UPLOAD_SUCCESSFULLY;
        dataTransformUtility.createFileInfo(partnerId, fileId, fileName, initiatedOn, completedOn, status, uploadedGCPFileName, totalNumberOfContent, processedNoOfContent);
        fileLogInfoRepository.deleteByFileId(fileId);
    }

}
