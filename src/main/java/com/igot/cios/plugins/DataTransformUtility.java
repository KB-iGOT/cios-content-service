package com.igot.cios.plugins;

import com.bazaarvoice.jolt.Chainr;
import com.bazaarvoice.jolt.JsonUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.igot.cios.dto.LogStatus;
import com.igot.cios.entity.CornellContentEntity;
import com.igot.cios.entity.FileInfoEntity;
import com.igot.cios.exception.CiosContentException;
import com.igot.cios.repository.CornellContentRepository;
import com.igot.cios.repository.FileInfoRepository;
import com.igot.cios.util.CbServerProperties;
import com.igot.cios.util.Constants;
import com.igot.cios.util.elasticsearch.service.EsUtilService;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.ValidationMessage;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.poi.ss.usermodel.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.*;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.util.UriComponentsBuilder;

import java.io.*;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.net.http.HttpRequest;

import static javax.xml.bind.DatatypeConverter.parseDate;

@Slf4j
@Component
public class DataTransformUtility {

    @Autowired
    ObjectMapper objectMapper;

    @Autowired
    CbServerProperties cbServerProperties;

    @Autowired
    RestTemplate restTemplate;

    @Autowired
    FileInfoRepository fileInfoRepository;

    @Autowired
    private EsUtilService esUtilService;

    @Autowired
    private CornellContentRepository cornellContentRepository;

    @Autowired
    private LogStatus logStatus;

    private List<Map<String, String>> processSheetAndSendMessage(Sheet sheet) {
        log.info("CiosContentServiceImpl::processSheetAndSendMessage");
        DataFormatter formatter = new DataFormatter();
        Row headerRow = sheet.getRow(0);
        List<Map<String, String>> dataRows = new ArrayList<>();
        for (int rowIndex = 1; rowIndex <= sheet.getLastRowNum(); rowIndex++) {
            Row dataRow = sheet.getRow(rowIndex);

            if (dataRow == null) {
                break; // No more data rows, exit the loop
            }

            boolean allBlank = true;
            Map<String, String> rowData = new HashMap<>();

            for (int colIndex = 0; colIndex < headerRow.getLastCellNum(); colIndex++) {
                Cell headerCell = headerRow.getCell(colIndex);
                Cell valueCell = dataRow.getCell(colIndex);

                if (headerCell != null && headerCell.getCellType() != CellType.BLANK) {
                    String excelHeader =
                            formatter.formatCellValue(headerCell).replaceAll("[\\n*]", "").trim();
                    String cellValue = "";

                    if (valueCell != null && valueCell.getCellType() != CellType.BLANK) {
                        if (valueCell.getCellType() == CellType.NUMERIC
                                && DateUtil.isCellDateFormatted(valueCell)) {
                            // Handle date format
                            Date date = valueCell.getDateCellValue();
                            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
                            cellValue = dateFormat.format(date);
                        } else {
                            cellValue = formatter.formatCellValue(valueCell).replace("\n", ",").trim();
                        }
                        allBlank = false;
                    }

                    rowData.put(excelHeader, cellValue);
                }
            }
            if (allBlank) {
                break; // If all cells are blank in the current row, stop processing
            }

            dataRows.add(rowData);
        }
        log.info("Number of Data Rows Processed: " + dataRows.size());
        return dataRows;
    }

    private List<Map<String, String>> processCsvAndSendMessage(InputStream inputStream) throws IOException {
        log.info("DesignationServiceImpl::processCsvAndSendMessage");
        List<Map<String, String>> dataRows = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
             CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader())) {
            List<String> headers = csvParser.getHeaderNames();
            for (CSVRecord csvRecord : csvParser) {
                boolean allBlank = true;
                Map<String, String> rowData = new HashMap<>();
                for (String header : headers) {
                    String cellValue = csvRecord.get(header);
                    if (cellValue != null && !cellValue.trim().isEmpty()) {

                        cellValue = cellValue.replace("\n", ",").trim();
                        allBlank = false;
                    }
                    rowData.put(header, cellValue);
                }
                if (allBlank) {
                    break;
                }
                dataRows.add(rowData);
            }
            log.info("Number of Data Rows Processed: " + dataRows.size());
        } catch (Exception e) {
            log.error(e.getMessage());
            throw new RuntimeException(e.getMessage());
        }
        return dataRows;
    }

    public JsonNode transformData(Object sourceObject, List<Object> specJson) {
        log.debug("CiosContentServiceImpl::transformData");
        try {
            String inputJson = objectMapper.writeValueAsString(sourceObject);
            Chainr chainr = Chainr.fromSpec(specJson);
            Object transformedOutput = chainr.transform(JsonUtils.jsonToObject(inputJson));
            return objectMapper.convertValue(transformedOutput, JsonNode.class);
        } catch (JsonProcessingException e) {
            log.error("Error transforming data", e);
            return null;
        }

    }

    public List<Map<String, String>> processExcelFile(MultipartFile incomingFile) {
        log.info("CiosContentServiceImpl::processExcelFile");
        try {
            return validateFileAndProcessRows(incomingFile);
        } catch (Exception e) {
            log.error("Error occurred during file processing: {}", e.getMessage());
            throw new RuntimeException(e.getMessage());
        }
    }

    private List<Map<String, String>> validateFileAndProcessRows(MultipartFile file) {
        log.info("CiosContentServiceImpl::validateFileAndProcessRows");
        String fileName = file.getOriginalFilename();
        if (fileName == null) {
            throw new RuntimeException("File name is null");
        }
        try (InputStream inputStream = file.getInputStream()) {
            if (fileName.endsWith(".xlsx") || fileName.endsWith(".xls")) {
                Workbook workbook = WorkbookFactory.create(inputStream);
                Sheet sheet = workbook.getSheetAt(0);
                return processSheetAndSendMessage(sheet);
            } else if (fileName.endsWith(".csv")) {
                return processCsvAndSendMessage(inputStream);
            } else {
                throw new RuntimeException("Unsupported file type: " + fileName);
            }
        } catch (IOException e) {
            log.error("Error while processing Excel file: {}", e.getMessage());
            throw new RuntimeException(e.getMessage());
        }
    }

    public String updatingPartnerInfo(JsonNode jsonNode) {
        log.info("CiosContentServiceImpl::updatingPartnerInfo:updating partner data");
        String url = cbServerProperties.getCbPoresbaseUrl() + cbServerProperties.getPartnerCreateEndPoint();
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<Object> entity = new HttpEntity<>(jsonNode, headers);
        ResponseEntity<String> response = restTemplate.postForEntity(url, entity, String.class);
        if (response.getStatusCode().is2xxSuccessful()) {
            return response.getBody();
        } else {
            throw new CiosContentException("Error from update content partner api", HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }


    public JsonNode fetchPartnerInfoUsingApi(String partnerCode) {
        log.info("CiosContentServiceImpl::fetchPartnerInfoUsingApi:fetching partner data by partnerCode {}",partnerCode);
        String getApiUrl = cbServerProperties.getCbPoresbaseUrl() + cbServerProperties.getPartnerReadEndPoint() + partnerCode;
        Map<String, String> headers = new HashMap<>();
        Map<String, Object> readData = (Map<String, Object>) fetchResultUsingGet(getApiUrl, headers);

        if (readData == null) {
            throw new RuntimeException("Failed to get data from API: Response is null");
        }
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.convertValue(readData.get("result"), JsonNode.class);
    }

    public Object fetchResultUsingGet(String uri, Map<String, String> headersValues) {
        log.info("CiosContentServiceImpl::fetchResultUsingGet:fetching partner data by get API call");
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        Map<String, Object> response = null;
        try {
            if (log.isDebugEnabled()) {
                StringBuilder str = new StringBuilder(this.getClass().getCanonicalName())
                        .append(Constants.FETCH_RESULT_CONSTANT).append(System.lineSeparator());
                str.append(Constants.URI_CONSTANT).append(uri).append(System.lineSeparator());
                log.debug(str.toString());
            }
            HttpHeaders headers = new HttpHeaders();
            if (!CollectionUtils.isEmpty(headersValues)) {
                headersValues.forEach((k, v) -> headers.set(k, v));
            }
            HttpEntity<Object> entity = new HttpEntity<>(headers);
            response = restTemplate.exchange(uri, HttpMethod.GET, entity, Map.class).getBody();
        } catch (HttpClientErrorException e) {
            try {
                response = (new ObjectMapper()).readValue(e.getResponseBodyAsString(),
                        new TypeReference<HashMap<String, Object>>() {
                        });
            } catch (Exception e1) {
            }
            log.error("Error received: " + e.getResponseBodyAsString(), e);
        } catch (Exception e) {
            log.error(String.valueOf(e));
            try {
                log.warn("Error Response: " + mapper.writeValueAsString(response));
            } catch (Exception e1) {
            }
        }
        return response;
    }

    public void validatePayload(String fileName, JsonNode payload) {
        try {
            log.debug("PayloadValidation :: validatePayload");
            JsonSchemaFactory schemaFactory = JsonSchemaFactory.getInstance();
            InputStream schemaStream = schemaFactory.getClass().getResourceAsStream(fileName);
            JsonSchema schema = schemaFactory.getSchema(schemaStream);
            if (payload.isArray()) {
                for (JsonNode objectNode : payload) {
                    validateObject(schema, objectNode);
                }
            } else {
                validateObject(schema, payload);
            }
        } catch (Exception e) {
            log.error("Failed to validate payload", e);
            throw new CiosContentException("Failed to validate payload", e.getMessage(), HttpStatus.BAD_REQUEST);
        }
    }

    private void validateObject(JsonSchema schema, JsonNode objectNode) {
        Set<ValidationMessage> validationMessages = schema.validate(objectNode);
        if (!validationMessages.isEmpty()) {
            StringBuilder errorMessage = new StringBuilder("Validation error(s): \n");
            for (ValidationMessage message : validationMessages) {
                errorMessage.append(message.getMessage()).append("\n");
            }
            log.error("Validation Error", errorMessage);
            throw new CiosContentException("Validation Error", errorMessage.toString(), HttpStatus.BAD_REQUEST);
        }
    }

    public String createFileInfo(String partnerId, String fileId, String fileName, Timestamp initiatedOn, Timestamp completedOn, String status, String GCPFileName, String contentUploadedGCPFileName) {
        log.info("CiosContentService:: createFileInfo: creating file information");
        FileInfoEntity fileInfoEntity = new FileInfoEntity();
        if (fileId == null) {
            fileInfoEntity = new FileInfoEntity();
            fileId = UUID.randomUUID().toString();
            fileInfoEntity.setFileId(fileId);
        }
        fileInfoEntity.setFileId(fileId);
        fileInfoEntity.setFileName(fileName);
        fileInfoEntity.setInitiatedOn(initiatedOn);
        fileInfoEntity.setCompletedOn(completedOn);
        fileInfoEntity.setStatus(status);
        fileInfoEntity.setPartnerId(partnerId);
        fileInfoEntity.setGCPFileName(GCPFileName);
        fileInfoEntity.setContentUploadedGCPFileName(contentUploadedGCPFileName);
        fileInfoRepository.save(fileInfoEntity);
        log.info("created successfully fileInfo {}", fileId);
        return fileId;
    }

    public List<String> validateRowData(String fileName,JsonNode rowNode) {
        List<String> invalidErrList = new ArrayList<>();
        try {
            JsonSchemaFactory schemaFactory = JsonSchemaFactory.getInstance();
            InputStream schemaStream = schemaFactory.getClass().getResourceAsStream(fileName);
            JsonSchema schema = schemaFactory.getSchema(schemaStream);
            if (rowNode.isArray()) {
                for (JsonNode objectNode : rowNode) {
                    validateRowDataObject(schema, objectNode, invalidErrList);
                }
            } else {
                validateRowDataObject(schema, rowNode, invalidErrList);
            }
        } catch (IllegalArgumentException e) {
            throw new RuntimeException(e);
        }
        return invalidErrList;
    }

    private void validateRowDataObject(JsonSchema schema, JsonNode objectNode, List<String> invalidErrList) {
        Set<ValidationMessage> validationMessages = schema.validate(objectNode);
        if (!validationMessages.isEmpty()) {
            for (ValidationMessage message : validationMessages) {
                invalidErrList.add(message.getMessage());
            }
        }
    }

    public Map<String,Object> updateProcessedDataInDb(List<Map<String, String>> transformData, String partnerCode, String fileName, String fileId,String partnerId,List<Object> transformContentJson) {
        log.info("DataTransformUtility :: updateProcessedDataInDb");
        try {
            logStatus.clearLogs();
            ArrayNode transformedDataArray = JsonNodeFactory.instance.arrayNode();
            Timestamp currentTime = new Timestamp(System.currentTimeMillis());
            transformData.forEach(transformedData -> {
                JsonNode eachContentData = transformData(transformedData, transformContentJson);
                if (eachContentData != null &&
                        eachContentData.path(Constants.CONTENT) != null &&
                        eachContentData.path(Constants.CONTENT).get(Constants.DURATION) != null) {
                    String durationString = eachContentData.path(Constants.CONTENT).get(Constants.DURATION).textValue();
                    String[] parts = durationString.split(" ");
                    String duration = String.valueOf(Integer.parseInt(parts[0]));
                    ((ObjectNode) eachContentData.path(Constants.CONTENT)).put(Constants.DURATION, duration).asText();
                }
                ((ObjectNode) eachContentData.path(Constants.CONTENT)).put(Constants.FILE_ID, fileId).asText();
                ((ObjectNode) eachContentData.path(Constants.CONTENT)).put(Constants.SOURCE, fileName).asText();
                ((ObjectNode) eachContentData.path(Constants.CONTENT)).put(Constants.PARTNER_CODE, partnerCode).asText();
                ((ObjectNode) eachContentData.path(Constants.CONTENT)).put(Constants.STATUS, Constants.NOT_INITIATED).asText();
                ((ObjectNode) eachContentData.path(Constants.CONTENT)).put(Constants.CREATED_DATE, currentTime.toString()).asText();
                ((ObjectNode) eachContentData.path(Constants.CONTENT)).put(Constants.UPDATED_DATE, currentTime.toString()).asText();
                ((ObjectNode) eachContentData.path(Constants.CONTENT)).put(Constants.ACTIVE, Constants.ACTIVE_STATUS).asText();
                ((ObjectNode) eachContentData.path(Constants.CONTENT)).put(Constants.PUBLISHED_ON, "0000-00-00 00:00:00").asText();
                ((ObjectNode) eachContentData.path(Constants.CONTENT)).put(Constants.PARTNER_ID, partnerId).asText();
                transformedDataArray.add(eachContentData);
            });
            return validatePayloadAndWriteLogsToFile(Constants.DATA_PAYLOAD_VALIDATION_FILE, transformedDataArray, fileName, partnerCode, fileId, partnerId,currentTime);
        }catch (Exception e){
            log.error("Error while updating processed data in DB", e);
            throw new CiosContentException("Error while updating processed data in DB", e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    private JsonNode addSearchTags(JsonNode transformData) {
        List<String> searchTags = new ArrayList<>();
        searchTags.add(transformData.path(Constants.CONTENT).get(Constants.NAME).textValue().toLowerCase());
        ArrayNode searchTagsArray = objectMapper.valueToTree(searchTags);
        ((ObjectNode) transformData.path(Constants.CONTENT)).put(Constants.CONTENT_SEARCH_TAGS, searchTagsArray);
        return transformData;
    }

    public CornellContentEntity saveOrUpdateCornellContent(String externalId, JsonNode transformData, JsonNode rawContentData, Timestamp currentTime, String fileId,String partnerId,String partnerCode) {
        Optional<CornellContentEntity> optExternalContent = cornellContentRepository.findByExternalIdAndPartnerId(externalId,partnerId);
        if (optExternalContent.isPresent()) {
            CornellContentEntity externalContent = optExternalContent.get();
            if(!(externalContent.getCiosData().get(Constants.CONTENT).get("status").equals("live")||externalContent.getCiosData().get(Constants.CONTENT).get("status").equals("draft"))){
                externalContent.setExternalId(externalId);
                externalContent.setCiosData(transformData);
                externalContent.setIsActive(externalContent.getIsActive());
                externalContent.setCreatedDate(externalContent.getCreatedDate());
                externalContent.setUpdatedDate(currentTime);
                externalContent.setSourceData(rawContentData);
                externalContent.setFileId(fileId);
                externalContent.setPartnerId(partnerId);
                externalContent.setPartnerCode((partnerCode));
            }else{
                //kafka changes need to add
            }
            return externalContent;
        } else {
            CornellContentEntity externalContent = new CornellContentEntity();
            externalContent.setExternalId(externalId);
            externalContent.setCiosData(transformData);
            externalContent.setIsActive(false);
            externalContent.setCreatedDate(currentTime);
            externalContent.setUpdatedDate(currentTime);
            externalContent.setSourceData(rawContentData);
            externalContent.setFileId(fileId);
            externalContent.setPartnerId(partnerId);
            externalContent.setPartnerCode((partnerCode));
            return externalContent;
        }
    }

    private void dataBulkSave(List<CornellContentEntity> cornellContentEntityList, String partnerCode) {
        log.info("DataTransformUtility :: dataBulkSave");
        cornellContentRepository.saveAll(cornellContentEntityList);
        cornellContentEntityList.forEach(contentEntity -> {
            try {
                Map<String, Object> entityMap = objectMapper.convertValue(contentEntity, Map.class);
                flattenContentData(entityMap);
                String uniqueId = partnerCode + "_" + contentEntity.getExternalId();
                esUtilService.addDocument(
                        Constants.CIOS_CONTENT_INDEX_NAME,
                        uniqueId,
                        entityMap,
                        cbServerProperties.getElasticCiosContentJsonPath()
                );
            } catch (Exception e) {
                log.error("Error while processing contentEntity with externalId: {}", contentEntity.getExternalId(), e);
            }
        });
        Long totalCourseCount = cornellContentRepository.countByPartnerCode(partnerCode);
        log.info("Total courses onboarded {} for partner {}",totalCourseCount,partnerCode);
        JsonNode response = fetchPartnerInfoUsingApi(partnerCode);
        JsonNode resultData = response.path(Constants.DATA);
        ((ObjectNode) resultData).put(Constants.TOTAL_COURSES_COUNT, totalCourseCount);
        updatingPartnerInfo(response);
    }

    public void flattenContentData(Map<String, Object> entityMap) {
        if (entityMap == null || entityMap.isEmpty()) {
            return;
        }

        Object ciosObj = entityMap.get(Constants.CIOS_DATA);
        if (!(ciosObj instanceof Map<?, ?> ciosDataMap)) {
            return;
        }

        Object contentObj = ciosDataMap.get(Constants.CONTENT);
        if (!(contentObj instanceof Map<?, ?> contentMap)) {
            return;
        }

        entityMap.putAll((Map<String, Object>) contentMap);
        entityMap.remove(Constants.CIOS_DATA);
        entityMap.remove(Constants.SOURCE_DATA);
    }

    public JsonNode callCiosReadApi(String extCourseId,String partnerId) {
        log.info("CourseScheduler :: callCiosReadApi");
        try {
            String url = cbServerProperties.getCbPoresbaseUrl() + cbServerProperties.getFixedUrl() + extCourseId + "/" + partnerId;
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            HttpEntity<String> entity = new HttpEntity<>(headers);
            ResponseEntity<Object> response = restTemplate.exchange(
                    url,
                    HttpMethod.GET,
                    entity,
                    Object.class
            );
            if (response.getStatusCode().is2xxSuccessful()) {
                JsonNode jsonNode = objectMapper.valueToTree(response.getBody());
                return jsonNode;
            } else {
                throw new CiosContentException(Constants.ERROR, "Failed to retrieve externalId. Status code: " + response.getStatusCodeValue());
            }
        } catch (Exception e) {
            throw new CiosContentException(Constants.ERROR, e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    public Map<String, Object> processRowsAndCreateLogs(
            List<Map<String, String>> processedData,
            String fileId,
            String fileName,
            String partnerCode,
            String loadContentErrorMessage,
            String partnerId) throws IOException {

        Map<String, Object> result = new HashMap<>();
        log.info("Starting row validation and log generation for file: {}", fileName);
        JsonNode contentPartnerResponse = fetchPartnerInfoUsingApi(partnerCode);
        JsonNode jsonData = contentPartnerResponse.path("trasformContentJson");
        if (loadContentErrorMessage != null) {
            logStatus.clearLogs();
            LinkedHashMap<String, String> loadContentErrorLog = new LinkedHashMap<>();
            loadContentErrorLog.put(Constants.FILE_ID, fileId);
            loadContentErrorLog.put(Constants.FILE_NAME, fileName);
            loadContentErrorLog.put(Constants.STATUS, Constants.FAILED);
            loadContentErrorLog.put(Constants.ERROR_KEY, loadContentErrorMessage);
            logStatus.getErrorLogs().add(loadContentErrorLog);
            logStatus.setHasFailures(true);
            String logFileName = fileName + "_" + partnerCode + Constants.LOG_TEXT;
            File logFile = writeLogsToFile(logStatus.getErrorLogs(), logFileName);
            result.put(Constants.LOG_FILE, logFile);
            result.put(Constants.HAS_FAILURES, logStatus.isHasFailures());
            return result;
        } else {
            List<Object> transformContentJson = objectMapper.convertValue(
                    jsonData,
                    new TypeReference<List<Object>>() {
                    });
            if (transformContentJson == null || transformContentJson.isEmpty()) {
                logStatus.clearLogs();
                log.error("trasformContentJson is missing, please update in contentPartner for partner {}", partnerCode);
                loadContentErrorMessage = "trasformContentJson is missing, please update in contentPartner: " + partnerCode;
                LinkedHashMap<String, String> transformErrorLog = new LinkedHashMap<>();
                transformErrorLog.put(Constants.FILE_ID, fileId);
                transformErrorLog.put(Constants.FILE_NAME, fileName);
                transformErrorLog.put(Constants.STATUS, Constants.FAILED);
                transformErrorLog.put(Constants.ERROR_KEY, loadContentErrorMessage);
                logStatus.getErrorLogs().add(transformErrorLog);
                logStatus.setHasFailures(true);
                String logFileName = fileName + "_" + partnerCode + Constants.LOG_TEXT;
                File logFile = writeLogsToFile(logStatus.getErrorLogs(), logFileName);
                result.put(Constants.LOG_FILE, logFile);
                result.put(Constants.HAS_FAILURES, logStatus.isHasFailures());
                return result;
            } else {
                result = updateProcessedDataInDb(processedData, partnerCode, fileName, fileId, partnerId,transformContentJson);
            }
        }
        return result;
    }

    public Map<String, Object> validatePayloadAndWriteLogsToFile(String dataPayloadValidationFile, JsonNode transformData, String fileName, String partnerCode,String fileId,String partnerId,Timestamp currentTime){
        log.info("DataTransformUtility :: validatePayloadAndWriteLogsToFile");
        List<CornellContentEntity> cornellContentEntityList = new ArrayList<>();
        try {
            transformData.forEach(transformedData -> {
                JsonNode contentNode = transformedData.get("content");
                Map<String, String> row = objectMapper.convertValue(contentNode, new TypeReference<Map<String, String>>() {
                });
                LinkedHashMap<String, String> linkedRow = new LinkedHashMap<>(row);
                List<String> validationErrors = validateRowData(dataPayloadValidationFile, transformedData);
                if (validationErrors.isEmpty()) {
                    linkedRow.put(Constants.STATUS, Constants.SUCCESS);
                    linkedRow.put(Constants.ERROR_KEY, "");
                    logStatus.getSuccessLogs().add(linkedRow);
                    addSearchTags(transformedData);
                    parseCourseProviderIfPresent(contentNode);
                    String externalId = transformedData.path(Constants.CONTENT).path(Constants.EXTERNAL_ID).asText();
                    CornellContentEntity cornellContentEntity = saveOrUpdateCornellContent(externalId, transformedData, transformedData, currentTime, fileId, partnerId, partnerCode);
                    cornellContentEntityList.add(cornellContentEntity);
                } else {
                    linkedRow.put(Constants.STATUS, Constants.FAILED);
                    linkedRow.put("error", String.join(", ", validationErrors));
                    logStatus.getErrorLogs().add(linkedRow);
                    logStatus.setHasFailures(true);
                }
            });
            log.info("Data validation completed for file: {}", fileName);
            dataBulkSave(cornellContentEntityList, partnerCode);
            List<LinkedHashMap<String, String>> combinedLogs = new ArrayList<>(logStatus.getSuccessLogs());
            combinedLogs.addAll(logStatus.getErrorLogs());

            // Write logs to a local file
            String logFileName = fileName + "_" + partnerCode + Constants.LOG_TEXT;
            File logFile = writeLogsToFile(combinedLogs, logFileName);
            log.info("Log file created locally at: {}", logFile.getAbsolutePath());

            Map<String, Object> result = new HashMap<>();
            result.put(Constants.LOG_FILE, logFile);
            result.put(Constants.HAS_FAILURES, logStatus.isHasFailures());
            return result;
        } catch (Exception e) {
            log.error("Error while validating payload for file: {}", fileName, e);
            throw new RuntimeException(e);
        }
    }

    private void parseCourseProviderIfPresent(JsonNode contentNode) {
        if (!contentNode.has(Constants.COURSE_PROVIDER)) {
            return;
        }
        JsonNode courseProviderNode = contentNode.get(Constants.COURSE_PROVIDER);
        if (courseProviderNode == null || !courseProviderNode.isTextual()) {
            return;
        }
        try {
            String courseProviderStr = courseProviderNode.asText();
            JsonNode parsedCourseProvider = objectMapper.readTree(courseProviderStr);
            ((ObjectNode) contentNode).set(Constants.COURSE_PROVIDER, parsedCourseProvider);
            log.debug("Parsed courseProvider from JSON string to array for content");
        } catch (Exception e) {
            log.warn("Failed to parse courseProvider as JSON: {}", e.getMessage());
        }
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
                    header.append(escapeSpecialCharacters(key)).append(",");
                }
                header.append(Constants.TIME);
                writer.write(header.toString());
                writer.write(System.lineSeparator());
                for (LinkedHashMap<String, String> logEntry : logs) {
                    StringBuilder row = new StringBuilder();
                    for (String key : firstLog.keySet()) {
                        row.append(escapeSpecialCharacters(logEntry.getOrDefault(key, ""))).append(",");
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

    public String updateDateFormatFromTimestampForCoursera(Long timestampMillis) {
        Instant instant = Instant.ofEpochMilli(timestampMillis);
        DateTimeFormatter formatter = DateTimeFormatter
                .ofPattern(Constants.DATE_FORMAT)
                .withZone(ZoneId.of(Constants.UTC));
        return formatter.format(instant);
    }

    public String getAdminAccessToken() {
        try {
            String tokenUrl = cbServerProperties.keycloakUrl + cbServerProperties.ssoAdminTokenEndpoint ;

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_FORM_URLENCODED);

            MultiValueMap<String, String> form = new LinkedMultiValueMap<>();
            form.add(Constants.GRANT_TYPE, Constants.PASSWORD);
            form.add(Constants.CLIENTID, Constants.ADMIN_CLI);
            form.add(Constants.USERNAME, cbServerProperties.ssoUsername);
            form.add(Constants.PASSWORD, cbServerProperties.ssoPassword);

            HttpEntity<MultiValueMap<String, String>> entity = new HttpEntity<>(form, headers);

            ResponseEntity<Map<String, Object>> response = restTemplate.exchange(
                    tokenUrl,
                    HttpMethod.POST,
                    entity,
                    new ParameterizedTypeReference<Map<String, Object>>() {}
            );

            if (!response.getStatusCode().is2xxSuccessful() || response.getBody() == null) {
                throw new CiosContentException(Constants.ERROR, "Failed to get token: ");
            }

            return (String) response.getBody().get(Constants.ACCESS_TOKEN);

        } catch (Exception e) {
            log.error("Error while fetching admin access token", e.getMessage());
            throw new CiosContentException("Error while fetching admin access token", e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    public String createSsoConfiguration(String token, Map<String,Object> client){
        try {
            String body = objectMapper.writeValueAsString(client);
            String tokenUrl = cbServerProperties.keycloakUrl + cbServerProperties.ssoConfigCreateApi;
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            headers.setBearerAuth(token);
            HttpEntity<String> entity = new HttpEntity<>(body, headers);
            ResponseEntity<Void> response = restTemplate.exchange(
                    tokenUrl,
                    HttpMethod.POST,
                    entity,
                    Void.class
            );
            String location = response.getHeaders().getFirst(HttpHeaders.LOCATION);

            if (location != null) {
                return location.substring(location.lastIndexOf('/') + 1);
            }

        }catch (Exception e){
            log.error("Error while creating sso configuration", e.getMessage());
            throw new CiosContentException("Error while creating sso configuration in keycloak", e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return  null;
    }

    public void updateSsoConfiguration(String token, String clientId, Map<String, Object> clientPayload) {
        try {
            clientPayload.put("id", clientId);
            String body = objectMapper.writeValueAsString(clientPayload);
            String url = cbServerProperties.keycloakUrl + cbServerProperties.ssoConfigCreateApi + "/" + clientId;

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            headers.setBearerAuth(token);

            HttpEntity<String> entity = new HttpEntity<>(body, headers);

            restTemplate.exchange(
                    url,
                    HttpMethod.PUT,
                    entity,
                    Void.class
            );
            log.info("Successfully updated SSO configuration for client {}", clientId);

        } catch (Exception e) {
            log.error("Error updating SSO client {}", clientId, e);
            throw new CiosContentException(
                    "Error while updating SSO configuration in Keycloak",
                    e.getMessage(),
                    HttpStatus.INTERNAL_SERVER_ERROR
            );
        }
    }

    public Map<String, String> getExistingMappers(String token, String clientUuid) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.setBearerAuth(token);
        HttpEntity<Void> entity = new HttpEntity<>(headers);
        String url = UriComponentsBuilder
                .fromHttpUrl(cbServerProperties.keycloakUrl)
                .path(cbServerProperties.getSsoConfigMapperReadApi())
                .buildAndExpand(clientUuid)
                .toUriString();
        ResponseEntity<JsonNode> response = restTemplate.exchange(
                url,
                HttpMethod.GET,
                entity,
                JsonNode.class
        );
        Map<String, String> mapperNameToId = new HashMap<>();
        if (response.getStatusCode().is2xxSuccessful() && response.getBody() != null) {
            for (JsonNode mapper : response.getBody()) {
                mapperNameToId.put(
                        mapper.path("name").asText(),
                        mapper.path("id").asText()
                );
            }
        }
        return mapperNameToId;
    }

    public void updateProtocolMapper(String token, String text, Map<String, Object> mapper) {
        try {
            String body = objectMapper.writeValueAsString(mapper);
            String url = UriComponentsBuilder
                    .fromHttpUrl(cbServerProperties.keycloakUrl)
                    .path(cbServerProperties.getSsoConfigMapperUpdateApi())
                    .buildAndExpand(text,mapper.get(Constants.ID))
                    .toUriString();
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            headers.setBearerAuth(token);
            HttpEntity<String> entity = new HttpEntity<>(body, headers);
            restTemplate.exchange(
                    url,
                    HttpMethod.PUT,
                    entity,
                    Void.class
            );
            log.info("Successfully updated protocol mapper {}", mapper.get("name"));
        } catch (Exception e) {
            log.error("Error updating protocol mapper {}", mapper.get("name"), e);
        }
    }

    public void createProtocolMapper(String token, String id, Map<String, Object> mapper) {
        try {
            String body = objectMapper.writeValueAsString(mapper);
            String url = UriComponentsBuilder
                    .fromHttpUrl(cbServerProperties.keycloakUrl)
                    .path(cbServerProperties.getSsoConfigMapperReadApi())
                    .buildAndExpand(id)
                    .toUriString();

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            headers.setBearerAuth(token);

            HttpEntity<String> entity = new HttpEntity<>(body, headers);
            restTemplate.postForEntity(url, entity, String.class);
            log.info("Successfully created protocol mapper {}", mapper.get(Constants.NAME));
        } catch (Exception e) {
            log.error("Error creating protocol mapper {}", mapper.get(Constants.NAME), e);
            throw new CiosContentException(
                    "Error while creating protocol mapper in Keycloak",
                    e.getMessage(),
                    HttpStatus.INTERNAL_SERVER_ERROR
            );
        }
    }


}
