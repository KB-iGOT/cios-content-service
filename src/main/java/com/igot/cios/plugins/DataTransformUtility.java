package com.igot.cios.plugins;

import com.bazaarvoice.jolt.Chainr;
import com.bazaarvoice.jolt.JsonUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
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
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.poi.ss.usermodel.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.*;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.multipart.MultipartFile;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.*;

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
                        // Handle date format (assuming date is in a specific format)
                        if (isDate(cellValue)) {
                            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
                            cellValue = dateFormat.format(parseDate(cellValue));
                        } else {
                            cellValue = cellValue.replace("\n", ",").trim();
                        }
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

    private boolean isDate(String value) {
        try {
            parseDate(value);
            return true;
        } catch (Exception e) {
            return false;
        }
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

        // Create the request entity with body and headers
        HttpEntity<Object> entity = new HttpEntity<>(jsonNode, headers);

        // Make the POST request
        ResponseEntity<String> response = restTemplate.postForEntity(url, entity, String.class);
        if (response.getStatusCode().is2xxSuccessful()) {
            return response.getBody();
        } else {
            throw new CiosContentException("Error from update content partner api", HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }


    public JsonNode fetchPartnerInfoUsingApi(String partnerCode) {
        log.info("CiosContentServiceImpl::fetchPartnerInfoUsingApi:fetching partner data by partnerCode");
        String getApiUrl = cbServerProperties.getCbPoresbaseUrl() + cbServerProperties.getPartnerReadEndPoint() + partnerCode;
        Map<String, String> headers = new HashMap<>();
        Map<String, Object> readData = (Map<String, Object>) fetchResultUsingGet(getApiUrl, headers);

        if (readData == null) {
            throw new RuntimeException("Failed to get data from API: Response is null");
        }
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.convertValue(readData, JsonNode.class);
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
            log.error("Validation Error", errorMessage.toString());
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

    public List<String> validateRowData(Map<String, String> row, String schemaFilePath) {
        List<String> invalidErrList = new ArrayList<>();
        try {
            JsonNode rowNode = objectMapper.convertValue(row, JsonNode.class);
            JsonSchemaFactory schemaFactory = JsonSchemaFactory.getInstance();
            InputStream schemaStream = getClass().getResourceAsStream(schemaFilePath);
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

    public List<String> validateRowData(Map<String, String> row, JsonNode schemaNode) {
        List<String> invalidErrList = new ArrayList<>();
        try {
            JsonNode rowNode = objectMapper.convertValue(row, JsonNode.class);
            JsonSchemaFactory schemaFactory = JsonSchemaFactory.getInstance();
            JsonSchema schema = schemaFactory.getSchema(schemaNode);
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

    public void updateProcessedDataInDb(JsonNode processedData, String partnerCode, String fileName, String fileId, List<Object> contentJson,String partnerId) {
        List<CornellContentEntity> cornellContentEntityList = new ArrayList<>();
        processedData.forEach(eachContentData -> {
            JsonNode transformData = transformData(eachContentData, contentJson);
            Timestamp currentTime = new Timestamp(System.currentTimeMillis());
            ((ObjectNode) transformData.path(Constants.CONTENT)).put(Constants.FILE_ID, fileId).asText();
            ((ObjectNode) transformData.path(Constants.CONTENT)).put(Constants.SOURCE, fileName).asText();
            ((ObjectNode) transformData.path(Constants.CONTENT)).put(Constants.PARTNER_CODE, partnerCode).asText();
            ((ObjectNode) transformData.path(Constants.CONTENT)).put(Constants.STATUS, Constants.NOT_INITIATED).asText();
            ((ObjectNode) transformData.path(Constants.CONTENT)).put(Constants.CREATED_DATE, currentTime.toString()).asText();
            ((ObjectNode) transformData.path(Constants.CONTENT)).put(Constants.UPDATED_DATE, currentTime.toString()).asText();
            ((ObjectNode) transformData.path(Constants.CONTENT)).put(Constants.ACTIVE, Constants.ACTIVE_STATUS).asText();
            ((ObjectNode) transformData.path(Constants.CONTENT)).put(Constants.PUBLISHED_ON, "0000-00-00 00:00:00").asText();
            ((ObjectNode) transformData.path(Constants.CONTENT)).put(Constants.PARTNER_ID, partnerId).asText();
            validatePayload(Constants.DATA_PAYLOAD_VALIDATION_FILE, transformData);
            addSearchTags(transformData);
            String externalId = transformData.path(Constants.CONTENT).path(Constants.EXTERNAL_ID).asText();
            CornellContentEntity cornellContentEntity = saveOrUpdateCornellContent(externalId, transformData, eachContentData, currentTime, fileId,partnerId,partnerCode);
            cornellContentEntityList.add(cornellContentEntity);

        });
        cornellBulkSave(cornellContentEntityList, partnerCode);
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
            if(!externalContent.getCiosData().get("content").get("status").equals("live")||externalContent.getCiosData().get("content").get("status").equals("draft")) {
                externalContent.setExternalId(externalId);
                externalContent.setCiosData(transformData);
                externalContent.setIsActive(externalContent.getIsActive());
                externalContent.setCreatedDate(externalContent.getCreatedDate());
                externalContent.setUpdatedDate(currentTime);
                externalContent.setSourceData(rawContentData);
                externalContent.setFileId(fileId);
                externalContent.setPartnerId(partnerId);
                externalContent.setPartnerCode((partnerCode));
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
            externalContent.setPartnerId(fileId);
            externalContent.setPartnerId(partnerId);
            externalContent.setPartnerCode((partnerCode));
            return externalContent;
        }
    }

    private void cornellBulkSave(List<CornellContentEntity> cornellContentEntityList, String partnerCode) {
        log.info("DataTransformUtility :: cornellBulkSave");
        cornellContentRepository.saveAll(cornellContentEntityList);
        cornellContentEntityList.forEach(contentEntity -> {
            try {
                Map<String, Object> entityMap = objectMapper.convertValue(contentEntity, Map.class);
                flattenContentData(entityMap);
                String uniqueId = partnerCode + "_" + contentEntity.getExternalId();
                esUtilService.addDocument(
                        Constants.CIOS_CONTENT_INDEX_NAME,
                        Constants.INDEX_TYPE,
                        uniqueId,
                        entityMap,
                        cbServerProperties.getElasticCiosContentJsonPath()
                );
                //log.info("Added data to ES document for externalId: {}", contentEntity.getExternalId());
            } catch (Exception e) {
                log.error("Error while processing contentEntity with externalId: {}", contentEntity.getExternalId(), e);
            }
        });
        Long totalCourseCount = cornellContentRepository.countByPartnerCode(partnerCode);
        log.info("Total courses onboarded {} for partner {}",totalCourseCount,partnerCode);
        JsonNode response = fetchPartnerInfoUsingApi(partnerCode);
        JsonNode resultData = response.path(Constants.RESULT);
        JsonNode data = resultData.path(Constants.DATA);
        ((ObjectNode) data).put(Constants.TOTAL_COURSES_COUNT, totalCourseCount);
        updatingPartnerInfo(resultData);
    }

    public void flattenContentData(Map<String, Object> entityMap) {
        if (entityMap.containsKey("ciosData") && entityMap.get("ciosData") instanceof Map) {
            Map<String, Object> ciosDataMap = (Map<String, Object>) entityMap.get("ciosData");
            if (ciosDataMap.containsKey("content") && ciosDataMap.get("content") instanceof Map) {
                Map<String, Object> contentMap = (Map<String, Object>) ciosDataMap.get("content");
                entityMap.putAll(contentMap);
                entityMap.remove(Constants.CIOS_DATA);
                entityMap.remove(Constants.SOURCE_DATA);
            }
        }
    }
}
