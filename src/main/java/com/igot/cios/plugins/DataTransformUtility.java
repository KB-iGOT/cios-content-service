package com.igot.cios.plugins;

import com.bazaarvoice.jolt.Chainr;
import com.bazaarvoice.jolt.JsonUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.igot.cios.entity.FileInfoEntity;
import com.igot.cios.exception.CiosContentException;
import com.igot.cios.repository.FileInfoRepository;
import com.igot.cios.util.CbServerProperties;
import com.igot.cios.util.Constants;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.ValidationMessage;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.MapUtils;
import org.apache.poi.ss.usermodel.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.*;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.*;

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
        try (InputStream inputStream = file.getInputStream();
             Workbook workbook = WorkbookFactory.create(inputStream)) {
            Sheet sheet = workbook.getSheetAt(0);
            return processSheetAndSendMessage(sheet);
        } catch (IOException e) {
            log.error("Error while processing Excel file: {}", e.getMessage());
            throw new RuntimeException(e.getMessage());
        }
    }

    public String updatingPartnerInfo(JsonNode postData, String partnerName) {
        log.info("CiosContentServiceImpl::updatingPartnerInfo:updating partner data");
        String responseStatus = "";
        try {
            log.info("callPostApi started for partnerName: {}", partnerName);
            ObjectMapper objectMapper = new ObjectMapper();
            Map<String, Object> postDataMap = objectMapper.convertValue(postData, new TypeReference<Map<String, Object>>() {
            });
            Map<String, Object> request = new HashMap<>();
            request.putAll(postDataMap);
            log.info("Prepared request payload: {}", request);
            Map<String, String> headers = new HashMap<>();

            StringBuilder strUrl = new StringBuilder(cbServerProperties.getPartnerServiceUrl());
            strUrl.append(cbServerProperties.getPartnerCreateEndPoint());
            log.info("Constructed POST API URL: {}", strUrl);

            Map<String, Object> postApiResponse = (Map<String, Object>)
                    fetchResultUsingPost(strUrl.toString(), request, headers);

            if (MapUtils.isNotEmpty(postApiResponse) && Constants.OK.equalsIgnoreCase(
                    (String) postApiResponse.get(Constants.RESPONSE_CODE))) {
                Map<String, Object> result = (Map<String, Object>) postApiResponse.get(Constants.RESULT);
                responseStatus = (String) result.getOrDefault(Constants.STATUS, "UNKNOWN");
                log.info("POST API call succeeded with status: {}", responseStatus);
            } else {
                log.error("Failed to execute POST API: {}",
                        postApiResponse.get(Constants.RESPONSE_CODE));
            }
        } catch (Exception e) {
            log.error("Unexpected error occurred in callPostApi for partnerName: {}", partnerName, e);
        }

        return responseStatus;
    }

    public Map<String, Object> fetchResultUsingPost(String uri, Object request, Map<String, String> headersValues) {
        log.info("CiosContentServiceImpl::fetchResultUsingPost:updating partner data by get API call");
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        Map<String, Object> response = null;
        try {
            HttpHeaders headers = new HttpHeaders();
            if (!CollectionUtils.isEmpty(headersValues)) {
                headersValues.forEach((k, v) -> headers.set(k, v));
            }
            headers.setContentType(MediaType.APPLICATION_JSON);
            HttpEntity<Object> entity = new HttpEntity<>(request, headers);
            if (log.isDebugEnabled()) {
                StringBuilder str = new StringBuilder(this.getClass().getCanonicalName()).append(".fetchResult")
                        .append(System.lineSeparator());
                str.append("URI: ").append(uri).append(System.lineSeparator());
                str.append("Request: ").append(mapper.writeValueAsString(request)).append(System.lineSeparator());
                log.debug(str.toString());
            }
            response = restTemplate.postForObject(uri, entity, Map.class);
            if (log.isDebugEnabled()) {
                StringBuilder str = new StringBuilder("Response: ");
                str.append(mapper.writeValueAsString(response)).append(System.lineSeparator());
                log.debug(str.toString());
            }
        } catch (HttpClientErrorException hce) {
            try {
                response = (new ObjectMapper()).readValue(hce.getResponseBodyAsString(),
                        new TypeReference<HashMap<String, Object>>() {
                        });
            } catch (Exception e1) {
            }
            log.error("Error received: " + hce.getResponseBodyAsString(), hce);
        } catch (JsonProcessingException e) {
            log.error(String.valueOf(e));
            try {
                log.warn("Error Response: " + mapper.writeValueAsString(response));
            } catch (Exception e1) {
            }
        }
        return response;
    }

    public JsonNode fetchPartnerInfoUsingApi(String orgId) {
        log.info("CiosContentServiceImpl::fetchPartnerInfoUsingApi:fetching partner data by partnerName");
        String getApiUrl = cbServerProperties.getPartnerServiceUrl() + cbServerProperties.getPartnerReadEndPoint() + orgId;
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

    public String createFileInfo(String partnerId, String fileId, String fileName, Timestamp initiatedOn, Timestamp completedOn, String status) {
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
        fileInfoRepository.save(fileInfoEntity);
        log.info("created successfully fileInfo{}", fileId);
        return fileId;
    }

}
