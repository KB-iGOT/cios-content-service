package com.igot.cios.service.impl;

import com.bazaarvoice.jolt.Chainr;
import com.bazaarvoice.jolt.JsonUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.igot.cios.constant.CiosConstants;
import com.igot.cios.constant.ContentSource;
import com.igot.cios.dto.DeleteContentRequestDto;
import com.igot.cios.dto.PaginatedResponse;
import com.igot.cios.dto.RequestDto;
import com.igot.cios.entity.ContentPartnerEntity;
import com.igot.cios.entity.CornellContentEntity;
import com.igot.cios.entity.FileInfoEntity;
import com.igot.cios.entity.UpgradContentEntity;
import com.igot.cios.exception.CiosContentException;
import com.igot.cios.kafka.KafkaProducer;
import com.igot.cios.repository.ContentPartnerRepository;
import com.igot.cios.repository.CornellContentRepository;
import com.igot.cios.repository.FileInfoRepository;
import com.igot.cios.repository.UpgradContentRepository;
import com.igot.cios.service.CiosContentService;
import com.igot.cios.util.CiosServerProperties;
import com.igot.cios.util.Constants;
import com.igot.cios.util.PayloadValidation;
import com.igot.cios.util.cache.CacheService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.poi.ss.usermodel.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.*;


@Service
@Slf4j
public class CiosContentServiceImpl implements CiosContentService {
    @Autowired
    private CornellContentRepository contentRepository;
    @Autowired
    private UpgradContentRepository upgradContentRepository;
    @Autowired
    ObjectMapper objectMapper;
    @Autowired
    KafkaProducer kafkaProducer;
    @Autowired
    PayloadValidation payloadValidation;
    @Autowired
    private ContentPartnerRepository contentPartnerRepository;
    @Autowired
    private CacheService cacheService;
    @Value("${spring.kafka.cornell.topic.name}")
    private String topic;
    @Value("${cornell.progress.transformation.source-to-target.spec.path}")
    private String progressPathOfTragetFile;
    @Autowired
    private FileInfoRepository fileInfoRepository;
    @Autowired
    private RestTemplate restTemplate;
    @Autowired
    private CiosServerProperties ciosServerProperties;

    @Override
    public void loadContentFromExcel(MultipartFile file, String providerName) {
        log.info("CiosContentServiceImpl::loadJobsFromExcel");
        String fileName = file.getOriginalFilename();
        Timestamp initiatedOn = new Timestamp(System.currentTimeMillis());
        String fileId = createFileInfo(null,null, fileName, initiatedOn, null, null);
        try {
            List<Map<String, String>> processedData = processExcelFile(file);
            log.info("No.of processedData from excel: " + processedData.size());
            JsonNode jsonData = objectMapper.valueToTree(processedData);
            jsonData.forEach(node -> {
                if (node instanceof ObjectNode) {
                    ((ObjectNode) node).put("lessonSource", fileName);
                }
            });
            ContentSource contentSource = ContentSource.fromProviderName(providerName);
            if (contentSource == null) {
                log.warn("Unknown provider name: " + providerName);
                return;
            }
            ContentPartnerEntity entity = getContentDetailsByPartnerName(providerName);
            List<Object> contentJson = objectMapper.convertValue(entity.getTrasformContentJson(), new TypeReference<List<Object>>() {
            });
            if (contentJson == null || contentJson.isEmpty()) {
                throw new CiosContentException("Transformation data not present in content partner db", HttpStatus.INTERNAL_SERVER_ERROR);
            }
            List<CornellContentEntity> cornellContentEntityList = new ArrayList<>();
            List<UpgradContentEntity> upgradContentEntityList = new ArrayList<>();
            switch (contentSource) {
                case CORNELL:
                    log.info("inside cornell data");
                    jsonData.forEach(eachContentData -> {
                        JsonNode transformData = transformData(eachContentData, contentJson);
                        ((ObjectNode)transformData.path("content")).put("fileId",fileId).asText();
                        payloadValidation.validatePayload(CiosConstants.DATA_PAYLOAD_VALIDATION_FILE, transformData);
                        String externalId = transformData.path("content").path("externalId").asText();
                        Timestamp currentTime = new Timestamp(System.currentTimeMillis());
                        CornellContentEntity cornellContentEntity = saveOrUpdateCornellContent(externalId, transformData, eachContentData, currentTime,fileId);
                        cornellContentEntityList.add(cornellContentEntity);

                    });
                    cornellBulkSave(cornellContentEntityList, providerName);
                    break;
                case UPGRAD:
                    log.info("inside upgrad data");
                    jsonData.forEach(eachContentData -> {
                        JsonNode transformData = transformData(eachContentData, contentJson);
                        ((ObjectNode)transformData.path("content")).put("fileId",fileId).asText();
                        payloadValidation.validatePayload(CiosConstants.DATA_PAYLOAD_VALIDATION_FILE, transformData);
                        String externalId = transformData.path("content").path("externalId").asText();
                        Timestamp currentTime = new Timestamp(System.currentTimeMillis());
                        UpgradContentEntity upgradContentEntity = saveOrUpdateUpgradContent(externalId, transformData, eachContentData, currentTime,fileId);
                        upgradContentEntityList.add(upgradContentEntity);
                    });
                    upgradBulkSave(upgradContentEntityList, providerName);
                    break;
            }
            Timestamp completedOn = new Timestamp(System.currentTimeMillis());
            createFileInfo(entity.getId(),fileId, fileName, initiatedOn, completedOn, Constants.CONTENT_UPLOAD_SUCCESSFULLY);
        }catch (Exception e){
            ContentSource contentSource = ContentSource.fromProviderName(providerName);
            if (contentSource == null) {
                log.warn("Unknown provider name: " + providerName);
                return;
            }
            ContentPartnerEntity entity = getContentDetailsByPartnerName(providerName);
            createFileInfo(entity.getId(),fileId, fileName, initiatedOn, new Timestamp(System.currentTimeMillis()), Constants.CONTENT_UPLOAD_FAILED);
            throw new RuntimeException(e.getMessage());
        }
    }

    private void upgradBulkSave(List<UpgradContentEntity> upgradContentEntityList,String providerName) {
        upgradContentRepository.saveAll(upgradContentEntityList);
        Long totalCourseCount= upgradContentRepository.count();
        JsonNode responseJson = fetchPartnerInfoUsingApi(providerName);
        JsonNode resultData = responseJson.path(Constants.RESULT);
        JsonNode data = resultData.path(Constants.DATA);
        ((ObjectNode) data).put(Constants.TOTAL_COURSE_COUNT, totalCourseCount);
        ((ObjectNode) resultData).remove(Constants.DATA);
        ((ObjectNode) resultData).setAll((ObjectNode) (data));
        ((ObjectNode) resultData).remove(Constants.UPDATED_ON);
        ((ObjectNode) resultData).remove(Constants.CREATED_ON);
        if (resultData.path(Constants.CONTENT_UPLOAD_LAST_UPDATED_DATE).isNull()) {
            ((ObjectNode) resultData).put(Constants.CONTENT_PROGRESS_LAST_UPDATED_DATE, "0000-01-01T00:00:00Z");
        }
        if (resultData.path(Constants.CONTENT_PROGRESS_LAST_UPDATED_DATE).isNull()) {
            ((ObjectNode) resultData).put(Constants.CONTENT_PROGRESS_LAST_UPDATED_DATE, "0000-01-01T00:00:00Z");
        }
        updatingPartnerInfo(resultData, providerName);
    }

    private void cornellBulkSave(List<CornellContentEntity> cornellContentEntityList, String providerName) {
        contentRepository.saveAll(cornellContentEntityList);
        Long totalCourseCount = contentRepository.count();
        JsonNode responseJson = fetchPartnerInfoUsingApi(providerName);
        JsonNode resultData = responseJson.path(Constants.RESULT);
        JsonNode data = resultData.path(Constants.DATA);
        ((ObjectNode) data).put(Constants.TOTAL_COURSE_COUNT, totalCourseCount);
        ((ObjectNode) resultData).remove(Constants.DATA);
        ((ObjectNode) resultData).setAll((ObjectNode) (data));
        ((ObjectNode) resultData).remove(Constants.UPDATED_ON);
        ((ObjectNode) resultData).remove(Constants.CREATED_ON);
        if (resultData.path(Constants.CONTENT_UPLOAD_LAST_UPDATED_DATE).isNull()) {
            ((ObjectNode) resultData).put(Constants.CONTENT_PROGRESS_LAST_UPDATED_DATE, "0000-01-01T00:00:00Z");
        }
        if (resultData.path(Constants.CONTENT_PROGRESS_LAST_UPDATED_DATE).isNull()) {
            ((ObjectNode) resultData).put(Constants.CONTENT_PROGRESS_LAST_UPDATED_DATE, "0000-01-01T00:00:00Z");
        }
        updatingPartnerInfo(resultData, providerName);
    }

    @Override
    public PaginatedResponse<?> fetchAllContentFromDb(RequestDto dto) {
        log.info("CiosContentServiceImpl::fetchAllCornellContentFromDb");
        Pageable pageable= PageRequest.of(dto.getPage(), dto.getSize());
        ContentSource contentSource = ContentSource.fromProviderName(dto.getProviderName());
        if (contentSource == null) {
            log.warn("Unknown provider name: " + dto.getProviderName());
            return null;
        }
        try {
            Page<?> pageData = null;
            switch (contentSource) {
                case CORNELL:
                    Page<CornellContentEntity>  cornellPageData= contentRepository.findAllCiosDataAndIsActive(dto.getIsActive(),pageable,dto.getKeyword());
                    pageData = cornellPageData;
                    break;
                case UPGRAD:
                    Page<UpgradContentEntity> upgradPageData= upgradContentRepository.findAllCiosDataAndIsActive(dto.getIsActive(),pageable,dto.getKeyword());
                    pageData = upgradPageData;
                    break;
            }
            return new PaginatedResponse<>(
                    pageData.getContent(),
                    pageData.getTotalPages(),
                    pageData.getTotalElements(),
                    pageData.getNumberOfElements(),
                    pageData.getSize(),
                    pageData.getNumber()
            );
        } catch (DataAccessException dae) {
            log.error("Database access error while fetching content", dae.getMessage());
            throw new CiosContentException(CiosConstants.ERROR, "Database access error: " + dae.getMessage());
        } catch (Exception e) {
            throw new CiosContentException(CiosConstants.ERROR, e.getMessage());
        }

    }

    @Override
    public void loadContentProgressFromExcel(MultipartFile file,String providerName) {
        try {
            List<Map<String, String>> processedData = processExcelFile(file);
            log.info("No.of processedData from excel: " + processedData.size());
            JsonNode jsonData = objectMapper.valueToTree(processedData);
            jsonData.forEach(
                    eachContentData -> {
                        callCornellEnrollmentAPI(eachContentData,providerName);
                    });
        }catch (Exception e){
            throw new CiosContentException(e.getMessage(),HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @Override
    public ContentPartnerEntity getContentDetailsByPartnerName(String name) {
        log.info("CiosContentService:: ContentPartnerEntity: getContentDetailsByPartnerName {}",name);
        try {
            ContentPartnerEntity entity=null;
            String cachedJson = cacheService.getCache(Constants.REDIS_KEY_PREFIX+name);
            if (StringUtils.isNotEmpty(cachedJson)) {
                log.info("Record coming from redis cache");
                entity=objectMapper.readValue(cachedJson, new TypeReference<ContentPartnerEntity>() {});
                return entity;
            } else {
                Optional<ContentPartnerEntity> entityOptional = contentPartnerRepository.findByContentPartnerName(name);
                if (entityOptional.isPresent()) {
                    log.info("Record coming from postgres db");
                    entity = entityOptional.get();
                    cacheService.putCache(Constants.CONTENT_PARTNER_REDIS_KEY_PREFIX+name,entity);
                    return entity;

                } else {
                throw new CiosContentException("Content Partner Data not present in DB",HttpStatus.BAD_REQUEST);
                }
            }
        } catch (Exception e) {
            log.error("error while processing", e);
            throw new CiosContentException(e.getMessage(),HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    private UpgradContentEntity saveOrUpdateUpgradContent(String externalId, JsonNode transformData, JsonNode rawContentData, Timestamp currentTime, String fileId) {
        Optional<UpgradContentEntity> optExternalContent = upgradContentRepository.findByExternalId(externalId);
        if (optExternalContent.isPresent()) {
            UpgradContentEntity externalContent = optExternalContent.get();
            externalContent.setExternalId(externalId);
            externalContent.setCiosData(transformData);
            externalContent.setIsActive(externalContent.getIsActive());
            externalContent.setCreatedDate(externalContent.getCreatedDate());
            externalContent.setUpdatedDate(currentTime);
            externalContent.setSourceData(rawContentData);
            externalContent.setFileId(fileId);
            return externalContent;
        } else {
            UpgradContentEntity externalContent = new UpgradContentEntity();
            externalContent.setExternalId(externalId);
            externalContent.setCiosData(transformData);
            externalContent.setIsActive(false);
            externalContent.setCreatedDate(currentTime);
            externalContent.setUpdatedDate(currentTime);
            externalContent.setSourceData(rawContentData);
            return externalContent;
        }
    }

    private CornellContentEntity saveOrUpdateCornellContent(String externalId, JsonNode transformData, JsonNode rawContentData, Timestamp currentTime,String fileId) {
        Optional<CornellContentEntity> optExternalContent = contentRepository.findByExternalId(externalId);
        if (optExternalContent.isPresent()) {
            CornellContentEntity externalContent = optExternalContent.get();
            externalContent.setExternalId(externalId);
            externalContent.setCiosData(transformData);
            externalContent.setIsActive(externalContent.getIsActive());
            externalContent.setCreatedDate(externalContent.getCreatedDate());
            externalContent.setUpdatedDate(currentTime);
            externalContent.setSourceData(rawContentData);
            externalContent.setFileId(fileId);
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
            return externalContent;
        }

    }

    private void callCornellEnrollmentAPI(JsonNode rawContentData, String providerName) {
        try {
            log.info("CiosContentServiceImpl::saveOrUpdateContentFromProvider");
            ContentPartnerEntity entity = getContentDetailsByPartnerName(providerName);
            List<Object> contentJson = objectMapper.convertValue(entity.getTransformProgressJson(), new TypeReference<List<Object>>() {
            });
            JsonNode transformData = transformData(rawContentData, contentJson);
            payloadValidation.validatePayload(CiosConstants.PROGRESS_DATA_VALIDATION_FILE, transformData);
            kafkaProducer.push(topic, transformData);
            log.info("callCornellEnrollmentAPI {} ", transformData.asText());
        } catch (Exception e) {
            log.error("error while processing", e);
            throw new CiosContentException(e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    private JsonNode transformData(Object sourceObject, List<Object> specJson) {
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

    private JsonNode transformData(Object sourceObject, String destinationPath) {
        log.debug("CiosContentServiceImpl::transformData");
        try {
            String inputJson = objectMapper.writeValueAsString(sourceObject);
            List<Object> specJson = JsonUtils.classpathToList(destinationPath);
            Chainr chainr = Chainr.fromSpec(specJson);
            Object transformedOutput = chainr.transform(JsonUtils.jsonToObject(inputJson));
            return objectMapper.convertValue(transformedOutput, JsonNode.class);
        } catch (JsonProcessingException e) {
            log.error("Error transforming data", e);
            return null;
        }

    }

    private List<Map<String, String>> processExcelFile(MultipartFile incomingFile) {
        log.debug("CiosContentServiceImpl::processExcelFile");
        try {
            return validateFileAndProcessRows(incomingFile);
        } catch (Exception e) {
            log.error("Error occurred during file processing: {}", e.getMessage());
            throw new RuntimeException(e.getMessage());
        }
    }

    private List<Map<String, String>> validateFileAndProcessRows(MultipartFile file) {
        log.debug("CiosContentServiceImpl::validateFileAndProcessRows");
        try (InputStream inputStream = file.getInputStream(); Workbook workbook = WorkbookFactory.create(inputStream)) {
            Sheet sheet = workbook.getSheetAt(0);
            return processSheetAndSendMessage(sheet);
        } catch (IOException e) {
            log.error("Error while processing Excel file: {}", e.getMessage());
            throw new RuntimeException(e.getMessage());
        }
    }

    private List<Map<String, String>> processSheetAndSendMessage(Sheet sheet) {
        log.debug("CiosContentServiceImpl::processSheetAndSendMessage");
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
                    String excelHeader = formatter.formatCellValue(headerCell).replaceAll("[\\n*]", "").trim();
                    String cellValue = "";

                    if (valueCell != null && valueCell.getCellType() != CellType.BLANK) {
                        if (valueCell.getCellType() == CellType.NUMERIC && DateUtil.isCellDateFormatted(valueCell)) {
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
        return dataRows;
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
            throw new CiosContentException(CiosConstants.ERROR, "Database access error: " + dae.getMessage());
        } catch (Exception e) {
            throw new CiosContentException(CiosConstants.ERROR, e.getMessage());
        }
    }

    private JsonNode fetchPartnerInfoUsingApi(String partnerName) {
        log.info("CiosContentServiceImpl::fetchPartnerInfoUsingApi:fetching partner data by partnerName");
        String getApiUrl = ciosServerProperties.getPartnerServiceUrl() + ciosServerProperties.getPartnerReadEndPoint() + partnerName;
        Map<String, String> headers = new HashMap<>();
        //headers.put("Authorization", "Bearer " + ciosServerProperties.getSbApiKey());
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

            StringBuilder strUrl = new StringBuilder(ciosServerProperties.getPartnerServiceUrl());
            strUrl.append(ciosServerProperties.getPartnerCreateEndPoint());
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

    @Override
    public ResponseEntity<?> deleteNotPublishContent(DeleteContentRequestDto deleteContentRequestDto) {
        log.info("CiosContentServiceImpl:: deleteNotPublishContent: deleting non-published content");
        String partnerName = deleteContentRequestDto.getPartnerName();
        List<String> externalIds = deleteContentRequestDto.getExternalId();
        ContentSource contentSource = ContentSource.fromProviderName(partnerName);
        if (contentSource == null) {
            log.warn("Unknown partner name: " + partnerName);
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Invalid partner name.");
        }
        try {
            List<?> allContent = fetchAllContentByPartnerName(contentSource);
            boolean hasValidationErrors = false;
            StringBuilder validationErrors = new StringBuilder();
            List<Object> validContentToDelete = new ArrayList<>();
            for (String externalId : externalIds) {
                Optional<?> contentEntityOpt = allContent.stream()
                        .filter(content -> getExternalId(content).equals(externalId))
                        .findFirst();
                if (contentEntityOpt.isPresent()) {
                    Object contentEntity = contentEntityOpt.get();
                    boolean isActive = getIsActiveStatus(contentEntity);
                    if (isActive) {
                        validationErrors.append("External ID: ").append(externalId)
                                .append(" is live, we can't delete live content.\n");
                        hasValidationErrors = true;
                    } else {
                        validContentToDelete.add(contentEntity);
                    }
                } else {
                    validationErrors.append("External ID: ").append(externalId)
                            .append(" does not exist.\n");
                    hasValidationErrors = true;
                }
            }
            if (hasValidationErrors) {
                log.error("No valid content found for deletion.");
                validationErrors.append("No valid content found for deletion.");
                return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(validationErrors.toString());
            }
            for (Object content : validContentToDelete) {
                deleteContent(content);
            }
            return ResponseEntity.ok("Content deleted successfully.");
        } catch (DataAccessException dae) {
            log.error("Database access error while deleting content", dae.getMessage());
            throw new CiosContentException(CiosConstants.ERROR, "Database access error: " + dae.getMessage());
        } catch (Exception e) {
            log.error("Error occurred while deleting content", e.getMessage());
            throw new CiosContentException(CiosConstants.ERROR, e.getMessage());
        }
    }


    private List<?> fetchAllContentByPartnerName(ContentSource contentSource) {
        switch (contentSource) {
            case CORNELL:
                return contentRepository.findAll();
            case UPGRAD:
                return upgradContentRepository.findAll();
            // Add more cases if needed
            default:
                return Collections.emptyList();
        }
    }

    private String getExternalId(Object contentEntity) {
        if (contentEntity instanceof CornellContentEntity) {
            return ((CornellContentEntity) contentEntity).getExternalId();
        } else if (contentEntity instanceof UpgradContentEntity) {
            return ((UpgradContentEntity) contentEntity).getExternalId();
        }
        return null;
    }

    private boolean getIsActiveStatus(Object contentEntity) {
        if (contentEntity instanceof CornellContentEntity) {
            return ((CornellContentEntity) contentEntity).getIsActive();
        } else if (contentEntity instanceof UpgradContentEntity) {
            return ((UpgradContentEntity) contentEntity).getIsActive();
        }
        return false;
    }

    private void deleteContent(Object contentEntity) {
        if (contentEntity instanceof CornellContentEntity) {
            contentRepository.delete((CornellContentEntity) contentEntity);
        } else if (contentEntity instanceof UpgradContentEntity) {
            upgradContentRepository.delete((UpgradContentEntity) contentEntity);
        }
        // Add more cases if needed
    }

}
