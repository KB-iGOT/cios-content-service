package com.igot.cios.service.impl;

import com.bazaarvoice.jolt.Chainr;
import com.bazaarvoice.jolt.JsonUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cios.constant.ContentSource;
import com.igot.cios.dto.PaginatedResponse;
import com.igot.cios.dto.RequestDto;
import com.igot.cios.entity.ContentPartnerEntity;
import com.igot.cios.entity.CornellContentEntity;
import com.igot.cios.entity.UpgradContentEntity;
import com.igot.cios.exception.CiosContentException;
import com.igot.cios.kafka.KafkaProducer;
import com.igot.cios.repository.ContentPartnerRepository;
import com.igot.cios.repository.CornellContentRepository;
import com.igot.cios.repository.UpgradContentRepository;
import com.igot.cios.service.CiosContentService;
import com.igot.cios.util.CbServerProperties;
import com.igot.cios.util.Constants;
import com.igot.cios.util.PayloadValidation;
import com.igot.cios.util.cache.CacheService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.poi.ss.usermodel.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
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
    @Autowired
    private CbServerProperties cbServerProperties;


    @Override
    public void loadContentFromExcel(MultipartFile file,String providerName) {
        log.info("CiosContentServiceImpl::loadJobsFromExcel");
        try {
            List<Map<String, String>> processedData = processExcelFile(file);
            log.info("No.of processedData from excel: " + processedData.size());
            JsonNode jsonData = objectMapper.valueToTree(processedData);
            ContentSource contentSource = ContentSource.fromProviderName(providerName);
            if (contentSource == null) {
                log.warn("Unknown provider name: " + providerName);
                return;
            }
//            ContentPartnerEntity entity=getContentDetailsByPartnerName(providerName);
//            List<Object> contentJson= objectMapper.convertValue(entity.getTrasformContentJson(), new TypeReference<List<Object>>() {});
//            if(contentJson==null||contentJson.isEmpty()){
//                throw new CiosContentException("Transformation data not present in content partner db",HttpStatus.INTERNAL_SERVER_ERROR);
//            }
            List<CornellContentEntity> cornellContentEntityList = new ArrayList<>();
            List<UpgradContentEntity> upgradContentEntityList = new ArrayList<>();
            switch (contentSource) {
                case CORNELL:
                    log.info("inside cornell data");
                    jsonData.forEach(eachContentData -> {
                        JsonNode transformData = transformData(eachContentData, contentSource.getFilePath());
                        payloadValidation.validatePayload(Constants.CORNELL_DATA_PAYLOAD_VALIDATION_FILE, transformData);
                        String externalId = transformData.path("content").path("externalId").asText();
                        Timestamp currentTime = new Timestamp(System.currentTimeMillis());
                        CornellContentEntity cornellContentEntity = saveOrUpdateCornellContent(externalId, transformData, eachContentData, currentTime);
                        cornellContentEntityList.add(cornellContentEntity);
                    });
                    cornellBulkSave(cornellContentEntityList);
                    break;
                case UPGRAD:
                    log.info("inside upgrad data");
                    jsonData.forEach(eachContentData -> {
                        JsonNode transformData = transformData(eachContentData, contentSource.getFilePath());
                        payloadValidation.validatePayload(Constants.CORNELL_DATA_PAYLOAD_VALIDATION_FILE, transformData);
                        String externalId = transformData.path("content").path("externalId").asText();
                        Timestamp currentTime = new Timestamp(System.currentTimeMillis());
                        UpgradContentEntity upgradContentEntity = saveOrUpdateUpgradContent(externalId, transformData, eachContentData, currentTime);
                        upgradContentEntityList.add(upgradContentEntity);
                    });
                    upgradBulkSave(upgradContentEntityList);
                    break;
            }
        }catch (Exception e){
            throw new RuntimeException(e.getMessage());
        }
    }

    private void upgradBulkSave(List<UpgradContentEntity> upgradContentEntityList) {
        upgradContentRepository.saveAll(upgradContentEntityList);
    }

    private void cornellBulkSave(List<CornellContentEntity> cornellContentEntityList) {
        contentRepository.saveAll(cornellContentEntityList);
    }

    @Override
    public PaginatedResponse<Object> fetchAllContentFromDb(RequestDto dto) {
        log.info("CiosContentServiceImpl::fetchAllCornellContentFromDb");
        Pageable pageable= PageRequest.of(dto.getPage(), dto.getSize());
        ContentSource contentSource = ContentSource.fromProviderName(dto.getProviderName());
        if (contentSource == null) {
            log.warn("Unknown provider name: " + dto.getProviderName());
            return null;
        }
        try {
            Page<Object> pageData = null;
            switch (contentSource) {
                case CORNELL:
                    pageData= contentRepository.findAllCiosDataAndIsActive(dto.getIsActive(),pageable);
                    break;
                case UPGRAD:
                    pageData= upgradContentRepository.findAllCiosDataAndIsActive(dto.getIsActive(),pageable);
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
            throw new CiosContentException(Constants.ERROR, "Database access error: " + dae.getMessage());
        } catch (Exception e) {
            throw new CiosContentException(Constants.ERROR, e.getMessage());
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

//    @Override
//    public ContentPartnerEntity getContentDetailsByPartnerName(String name) {
//        log.info("CiosContentService:: ContentPartnerEntity: getContentDetailsByPartnerName {}",name);
//        try {
//            ContentPartnerEntity entity=null;
//            String cachedJson = cacheService.getCache(Constants.REDIS_KEY_PREFIX+name);
//            if (StringUtils.isNotEmpty(cachedJson)) {
//                log.info("Record coming from redis cache");
//                entity=objectMapper.readValue(cachedJson, new TypeReference<ContentPartnerEntity>() {});
//                return entity;
//            } else {
//                Optional<ContentPartnerEntity> entityOptional = contentPartnerRepository.findByContentPartnerName(name);
//                if (entityOptional.isPresent()) {
//                    log.info("Record coming from postgres db");
//                    entity = entityOptional.get();
//                    cacheService.putCache(Constants.REDIS_KEY_PREFIX+name,entity);
//                    return entity;
//
//                } else {
//                throw new CiosContentException("Content Partner Data not present in DB",HttpStatus.BAD_REQUEST);
//                }
//            }
//        } catch (Exception e) {
//            log.error("error while processing", e);
//            throw new CiosContentException(e.getMessage(),HttpStatus.INTERNAL_SERVER_ERROR);
//        }
//    }

    private UpgradContentEntity saveOrUpdateUpgradContent(String externalId, JsonNode transformData, JsonNode rawContentData, Timestamp currentTime) {
        Optional<UpgradContentEntity> optExternalContent = upgradContentRepository.findByExternalId(externalId);
        if (optExternalContent.isPresent()) {
            UpgradContentEntity externalContent = optExternalContent.get();
            externalContent.setExternalId(externalId);
            externalContent.setCiosData(transformData);
            externalContent.setIsActive(externalContent.getIsActive());
            externalContent.setCreatedDate(externalContent.getCreatedDate());
            externalContent.setUpdatedDate(currentTime);
            externalContent.setSourceData(rawContentData);
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

    private CornellContentEntity saveOrUpdateCornellContent(String externalId, JsonNode transformData, JsonNode rawContentData, Timestamp currentTime) {
        Optional<CornellContentEntity> optExternalContent = contentRepository.findByExternalId(externalId);
        if (optExternalContent.isPresent()) {
            CornellContentEntity externalContent = optExternalContent.get();
            externalContent.setExternalId(externalId);
            externalContent.setCiosData(transformData);
            externalContent.setIsActive(externalContent.getIsActive());
            externalContent.setCreatedDate(externalContent.getCreatedDate());
            externalContent.setUpdatedDate(currentTime);
            externalContent.setSourceData(rawContentData);
            return externalContent;
        } else {
            CornellContentEntity externalContent = new CornellContentEntity();
            externalContent.setExternalId(externalId);
            externalContent.setCiosData(transformData);
            externalContent.setIsActive(false);
            externalContent.setCreatedDate(currentTime);
            externalContent.setUpdatedDate(currentTime);
            externalContent.setSourceData(rawContentData);
            return externalContent;
        }

    }
    private void callCornellEnrollmentAPI(JsonNode rawContentData,String providerName) {
        try {
            log.info("CiosContentServiceImpl::saveOrUpdateContentFromProvider");
//            ContentPartnerEntity entity=getContentDetailsByPartnerName(providerName);
//            List<Object> contentJson= objectMapper.convertValue(entity.getTransformProgressJson(), new TypeReference<List<Object>>() {});
            JsonNode transformData = transformData(rawContentData, cbServerProperties.getProgressPathOfTragetFile());
            payloadValidation.validatePayload(Constants.CORNELL_PROGRESS_DATA_VALIDATION_FILE, transformData);
            kafkaProducer.push(cbServerProperties.getTopic(), transformData);
            log.info("callCornellEnrollmentAPI {} ", transformData.asText());
        }catch (Exception e) {
            log.error("error while processing", e);
            throw new CiosContentException(e.getMessage(),HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

//    public JsonNode transformData(Object sourceObject, List<Object> specJson) {
//        log.debug("CiosContentServiceImpl::transformData");
//        try {
//        String inputJson = objectMapper.writeValueAsString(sourceObject);
//            Chainr chainr = Chainr.fromSpec(specJson);
//            Object transformedOutput = chainr.transform(JsonUtils.jsonToObject(inputJson));
//            return objectMapper.convertValue(transformedOutput, JsonNode.class);
//        } catch (JsonProcessingException e) {
//            log.error("Error transforming data", e);
//            return null;
//        }
//
//    }

    public JsonNode transformData(Object sourceObject, String destinationPath) {
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
        try (InputStream inputStream = file.getInputStream();
             Workbook workbook = WorkbookFactory.create(inputStream)) {
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
        return dataRows;
    }
}
