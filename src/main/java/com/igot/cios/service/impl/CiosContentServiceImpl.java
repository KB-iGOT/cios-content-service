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
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;


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
    private StoreFileToGCP storeFileToGCP;
    @Autowired
    private CbServerProperties cbServerProperties;

    @Override
    public SBApiResponse loadContentFromExcel(MultipartFile file, String partnerCode, String partnerId) {
        log.info("CiosContentServiceImpl::loadContentFromExcel");
        SBApiResponse response = SBApiResponse.createDefaultResponse(Constants.API_CIOS_LOAD_EXCEL_CONTENT);
        try {
            String fileName = file.getOriginalFilename();
            if (!isValidFileFormat(fileName)) {
                log.error("Invalid file format for file: {}", fileName);
                response.getParams().setStatus(Constants.FAILED);
                response.getParams().setErrmsg("Invalid file format. Only Excel (.xlsx, .xls) or CSV (.csv) files are supported.");
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                return response;
            }
            SBApiResponse uploadResponse = storeFileToGCP.uploadCiosContentFile(file, cbServerProperties.getCiosCloudContainerName(), cbServerProperties.getCiosContentFileCloudFolderName());
            if (!HttpStatus.OK.equals(uploadResponse.getResponseCode())) {
                log.error("File upload failed. Response Code: {}, Error Message: {}",
                        uploadResponse.getResponseCode(),
                        uploadResponse.getParams().getErrmsg());

                response.getParams().setStatus(Constants.FAILED);
                response.getParams().setErrmsg(String.format("Failed to upload file. Error: %s",
                        uploadResponse.getParams().getErrmsg()));
                response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
                return response;
            }
            String contentUploadedGCPFileName = uploadResponse.getResult().get(Constants.NAME).toString();
            Timestamp initiatedOn = new Timestamp(System.currentTimeMillis());
            String fileId = dataTransformUtility.createFileInfo(partnerId, null, fileName, initiatedOn, null, Constants.CONTENT_UPLOAD_IN_PROGRESS, null, contentUploadedGCPFileName);

            Map<String, Object> uploadedFile = new HashMap<>();
            uploadedFile.put(Constants.PARTNER_CODE, partnerCode);
            uploadedFile.put(Constants.FILE_NAME, fileName);
            uploadedFile.put(Constants.INITIATED_ON, initiatedOn);
            uploadedFile.put(Constants.FILE_ID, fileId);
            uploadedFile.put(Constants.PARTNER_ID, partnerId);

            kafkaProducer.push(cbServerProperties.getCiosContentOnboardTopic(), uploadedFile);
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
            log.error("Database access error while fetching content {}", dae.getMessage());
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
            List<Object> contentJson = objectMapper.convertValue(entity.get("transformProgressJson"), new TypeReference<List<Object>>() {
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
        String partnerCode = deleteContentRequestDto.getPartnerCode();
        List<String> externalIds = deleteContentRequestDto.getExternalId();
        List<CornellContentEntity> entities = repository.findByExternalIdInAndPartnerCode(externalIds, partnerCode);

        List<String> errors = new ArrayList<>();
        for (String id : externalIds) {
            Optional<CornellContentEntity> entityOpt = findEntityByExternalId(entities, id);
            if (entityOpt.isEmpty()) {
                errors.add(Constants.EXTERNAL_ID_ERROR + id + " does not exist.");
            } else {
                CornellContentEntity entity = entityOpt.get();
                if (Boolean.TRUE.equals(entity.getIsActive())) {
                    errors.add(Constants.EXTERNAL_ID_ERROR + id + " is live, cannot delete.");
                } else {
                    String error = handleDeletionIfAllowed(entity, deleteContentRequestDto);
                    if (error != null) {
                        errors.add(error);
                    }
                }
            }
        }

        Long totalCourseCount = repository.countByPartnerCode(partnerCode);
        JsonNode contentPartnerResponse = dataTransformUtility.fetchPartnerInfoUsingApi(partnerCode);
        JsonNode data = contentPartnerResponse.path(Constants.DATA);
        if (data.isMissingNode() || !data.isObject()) {
            ObjectNode dataNode = com.fasterxml.jackson.databind.node.JsonNodeFactory.instance.objectNode();
            ((ObjectNode) contentPartnerResponse).set(Constants.DATA, dataNode);
            data = dataNode;
        }
        ((ObjectNode) data).put(Constants.TOTAL_COURSES_COUNT, totalCourseCount);
        dataTransformUtility.updatingPartnerInfo(contentPartnerResponse);

        if (!errors.isEmpty()) {
            log.error("Validation errors: {}", errors);
            return buildErrorResponse(response, String.join("\n", errors));
        }
        response.getResult().put(Constants.STATUS, Constants.SUCCESS);
        response.getResult().put(Constants.MESSAGE, "Content deleted successfully.");
        return ResponseEntity.ok(response);
    }

    private Optional<CornellContentEntity> findEntityByExternalId(List<CornellContentEntity> entities, String externalId) {
        return entities.stream()
                .filter(e -> externalId.equals(e.getExternalId()))
                .findFirst();
    }

    private String handleDeletionIfAllowed(CornellContentEntity entity, DeleteContentRequestDto dto) {
        JsonNode ciosData = entity.getCiosData();
        if (ciosData == null || !ciosData.path(Constants.CONTENT).has("status")) {
            return "External ID: " + entity.getExternalId() + " does not have a valid status in ciosData.";
        }

        String status = ciosData.path(Constants.CONTENT).get("status").asText();
        boolean canDelete = Constants.NOT_INITIATED.equalsIgnoreCase(status) || Constants.DRAFT.equalsIgnoreCase(status);
        if (canDelete) {
            repository.delete(entity);
            String uniqueId = dto.getPartnerCode() + "_" + entity.getExternalId();
            esUtilService.deleteDocument(uniqueId, Constants.CIOS_CONTENT_INDEX_NAME);
            return null;
        } else {
            return "External ID: " + entity.getExternalId() + " cannot be deleted because its status is not 'notInitiated'.";
        }
    }

    private ResponseEntity<?> buildErrorResponse(SBApiResponse response, String errorMessage) {
        response.getParams().setStatus(Constants.FAILED);
        response.getParams().setErr(errorMessage);
        response.setResponseCode(HttpStatus.BAD_REQUEST);
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(response);
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
            return esUtilService.searchDocuments(Constants.CIOS_CONTENT_INDEX_NAME, searchCriteria);
        } catch (Exception e) {
            throw new CiosContentException("ERROR", e.getMessage(), HttpStatus.BAD_REQUEST);
        }
    }

    @Override
    public Object updateContent(JsonNode jsonNode) {
        log.info("CiosContentServiceImpl::updateContent");
        String partnerCode = jsonNode.path(Constants.CONTENT).get("contentPartner").get("partnerCode").asText();
        String partnerId = jsonNode.path(Constants.CONTENT).get("contentPartner").get("id").asText();
        Timestamp currentTime = new Timestamp(System.currentTimeMillis());
        String externalId = jsonNode.path(Constants.CONTENT).get("externalId").asText();
        boolean isActive = jsonNode.path(Constants.CONTENT).get("isActive").asBoolean(false);
        return saveOrUpdateContent(externalId,jsonNode,currentTime,isActive,partnerCode,partnerId);
    }

    private CornellContentEntity saveOrUpdateContent(String externalId, JsonNode transformData, Timestamp currentTime, boolean isActive, String partnerCode,String partnerId) {
        ((ObjectNode) transformData.path(Constants.CONTENT)).put(Constants.PARTNER_CODE, partnerCode).asText();
        addSearchTags(transformData);
        CornellContentEntity externalContent;
        Optional<CornellContentEntity> optExternalContent = repository.findByExternalIdAndPartnerId(externalId,partnerId);
        if (optExternalContent.isPresent()) {
            externalContent = optExternalContent.get();
            externalContent.setCreatedDate(externalContent.getCreatedDate());
        } else {
            externalContent = new CornellContentEntity();
            externalContent.setCreatedDate(currentTime);
        }
        externalContent.setExternalId(externalId);
        externalContent.setCiosData(transformData);
        externalContent.setIsActive(isActive);
        externalContent.setUpdatedDate(currentTime);
        externalContent.setPartnerCode(partnerCode);
        externalContent.setPartnerId(partnerId);
        repository.save(externalContent);
        Map<String, Object> entityMap = objectMapper.convertValue(externalContent, Map.class);
        dataTransformUtility.flattenContentData(entityMap);
        String uniqueId = partnerCode + "_" + externalContent.getExternalId();
        esUtilService.updateDocument(Constants.CIOS_CONTENT_INDEX_NAME,
                uniqueId,
                entityMap,
                cbServerProperties.getElasticCiosContentJsonPath()
        );
        return externalContent;
    }

    private void addSearchTags(JsonNode transformData) {
        List<String> searchTags = new ArrayList<>();
        searchTags.add(transformData.path(Constants.CONTENT).get(Constants.NAME).textValue().toLowerCase());
        ArrayNode searchTagsArray = objectMapper.valueToTree(searchTags);
        ((ObjectNode) transformData.path(Constants.CONTENT)).set(Constants.CONTENT_SEARCH_TAGS, searchTagsArray);
    }
    private boolean isValidFileFormat(String fileName) {
        if (fileName == null) {
            return false;
        }
        String lowerCaseFileName = fileName.toLowerCase();
        return lowerCaseFileName.endsWith(".xlsx") || lowerCaseFileName.endsWith(".xls") || lowerCaseFileName.endsWith(".csv");
    }

}
