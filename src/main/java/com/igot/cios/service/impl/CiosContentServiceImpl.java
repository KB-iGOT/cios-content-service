package com.igot.cios.service.impl;

import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.igot.cios.dto.SBApiResponse;
import com.igot.cios.plugins.ContentSource;
import com.igot.cios.dto.DeleteContentRequestDto;
import com.igot.cios.dto.PaginatedResponse;
import com.igot.cios.dto.RequestDto;
import com.igot.cios.entity.CornellContentEntity;
import com.igot.cios.entity.FileInfoEntity;
import com.igot.cios.entity.UpgradContentEntity;
import com.igot.cios.exception.CiosContentException;
import com.igot.cios.kafka.KafkaProducer;
import com.igot.cios.plugins.ContentPartnerPluginService;
import com.igot.cios.plugins.DataTransformUtility;
import com.igot.cios.plugins.config.ContentPartnerServiceFactory;
import com.igot.cios.repository.FileInfoRepository;
import com.igot.cios.service.CiosContentService;
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
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.TimeUnit;


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
        String fileId = dataTransformUtility.createFileInfo(null, null, fileName, initiatedOn, null, null);
        try {
            List<Map<String, String>> processedData = dataTransformUtility.processExcelFile(file);
            log.info("No.of processedData from excel: " + processedData.size());
            JsonNode jsonData = objectMapper.valueToTree(processedData);

            JsonNode entity = dataTransformUtility.fetchPartnerInfoUsingApi(partnerCode);
            List<Object> contentJson = objectMapper.convertValue(entity.path("result").path("trasformContentJson"), new TypeReference<List<Object>>() {
            });
            if (contentJson == null || contentJson.isEmpty()) {
                throw new CiosContentException("Transformation data not present in content partner db", HttpStatus.INTERNAL_SERVER_ERROR);
            }
            ContentPartnerPluginService service = contentPartnerServiceFactory.getContentPartnerPluginService(contentSource);
            service.loadContentFromExcel(jsonData, partnerCode, fileName, fileId, contentJson);
            Timestamp completedOn = new Timestamp(System.currentTimeMillis());
            dataTransformUtility.createFileInfo(entity.path("result").get("id").asText(), fileId, fileName, initiatedOn, completedOn, Constants.CONTENT_UPLOAD_SUCCESSFULLY);
        } catch (Exception e) {
            JsonNode entity = dataTransformUtility.fetchPartnerInfoUsingApi(partnerCode);
            dataTransformUtility.createFileInfo(entity.get("id").asText(), fileId, fileName, initiatedOn, new Timestamp(System.currentTimeMillis()), Constants.CONTENT_UPLOAD_FAILED);
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
    public void loadContentProgressFromExcel(MultipartFile file, String orgId) {
        try {
            List<Map<String, String>> processedData = dataTransformUtility.processExcelFile(file);
            log.info("No.of processedData from excel: " + processedData.size());
            JsonNode jsonData = objectMapper.valueToTree(processedData);
            jsonData.forEach(
                    eachContentData -> {
                        callEnrollmentAPI(eachContentData, orgId);
                    });
        } catch (Exception e) {
            throw new CiosContentException(e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    private void callEnrollmentAPI(JsonNode rawContentData, String orgId) {
        try {
            log.info("CiosContentServiceImpl::saveOrUpdateContentFromProvider");
            JsonNode entity = dataTransformUtility.fetchPartnerInfoUsingApi(orgId);
            List<Object> contentJson = objectMapper.convertValue(entity.path("result").path("transformProgressJson"), new TypeReference<List<Object>>() {
            });
            JsonNode transformData = dataTransformUtility.transformData(rawContentData, contentJson);
            payloadValidation.validatePayload(Constants.PROGRESS_DATA_VALIDATION_FILE, transformData);
            ((ObjectNode) transformData).put("orgId", orgId);
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
        String partnerCode = jsonNode.path("content").get("partnerCode").asText();
        ContentSource contentSource = ContentSource.fromPartnerCode(partnerCode);
        if (contentSource == null) {
            log.warn("Unknown provider name: " + partnerCode);
            throw new CiosContentException("Unknown provider name:" + partnerCode, HttpStatus.BAD_REQUEST);
        }
        ContentPartnerPluginService service = contentPartnerServiceFactory.getContentPartnerPluginService(contentSource);
        return service.updateContent(jsonNode, partnerCode);
    }
}
