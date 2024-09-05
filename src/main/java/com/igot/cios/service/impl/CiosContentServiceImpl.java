package com.igot.cios.service.impl;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cios.constant.CiosConstants;
import com.igot.cios.constant.ContentSource;
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
import com.igot.cios.repository.ContentPartnerRepository;
import com.igot.cios.repository.FileInfoRepository;
import com.igot.cios.service.CiosContentService;
import com.igot.cios.util.CbServerProperties;
import com.igot.cios.util.Constants;
import com.igot.cios.util.PayloadValidation;
import com.igot.cios.util.cache.CacheService;
import lombok.extern.slf4j.Slf4j;
import org.apache.poi.ss.usermodel.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
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

    @Override
    public void loadContentFromExcel(MultipartFile file, String providerName) {
        log.info("CiosContentServiceImpl::loadJobsFromExcel");
        String fileName = file.getOriginalFilename();
        Timestamp initiatedOn = new Timestamp(System.currentTimeMillis());
        String fileId = dataTransformUtility.createFileInfo(null, null, fileName, initiatedOn, null, null);
        try {
            List<Map<String, String>> processedData = dataTransformUtility.processExcelFile(file);
            log.info("No.of processedData from excel: " + processedData.size());
            JsonNode jsonData = objectMapper.valueToTree(processedData);
            ContentSource contentSource = ContentSource.fromProviderName(providerName);
            if (contentSource == null) {
                log.warn("Unknown provider name: " + providerName);
                return;
            }
            JsonNode entity = dataTransformUtility.fetchPartnerInfoUsingApi(providerName);
            List<Object> contentJson = objectMapper.convertValue(entity.path("result").path("trasformContentJson"), new TypeReference<List<Object>>() {
            });
            if (contentJson == null || contentJson.isEmpty()) {
                throw new CiosContentException("Transformation data not present in content partner db", HttpStatus.INTERNAL_SERVER_ERROR);
            }
            ContentPartnerPluginService service = contentPartnerServiceFactory.getContentPartnerPluginService(contentSource);
            service.loadContentFromExcel(jsonData, providerName, fileName, fileId, contentJson);
            Timestamp completedOn = new Timestamp(System.currentTimeMillis());
            dataTransformUtility.createFileInfo(entity.path("result").get("id").asText(), fileId, fileName, initiatedOn, completedOn, Constants.CONTENT_UPLOAD_SUCCESSFULLY);
        } catch (Exception e) {
            ContentSource contentSource = ContentSource.fromProviderName(providerName);
            if (contentSource == null) {
                log.warn("Unknown provider name: " + providerName);
                return;
            }
            JsonNode entity = dataTransformUtility.fetchPartnerInfoUsingApi(providerName);
            dataTransformUtility.createFileInfo(entity.get("id").asText(), fileId, fileName, initiatedOn, new Timestamp(System.currentTimeMillis()), Constants.CONTENT_UPLOAD_FAILED);
            throw new RuntimeException(e.getMessage());
        }
    }

    @Override
    public PaginatedResponse<?> fetchAllContentFromSecondaryDb(RequestDto dto) {
        log.info("CiosContentServiceImpl::fetchAllCornellContentFromDb");
        ContentSource contentSource = ContentSource.fromProviderName(dto.getProviderName());
        if (contentSource == null) {
            log.warn("Unknown provider name: " + dto.getProviderName());
            return null;
        }
        try {
            ContentPartnerPluginService service = contentPartnerServiceFactory.getContentPartnerPluginService(contentSource);
            Page<?> pageData = service.fetchAllContentFromSecondaryDb(dto);
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
    public void loadContentProgressFromExcel(MultipartFile file, String providerName) {
        try {
            List<Map<String, String>> processedData = dataTransformUtility.processExcelFile(file);
            log.info("No.of processedData from excel: " + processedData.size());
            JsonNode jsonData = objectMapper.valueToTree(processedData);
            jsonData.forEach(
                    eachContentData -> {
                        callEnrollmentAPI(eachContentData, providerName);
                    });
        } catch (Exception e) {
            throw new CiosContentException(e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    private void callEnrollmentAPI(JsonNode rawContentData, String providerName) {
        try {
            log.info("CiosContentServiceImpl::saveOrUpdateContentFromProvider");
            JsonNode entity = dataTransformUtility.fetchPartnerInfoUsingApi(providerName);
            List<Object> contentJson = Collections.singletonList(entity.path("transformProgressJson").asText());
            JsonNode transformData = dataTransformUtility.transformData(rawContentData, contentJson);
            payloadValidation.validatePayload(CiosConstants.PROGRESS_DATA_VALIDATION_FILE, transformData);
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
            throw new CiosContentException(CiosConstants.ERROR, "Database access error: " + dae.getMessage());
        } catch (Exception e) {
            throw new CiosContentException(CiosConstants.ERROR, e.getMessage());
        }
    }

    @Override
    public ResponseEntity<?> deleteNotPublishContent(DeleteContentRequestDto deleteContentRequestDto) {
        log.info("CiosContentServiceImpl:: deleteNotPublishContent: deleting non-published content");
        String partnerName = deleteContentRequestDto.getPartnerName();
        ContentSource contentSource = ContentSource.fromProviderName(partnerName);
        if (contentSource == null) {
            log.warn("Unknown provider name: " + partnerName);
            return null;
        }
        List<String> externalIds = deleteContentRequestDto.getExternalId();
        ContentPartnerPluginService service = contentPartnerServiceFactory.getContentPartnerPluginService(contentSource);
        try {
            List<?> allContent = service.fetchAllContentByPartnerName();
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
                service.deleteContent(content);
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
}
