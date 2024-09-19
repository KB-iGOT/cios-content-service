package com.igot.cios.plugins.upgrad;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.igot.cios.dto.RequestDto;
import com.igot.cios.entity.CornellContentEntity;
import com.igot.cios.entity.UpgradContentEntity;
import com.igot.cios.exception.CiosContentException;
import com.igot.cios.plugins.ContentPartnerPluginService;
import com.igot.cios.plugins.DataTransformUtility;
import com.igot.cios.repository.UpgradContentRepository;
import com.igot.cios.util.Constants;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Service
public class UpgradPluginServiceImpl implements ContentPartnerPluginService {
    @Autowired
    DataTransformUtility dataTransformUtility;

    @Autowired
    UpgradContentRepository upgradContentRepository;

    @Override
    public void loadContentFromExcel(JsonNode processedData, String orgId, String fileName, String fileId, List<Object> contentJson) {
        List<UpgradContentEntity> upgradContentEntityList = new ArrayList<>();
        processedData.forEach(eachContentData -> {
            JsonNode transformData = dataTransformUtility.transformData(eachContentData, contentJson);
            ((ObjectNode) transformData.path("content")).put("fileId", fileId).asText();
            dataTransformUtility.validatePayload(Constants.DATA_PAYLOAD_VALIDATION_FILE, transformData);
            String externalId = transformData.path("content").path("externalId").asText();
            Timestamp currentTime = new Timestamp(System.currentTimeMillis());
            UpgradContentEntity upgradContentEntity = saveOrUpdateUpgradContent(externalId, transformData, eachContentData, currentTime, fileId);
            upgradContentEntityList.add(upgradContentEntity);
        });
        upgradBulkSave(upgradContentEntityList, orgId);
    }

    private void upgradBulkSave(List<UpgradContentEntity> upgradContentEntityList, String orgId) {
        upgradContentRepository.saveAll(upgradContentEntityList);
        Long totalCourseCount = upgradContentRepository.count();
        JsonNode responseJson = dataTransformUtility.fetchPartnerInfoUsingApi(orgId);
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
        dataTransformUtility.updatingPartnerInfo(resultData, orgId);
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

    @Override
    public Page<?> fetchAllContentFromSecondaryDb(RequestDto dto) {
        Pageable pageable= PageRequest.of(dto.getPage(), dto.getSize());
        return upgradContentRepository.findAllCiosDataAndIsActive(dto.getIsActive(),pageable,dto.getKeyword());
    }

    @Override
    public List<?> fetchAllContent() {
        return upgradContentRepository.findAll();
    }

    @Override
    public void deleteContent(Object contentEntity) {
        upgradContentRepository.delete((UpgradContentEntity) contentEntity);
    }

    @Override
    public Object readContentByExternalId(String externalid) {
        Optional<UpgradContentEntity> entity = upgradContentRepository.findByExternalId(externalid);
        if(entity.isPresent()){
            return entity.get().getCiosData();
        }else{
            throw new CiosContentException("No data found for given id",externalid, HttpStatus.BAD_REQUEST);
        }
    }
}
