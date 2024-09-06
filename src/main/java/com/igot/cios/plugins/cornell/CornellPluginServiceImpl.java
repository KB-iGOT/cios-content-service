package com.igot.cios.plugins.cornell;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.igot.cios.dto.RequestDto;
import com.igot.cios.entity.CornellContentEntity;
import com.igot.cios.plugins.ContentPartnerPluginService;
import com.igot.cios.plugins.DataTransformUtility;
import com.igot.cios.repository.CornellContentRepository;
import com.igot.cios.util.Constants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Slf4j
@Service
public class CornellPluginServiceImpl implements ContentPartnerPluginService {

    @Autowired
    private CornellContentRepository cornellContentRepository;

    @Autowired
    DataTransformUtility dataTransformUtility;

    @Value("${transformation.source-to-target.spec.path}")
    private String pathOfTragetFile;

    @Override
    public void loadContentFromExcel(JsonNode processedData, String orgId,String fileName,String fileId,List<Object> contentJson){
        List<CornellContentEntity> cornellContentEntityList = new ArrayList<>();
        processedData.forEach(eachContentData -> {
            JsonNode transformData = dataTransformUtility.transformData(eachContentData, contentJson);
            ((ObjectNode)transformData.path("content")).put("fileId",fileId).asText();
            ((ObjectNode)transformData.path("content")).put("source",fileName).asText();
            dataTransformUtility.validatePayload(Constants.DATA_PAYLOAD_VALIDATION_FILE, transformData);
            String externalId = transformData.path("content").path("externalId").asText();
            Timestamp currentTime = new Timestamp(System.currentTimeMillis());
            CornellContentEntity cornellContentEntity = saveOrUpdateCornellContent(externalId, transformData, eachContentData, currentTime,fileId);
            cornellContentEntityList.add(cornellContentEntity);

        });
        cornellBulkSave(cornellContentEntityList, orgId);
    }
    private CornellContentEntity saveOrUpdateCornellContent(String externalId, JsonNode transformData, JsonNode rawContentData, Timestamp currentTime,String fileId) {
        Optional<CornellContentEntity> optExternalContent = cornellContentRepository.findByExternalId(externalId);
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
    private void cornellBulkSave(List<CornellContentEntity> cornellContentEntityList, String orgId) {
        cornellContentRepository.saveAll(cornellContentEntityList);
        Long totalCourseCount = cornellContentRepository.count();
        JsonNode responseJson = dataTransformUtility.fetchPartnerInfoUsingApi(orgId);
        JsonNode resultData = responseJson.path(Constants.RESULT);
        JsonNode data = resultData.path(Constants.DATA);
        ((ObjectNode) data).put(Constants.TOTAL_COURSE_COUNT, totalCourseCount);
        ((ObjectNode) resultData).remove(Constants.DATA);
        ((ObjectNode) resultData).setAll((ObjectNode) (data));
        ((ObjectNode) resultData).remove(Constants.UPDATED_ON);
        ((ObjectNode) resultData).remove(Constants.CREATED_ON);
        if (resultData.path(Constants.CONTENT_UPLOAD_LAST_UPDATED_DATE).isNull()) {
            ((ObjectNode) resultData).put(Constants.CONTENT_UPLOAD_LAST_UPDATED_DATE, "0000-01-01T00:00:00Z");
        }
        if (resultData.path(Constants.CONTENT_PROGRESS_LAST_UPDATED_DATE).isNull()) {
            ((ObjectNode) resultData).put(Constants.CONTENT_PROGRESS_LAST_UPDATED_DATE, "0000-01-01T00:00:00Z");
        }
        dataTransformUtility.updatingPartnerInfo(resultData, orgId);
    }

    @Override
    public Page<?> fetchAllContentFromSecondaryDb(RequestDto dto) {
        Pageable pageable= PageRequest.of(dto.getPage(), dto.getSize());
        return cornellContentRepository.findAllCiosDataAndIsActive(dto.getIsActive(),pageable,dto.getKeyword());
    }

    @Override
    public List<?> fetchAllContentByPartnerName() {
        return cornellContentRepository.findAll();
    }

    @Override
    public void deleteContent(Object contentEntity) {
        cornellContentRepository.delete((CornellContentEntity) contentEntity);
    }

}
