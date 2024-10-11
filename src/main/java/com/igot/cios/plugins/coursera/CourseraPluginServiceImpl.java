package com.igot.cios.plugins.coursera;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.igot.cios.dto.DeleteContentRequestDto;
import com.igot.cios.dto.RequestDto;
import com.igot.cios.dto.SBApiResponse;
import com.igot.cios.entity.CourseraContentEntity;
import com.igot.cios.exception.CiosContentException;
import com.igot.cios.plugins.ContentPartnerPluginService;
import com.igot.cios.plugins.DataTransformUtility;
import com.igot.cios.repository.CourseraContentRepository;
import com.igot.cios.util.CbServerProperties;
import com.igot.cios.util.Constants;
import com.igot.cios.util.elasticsearch.service.EsUtilService;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Service
@Slf4j
public class CourseraPluginServiceImpl implements ContentPartnerPluginService {
    @Autowired
    private CourseraContentRepository courseraContentRepository;

    @Autowired
    private EsUtilService esUtilService;

    @Autowired
    private CbServerProperties cbServerProperties;

    @Autowired
    private ObjectMapper objectMapper;

    @Override
    public void loadContentFromExcel(JsonNode processedData, String partnerCode, String fileName, String fileId, List<Object> contentJson) {

    }

    private CourseraContentEntity saveOrUpdateCornellContent(String externalId, JsonNode transformData, Timestamp currentTime,boolean isActive,String partnerCode) {
        ((ObjectNode) transformData.path(Constants.CONTENT)).put(Constants.PARTNER_CODE, partnerCode).asText();
        addSearchTags(transformData);
        CourseraContentEntity externalContent;
        Optional<CourseraContentEntity> optExternalContent = courseraContentRepository.findByExternalId(externalId);
        if (optExternalContent.isPresent()) {
            externalContent = optExternalContent.get();
            externalContent.setExternalId(externalId);
            externalContent.setCiosData(transformData);
            externalContent.setIsActive(isActive);
            externalContent.setCreatedDate(externalContent.getCreatedDate());
            externalContent.setUpdatedDate(currentTime);
        } else {
            externalContent = new CourseraContentEntity();
            externalContent.setExternalId(externalId);
            externalContent.setCiosData(transformData);
            externalContent.setIsActive(isActive);
            externalContent.setCreatedDate(currentTime);
            externalContent.setUpdatedDate(currentTime);
        }
        courseraContentRepository.save(externalContent);
        Map<String, Object> entityMap = objectMapper.convertValue(externalContent, Map.class);
        flattenContentData(entityMap);
        String uniqueId = partnerCode + "_" + externalContent.getExternalId();
        esUtilService.updateDocument(Constants.CIOS_CONTENT_INDEX_NAME, Constants.INDEX_TYPE,
                uniqueId,
                entityMap,
                cbServerProperties.getElasticCiosContentJsonPath()
        );
        return externalContent;
    }

    private JsonNode addSearchTags(JsonNode transformData) {
        List<String> searchTags = new ArrayList<>();
        searchTags.add(transformData.path(Constants.CONTENT).get(Constants.NAME).textValue().toLowerCase());
        ArrayNode searchTagsArray = objectMapper.valueToTree(searchTags);
        ((ObjectNode) transformData.path(Constants.CONTENT)).put(Constants.CONTENT_SEARCH_TAGS, searchTagsArray);
        return transformData;
    }

    private void flattenContentData(Map<String, Object> entityMap) {
        if (entityMap.containsKey("ciosData") && entityMap.get("ciosData") instanceof Map) {
            Map<String, Object> ciosDataMap = (Map<String, Object>) entityMap.get("ciosData");
            if (ciosDataMap.containsKey("content") && ciosDataMap.get("content") instanceof Map) {
                Map<String, Object> contentMap = (Map<String, Object>) ciosDataMap.get("content");
                entityMap.putAll(contentMap);
                entityMap.remove("ciosData");
            }
        }
    }

    @Override
    public Page<?> fetchAllContentFromSecondaryDb(RequestDto dto) {
        Pageable pageable = PageRequest.of(dto.getPage(), dto.getSize());
        return courseraContentRepository.findAllCiosDataAndIsActive(dto.getIsActive(), pageable, dto.getKeyword());
    }

    @Override
    public Object readContentByExternalId(String externalid) {
        Optional<CourseraContentEntity> entity = courseraContentRepository.findByExternalId(externalid);
        if (entity.isPresent()) {
            return entity.get().getCiosData();
        } else {
            throw new CiosContentException("No data found for given id", externalid, HttpStatus.BAD_REQUEST);
        }

    }

    @Override
    public Object updateContent(JsonNode jsonNode, String partnerCode) {
        Timestamp currentTime = new Timestamp(System.currentTimeMillis());
        String externalId = jsonNode.path("content").get("externalId").asText();
        boolean isActive = jsonNode.path("content").get("isActive").asBoolean(false);
        return saveOrUpdateCornellContent(externalId,jsonNode,currentTime,isActive,partnerCode);
    }

    @Override
    public ResponseEntity<?> deleteNotPublishContent(DeleteContentRequestDto deleteContentRequestDto) {
        return null;
    }
}
