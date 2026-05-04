package com.igot.cios.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.igot.cios.dto.DeleteContentRequestDto;
import com.igot.cios.dto.SBApiResponse;
import com.igot.cios.entity.CornellContentEntity;
import com.igot.cios.plugins.DataTransformUtility;
import com.igot.cios.repository.CornellContentRepository;
import com.igot.cios.service.impl.CiosContentServiceImpl;
import com.igot.cios.util.CbServerProperties;
import com.igot.cios.util.Constants;
import com.igot.cios.util.elasticsearch.service.EsUtilService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.util.ReflectionTestUtils;

import java.sql.Timestamp;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class CiosContentServiceImplTest {

    @Mock
    private CornellContentRepository repository;

    @Mock
    private EsUtilService esUtilService;

    @Mock
    private DataTransformUtility dataTransformUtility;

    @Mock
    private CbServerProperties cbServerProperties;

    @Mock
    private ObjectMapper objectMapper;

    @InjectMocks
    private CiosContentServiceImpl ciosContentService;

    private ObjectMapper realObjectMapper;
    private DeleteContentRequestDto deleteContentRequestDto;
    private String partnerCode;
    private String partnerId;
    private List<String> externalIds;

    @BeforeEach
    void setUp() {
        realObjectMapper = new ObjectMapper();
        partnerCode = "TEST_PARTNER";
        partnerId = "partner-123";
        externalIds = Arrays.asList("ext-001", "ext-002");

        deleteContentRequestDto = new DeleteContentRequestDto();
        deleteContentRequestDto.setPartnerCode(partnerCode);
        deleteContentRequestDto.setExternalId(externalIds);

        // Set the real ObjectMapper for the service
        ReflectionTestUtils.setField(ciosContentService, "objectMapper", realObjectMapper);
    }

    // -------------------- DELETE CONTENT TEST CASES --------------------

    @Test
    void deleteNotPublishContent_success_multipleValidEntities() throws Exception {
        // Arrange
        List<CornellContentEntity> entities = new ArrayList<>();
        CornellContentEntity entity1 = createMockEntity("ext-001", false, Constants.NOT_INITIATED);
        CornellContentEntity entity2 = createMockEntity("ext-002", false, Constants.DRAFT);
        entities.add(entity1);
        entities.add(entity2);

        when(repository.findByExternalIdInAndPartnerCode(externalIds, partnerCode)).thenReturn(entities);
        doNothing().when(repository).delete(any(CornellContentEntity.class));
        doNothing().when(esUtilService).deleteDocument(anyString(), anyString());
        when(repository.countByPartnerCode(partnerCode)).thenReturn(20L);

        JsonNode partnerResponse = createMockPartnerResponse();
        when(dataTransformUtility.fetchPartnerInfoUsingApi(partnerCode)).thenReturn(partnerResponse);
        when(dataTransformUtility.updatingPartnerInfo(any(JsonNode.class))).thenReturn("success");

        // Act
        ResponseEntity<?> response = ciosContentService.deleteNotPublishContent(deleteContentRequestDto);

        // Assert
        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getStatusCode());
        SBApiResponse apiResponse = (SBApiResponse) response.getBody();
        assertEquals(Constants.SUCCESS, apiResponse.getResult().get(Constants.STATUS));

        verify(repository, times(2)).delete(any(CornellContentEntity.class));
        verify(esUtilService, times(2)).deleteDocument(anyString(), eq(Constants.CIOS_CONTENT_INDEX_NAME));
    }

    @Test
    void deleteNotPublishContent_error_entityIsActive() throws Exception {
        // Arrange
        List<CornellContentEntity> entities = new ArrayList<>();
        CornellContentEntity entity1 = createMockEntity("ext-001", true, Constants.NOT_INITIATED);
        entities.add(entity1);

        when(repository.findByExternalIdInAndPartnerCode(externalIds, partnerCode)).thenReturn(entities);
        when(repository.countByPartnerCode(partnerCode)).thenReturn(10L);

        JsonNode partnerResponse = createMockPartnerResponse();
        when(dataTransformUtility.fetchPartnerInfoUsingApi(partnerCode)).thenReturn(partnerResponse);
        when(dataTransformUtility.updatingPartnerInfo(any(JsonNode.class))).thenReturn("success");

        // Act
        ResponseEntity<?> response = ciosContentService.deleteNotPublishContent(deleteContentRequestDto);

        // Assert
        assertNotNull(response);
        assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
        SBApiResponse apiResponse = (SBApiResponse) response.getBody();
        assertNotNull(apiResponse);
        assertEquals(Constants.FAILED, apiResponse.getParams().getStatus());
        assertTrue(apiResponse.getParams().getErr().contains("External ID: ext-001 is live, cannot delete."));

        verify(repository, never()).delete(any(CornellContentEntity.class));
        verify(esUtilService, never()).deleteDocument(anyString(), anyString());
    }

    @Test
    void deleteNotPublishContent_error_statusNotDraftOrNotInitiated() throws Exception {
        // Arrange
        List<CornellContentEntity> entities = new ArrayList<>();
        CornellContentEntity entity1 = createMockEntity("ext-001", false, "Published");
        entities.add(entity1);

        when(repository.findByExternalIdInAndPartnerCode(externalIds, partnerCode)).thenReturn(entities);
        when(repository.countByPartnerCode(partnerCode)).thenReturn(10L);

        JsonNode partnerResponse = createMockPartnerResponse();
        when(dataTransformUtility.fetchPartnerInfoUsingApi(partnerCode)).thenReturn(partnerResponse);
        when(dataTransformUtility.updatingPartnerInfo(any(JsonNode.class))).thenReturn("success");

        // Act
        ResponseEntity<?> response = ciosContentService.deleteNotPublishContent(deleteContentRequestDto);

        // Assert
        assertNotNull(response);
        assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
        SBApiResponse apiResponse = (SBApiResponse) response.getBody();
        assertEquals(Constants.FAILED, apiResponse.getParams().getStatus());
        assertTrue(apiResponse.getParams().getErr().contains("External ID: ext-001 cannot be deleted because its status is not 'notInitiated'."));

        verify(repository, never()).delete(any(CornellContentEntity.class));
        verify(esUtilService, never()).deleteDocument(anyString(), anyString());
    }

    @Test
    void deleteNotPublishContent_error_externalIdDoesNotExist() throws Exception {
        // Arrange
        List<CornellContentEntity> entities = new ArrayList<>();
        CornellContentEntity entity1 = createMockEntity("ext-001", false, Constants.NOT_INITIATED);
        entities.add(entity1);
        // ext-002 is missing from the returned entities

        when(repository.findByExternalIdInAndPartnerCode(externalIds, partnerCode)).thenReturn(entities);
        when(repository.countByPartnerCode(partnerCode)).thenReturn(10L);

        JsonNode partnerResponse = createMockPartnerResponse();
        when(dataTransformUtility.fetchPartnerInfoUsingApi(partnerCode)).thenReturn(partnerResponse);
        when(dataTransformUtility.updatingPartnerInfo(any(JsonNode.class))).thenReturn("success");

        // Act
        ResponseEntity<?> response = ciosContentService.deleteNotPublishContent(deleteContentRequestDto);

        // Assert
        assertNotNull(response);
        assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
        SBApiResponse apiResponse = (SBApiResponse) response.getBody();
        assertEquals(Constants.FAILED, apiResponse.getParams().getStatus());
        assertTrue(apiResponse.getParams().getErr().contains("External ID: ext-002 does not exist."));

        verify(repository, times(1)).delete(any(CornellContentEntity.class)); // Only deletes ext-001
    }

    @Test
    void deleteNotPublishContent_error_ciosDataNull() throws Exception {
        // Arrange
        List<CornellContentEntity> entities = new ArrayList<>();
        CornellContentEntity entity1 = new CornellContentEntity();
        entity1.setExternalId("ext-001");
        entity1.setPartnerId(partnerId);
        entity1.setPartnerCode(partnerCode);
        entity1.setIsActive(false);
        entity1.setCiosData(null); // Null ciosData

        entities.add(entity1);

        when(repository.findByExternalIdInAndPartnerCode(externalIds, partnerCode)).thenReturn(entities);
        when(repository.countByPartnerCode(partnerCode)).thenReturn(10L);

        JsonNode partnerResponse = createMockPartnerResponse();
        when(dataTransformUtility.fetchPartnerInfoUsingApi(partnerCode)).thenReturn(partnerResponse);
        when(dataTransformUtility.updatingPartnerInfo(any(JsonNode.class))).thenReturn("success");

        // Act
        ResponseEntity<?> response = ciosContentService.deleteNotPublishContent(deleteContentRequestDto);

        // Assert
        assertNotNull(response);
        assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
        SBApiResponse apiResponse = (SBApiResponse) response.getBody();
        assertEquals(Constants.FAILED, apiResponse.getParams().getStatus());
        assertTrue(apiResponse.getParams().getErr().contains("External ID: ext-001 does not have a valid status in ciosData."));

        verify(repository, never()).delete(any(CornellContentEntity.class));
    }

    @Test
    void deleteNotPublishContent_error_missingStatusInCiosData() throws Exception {
        // Arrange
        List<CornellContentEntity> entities = new ArrayList<>();
        CornellContentEntity entity1 = new CornellContentEntity();
        entity1.setExternalId("ext-001");
        entity1.setPartnerId(partnerId);
        entity1.setPartnerCode(partnerCode);
        entity1.setIsActive(false);

        // CiosData without status
        ObjectNode ciosData = realObjectMapper.createObjectNode();
        ObjectNode content = realObjectMapper.createObjectNode();
        content.put("name", "Test Content");
        ciosData.set("content", content);
        entity1.setCiosData(ciosData);

        entities.add(entity1);

        when(repository.findByExternalIdInAndPartnerCode(externalIds, partnerCode)).thenReturn(entities);
        when(repository.countByPartnerCode(partnerCode)).thenReturn(10L);

        JsonNode partnerResponse = createMockPartnerResponse();
        when(dataTransformUtility.fetchPartnerInfoUsingApi(partnerCode)).thenReturn(partnerResponse);
        when(dataTransformUtility.updatingPartnerInfo(any(JsonNode.class))).thenReturn("success");

        // Act
        ResponseEntity<?> response = ciosContentService.deleteNotPublishContent(deleteContentRequestDto);

        // Assert
        assertNotNull(response);
        assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
        SBApiResponse apiResponse = (SBApiResponse) response.getBody();
        assertEquals(Constants.FAILED, apiResponse.getParams().getStatus());
        assertTrue(apiResponse.getParams().getErr().contains("External ID: ext-001 does not have a valid status in ciosData."));

        verify(repository, never()).delete(any(CornellContentEntity.class));
    }

    @Test
    void deleteNotPublishContent_mixedScenario_someSuccessSomeErrors() throws Exception {
        // Arrange
        List<CornellContentEntity> entities = new ArrayList<>();
        CornellContentEntity entity1 = createMockEntity("ext-001", false, Constants.NOT_INITIATED); // Valid - should delete
        CornellContentEntity entity2 = createMockEntity("ext-002", true, Constants.DRAFT); // Invalid - is active

        List<String> mixedIds = Arrays.asList("ext-001", "ext-002", "ext-003"); // ext-003 doesn't exist
        deleteContentRequestDto.setExternalId(mixedIds);

        entities.add(entity1);
        entities.add(entity2);

        when(repository.findByExternalIdInAndPartnerCode(mixedIds, partnerCode)).thenReturn(entities);
        doNothing().when(repository).delete(any(CornellContentEntity.class));
        doNothing().when(esUtilService).deleteDocument(anyString(), anyString());
        when(repository.countByPartnerCode(partnerCode)).thenReturn(10L);

        JsonNode partnerResponse = createMockPartnerResponse();
        when(dataTransformUtility.fetchPartnerInfoUsingApi(partnerCode)).thenReturn(partnerResponse);
        when(dataTransformUtility.updatingPartnerInfo(any(JsonNode.class))).thenReturn("success");

        // Act
        ResponseEntity<?> response = ciosContentService.deleteNotPublishContent(deleteContentRequestDto);

        // Assert
        assertNotNull(response);
        assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
        SBApiResponse apiResponse = (SBApiResponse) response.getBody();
        assertEquals(Constants.FAILED, apiResponse.getParams().getStatus());

        String errorMsg = apiResponse.getParams().getErr();
        assertTrue(errorMsg.contains("External ID: ext-002 is live, cannot delete."));
        assertTrue(errorMsg.contains("External ID: ext-003 does not exist."));

        // Only entity1 should be deleted
        verify(repository, times(1)).delete(entity1);
        verify(esUtilService, times(1)).deleteDocument(eq(partnerId + "_ext-001"), eq(Constants.CIOS_CONTENT_INDEX_NAME));
    }

    @Test
    void deleteNotPublishContent_emptyExternalIdList() throws Exception {
        // Arrange
        deleteContentRequestDto.setExternalId(Collections.emptyList());
        when(repository.findByExternalIdInAndPartnerCode(anyList(), anyString())).thenReturn(Collections.emptyList());
        when(repository.countByPartnerCode(partnerCode)).thenReturn(10L);

        JsonNode partnerResponse = createMockPartnerResponse();
        when(dataTransformUtility.fetchPartnerInfoUsingApi(partnerCode)).thenReturn(partnerResponse);
        when(dataTransformUtility.updatingPartnerInfo(any(JsonNode.class))).thenReturn("success");

        // Act
        ResponseEntity<?> response = ciosContentService.deleteNotPublishContent(deleteContentRequestDto);

        // Assert
        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getStatusCode());
        SBApiResponse apiResponse = (SBApiResponse) response.getBody();
        assertEquals(Constants.SUCCESS, apiResponse.getResult().get(Constants.STATUS));

        verify(repository, never()).delete(any(CornellContentEntity.class));
        verify(esUtilService, never()).deleteDocument(anyString(), anyString());
    }

    @Test
    void saveOrUpdateContent_createNewContent_success() throws Exception {
        // Arrange
        String externalId = "new-ext-001";
        Timestamp currentTime = new Timestamp(System.currentTimeMillis());
        JsonNode transformData = createMockTransformData(externalId, "Test Content");

        when(repository.findByExternalIdAndPartnerId(externalId, partnerId)).thenReturn(Optional.empty());
        when(repository.save(any(CornellContentEntity.class))).thenAnswer(invocation -> invocation.getArgument(0));
        when(cbServerProperties.getElasticCiosContentJsonPath()).thenReturn("/path/to/mapping.json");
        doNothing().when(dataTransformUtility).flattenContentData(anyMap());
        when(esUtilService.updateDocument(anyString(), anyString(), anyMap(), anyString())).thenReturn("document-id");

        // Act
        CornellContentEntity result = ReflectionTestUtils.invokeMethod(
            ciosContentService,
            "saveOrUpdateContent",
            externalId,
            transformData,
            currentTime,
            false,
            partnerCode,
            partnerId
        );

        // Assert
        assertNotNull(result);
        assertEquals(externalId, result.getExternalId());
        assertEquals(partnerId, result.getPartnerId());
        assertEquals(partnerCode, result.getPartnerCode());
        assertFalse(result.getIsActive());
        assertEquals(currentTime, result.getCreatedDate());
        assertEquals(currentTime, result.getUpdatedDate());
        assertNotNull(result.getCiosData());

        verify(repository, times(1)).findByExternalIdAndPartnerId(externalId, partnerId);
        verify(repository, times(1)).save(any(CornellContentEntity.class));
        verify(dataTransformUtility, times(1)).flattenContentData(anyMap());
        verify(esUtilService, times(1)).updateDocument(
            eq(Constants.CIOS_CONTENT_INDEX_NAME),
            eq(partnerId + "_" + externalId),
            anyMap(),
            anyString()
        );
    }

    @Test
    void saveOrUpdateContent_updateExistingContent_success() throws Exception {
        // Arrange
        String externalId = "existing-ext-001";
        Timestamp createdTime = new Timestamp(System.currentTimeMillis() - 100000);
        Timestamp updateTime = new Timestamp(System.currentTimeMillis());
        JsonNode transformData = createMockTransformData(externalId, "Updated Content");

        CornellContentEntity existingEntity = new CornellContentEntity();
        existingEntity.setExternalId(externalId);
        existingEntity.setPartnerId(partnerId);
        existingEntity.setPartnerCode(partnerCode);
        existingEntity.setIsActive(false);
        existingEntity.setCreatedDate(createdTime);
        existingEntity.setUpdatedDate(createdTime);
        existingEntity.setCiosData(createMockTransformData(externalId, "Old Content"));

        when(repository.findByExternalIdAndPartnerId(externalId, partnerId)).thenReturn(Optional.of(existingEntity));
        when(repository.save(any(CornellContentEntity.class))).thenAnswer(invocation -> invocation.getArgument(0));
        when(cbServerProperties.getElasticCiosContentJsonPath()).thenReturn("/path/to/mapping.json");
        doNothing().when(dataTransformUtility).flattenContentData(anyMap());
        when(esUtilService.updateDocument(anyString(), anyString(), anyMap(), anyString())).thenReturn("document-id");

        // Act
        CornellContentEntity result = ReflectionTestUtils.invokeMethod(
            ciosContentService,
            "saveOrUpdateContent",
            externalId,
            transformData,
            updateTime,
            true,
            partnerCode,
            partnerId
        );

        // Assert
        assertNotNull(result);
        assertEquals(externalId, result.getExternalId());
        assertEquals(partnerId, result.getPartnerId());
        assertEquals(partnerCode, result.getPartnerCode());
        assertTrue(result.getIsActive());
        assertEquals(createdTime, result.getCreatedDate()); // Created date should remain unchanged
        assertEquals(updateTime, result.getUpdatedDate()); // Updated date should be new
        assertNotNull(result.getCiosData());

        verify(repository, times(1)).findByExternalIdAndPartnerId(externalId, partnerId);
        verify(repository, times(1)).save(any(CornellContentEntity.class));
        verify(dataTransformUtility, times(1)).flattenContentData(anyMap());
        verify(esUtilService, times(1)).updateDocument(
            eq(Constants.CIOS_CONTENT_INDEX_NAME),
            eq(partnerId + "_" + externalId),
            anyMap(),
            anyString()
        );
    }

    @Test
    void saveOrUpdateContent_verifyElasticsearchDocumentUpdate() throws Exception {
        // Arrange
        String externalId = "es-test-001";
        Timestamp currentTime = new Timestamp(System.currentTimeMillis());
        JsonNode transformData = createMockTransformData(externalId, "ES Test Content");
        String elasticPath = "/elastic/mapping.json";
        String expectedUniqueId = partnerId + "_" + externalId;

        when(repository.findByExternalIdAndPartnerId(externalId, partnerId)).thenReturn(Optional.empty());
        when(repository.save(any(CornellContentEntity.class))).thenAnswer(invocation -> invocation.getArgument(0));
        when(cbServerProperties.getElasticCiosContentJsonPath()).thenReturn(elasticPath);
        doNothing().when(dataTransformUtility).flattenContentData(anyMap());
        when(esUtilService.updateDocument(anyString(), anyString(), anyMap(), anyString())).thenReturn("document-id");

        // Act
        CornellContentEntity result = ReflectionTestUtils.invokeMethod(
            ciosContentService,
            "saveOrUpdateContent",
            externalId,
            transformData,
            currentTime,
            false,
            partnerCode,
            partnerId
        );

        // Assert
        assertNotNull(result);
        verify(esUtilService, times(1)).updateDocument(
            eq(Constants.CIOS_CONTENT_INDEX_NAME),
            eq(expectedUniqueId),
            anyMap(),
            eq(elasticPath)
        );
    }

    @Test
    void saveOrUpdateContent_verifyDataFlattening() throws Exception {
        // Arrange
        String externalId = "flatten-test-001";
        Timestamp currentTime = new Timestamp(System.currentTimeMillis());
        JsonNode transformData = createMockTransformData(externalId, "Flatten Test");

        when(repository.findByExternalIdAndPartnerId(externalId, partnerId)).thenReturn(Optional.empty());
        when(repository.save(any(CornellContentEntity.class))).thenAnswer(invocation -> invocation.getArgument(0));
        when(cbServerProperties.getElasticCiosContentJsonPath()).thenReturn("/path/to/mapping.json");
        doNothing().when(dataTransformUtility).flattenContentData(anyMap());
        when(esUtilService.updateDocument(anyString(), anyString(), anyMap(), anyString())).thenReturn("document-id");

        // Act
        CornellContentEntity result = ReflectionTestUtils.invokeMethod(
            ciosContentService,
            "saveOrUpdateContent",
            externalId,
            transformData,
            currentTime,
            false,
            partnerCode,
            partnerId
        );

        // Assert
        assertNotNull(result);
        verify(dataTransformUtility, times(1)).flattenContentData(argThat(map ->
            map.containsKey("externalId") &&
            map.get("externalId").equals(externalId)
        ));
    }

    @Test
    void saveOrUpdateContent_updateExistingContent_preservesCreatedDate() throws Exception {
        // Arrange
        String externalId = "preserve-date-001";
        Timestamp originalCreatedTime = new Timestamp(System.currentTimeMillis() - 1000000);
        Timestamp updateTime = new Timestamp(System.currentTimeMillis());
        JsonNode transformData = createMockTransformData(externalId, "Date Test");

        CornellContentEntity existingEntity = new CornellContentEntity();
        existingEntity.setExternalId(externalId);
        existingEntity.setPartnerId(partnerId);
        existingEntity.setPartnerCode(partnerCode);
        existingEntity.setIsActive(false);
        existingEntity.setCreatedDate(originalCreatedTime);
        existingEntity.setUpdatedDate(originalCreatedTime);

        when(repository.findByExternalIdAndPartnerId(externalId, partnerId)).thenReturn(Optional.of(existingEntity));
        when(repository.save(any(CornellContentEntity.class))).thenAnswer(invocation -> invocation.getArgument(0));
        when(cbServerProperties.getElasticCiosContentJsonPath()).thenReturn("/path/to/mapping.json");
        doNothing().when(dataTransformUtility).flattenContentData(anyMap());
        when(esUtilService.updateDocument(anyString(), anyString(), anyMap(), anyString())).thenReturn("document-id");

        // Act
        CornellContentEntity result = ReflectionTestUtils.invokeMethod(
            ciosContentService,
            "saveOrUpdateContent",
            externalId,
            transformData,
            updateTime,
            true,
            partnerCode,
            partnerId
        );

        // Assert
        assertNotNull(result);
        assertEquals(originalCreatedTime, result.getCreatedDate());
        assertEquals(updateTime, result.getUpdatedDate());
        assertNotEquals(result.getCreatedDate(), result.getUpdatedDate());
    }

    @Test
    void saveOrUpdateContent_togglesIsActiveFlag() throws Exception {
        // Arrange
        String externalId = "active-toggle-001";
        Timestamp currentTime = new Timestamp(System.currentTimeMillis());
        JsonNode transformData = createMockTransformData(externalId, "Active Toggle Test");

        CornellContentEntity existingEntity = new CornellContentEntity();
        existingEntity.setExternalId(externalId);
        existingEntity.setPartnerId(partnerId);
        existingEntity.setPartnerCode(partnerCode);
        existingEntity.setIsActive(false);
        existingEntity.setCreatedDate(currentTime);
        existingEntity.setUpdatedDate(currentTime);

        when(repository.findByExternalIdAndPartnerId(externalId, partnerId)).thenReturn(Optional.of(existingEntity));
        when(repository.save(any(CornellContentEntity.class))).thenAnswer(invocation -> invocation.getArgument(0));
        when(cbServerProperties.getElasticCiosContentJsonPath()).thenReturn("/path/to/mapping.json");
        doNothing().when(dataTransformUtility).flattenContentData(anyMap());
        when(esUtilService.updateDocument(anyString(), anyString(), anyMap(), anyString())).thenReturn("document-id");

        // Act - Update to active
        CornellContentEntity result = ReflectionTestUtils.invokeMethod(
            ciosContentService,
            "saveOrUpdateContent",
            externalId,
            transformData,
            currentTime,
            true, // Toggle to active
            partnerCode,
            partnerId
        );

        // Assert
        assertNotNull(result);
        assertTrue(result.getIsActive());

        // Verify the entity was saved with the correct active status
        verify(repository, times(1)).save(argThat(entity ->
            entity.getIsActive() == true
        ));
    }

    @Test
    void saveOrUpdateContent_updatesPartnerCodeInTransformData() throws Exception {
        // Arrange
        String externalId = "partner-code-001";
        Timestamp currentTime = new Timestamp(System.currentTimeMillis());
        JsonNode transformData = createMockTransformData(externalId, "Partner Code Test");

        when(repository.findByExternalIdAndPartnerId(externalId, partnerId)).thenReturn(Optional.empty());
        when(repository.save(any(CornellContentEntity.class))).thenAnswer(invocation -> invocation.getArgument(0));
        when(cbServerProperties.getElasticCiosContentJsonPath()).thenReturn("/path/to/mapping.json");
        doNothing().when(dataTransformUtility).flattenContentData(anyMap());
        when(esUtilService.updateDocument(anyString(), anyString(), anyMap(), anyString())).thenReturn("document-id");

        // Act
        CornellContentEntity result = ReflectionTestUtils.invokeMethod(
            ciosContentService,
            "saveOrUpdateContent",
            externalId,
            transformData,
            currentTime,
            false,
            partnerCode,
            partnerId
        );

        // Assert
        assertNotNull(result);
        JsonNode contentNode = result.getCiosData().path(Constants.CONTENT);
        assertTrue(contentNode.has(Constants.PARTNER_CODE));
        assertEquals(partnerCode, contentNode.get(Constants.PARTNER_CODE).asText());
    }

    // -------------------- HELPER METHODS --------------------

    private CornellContentEntity createMockEntity(String externalId, boolean isActive, String status) {
        CornellContentEntity entity = new CornellContentEntity();
        entity.setExternalId(externalId);
        entity.setPartnerId(partnerId);
        entity.setPartnerCode(partnerCode);
        entity.setIsActive(isActive);
        entity.setCreatedDate(new Timestamp(System.currentTimeMillis()));
        entity.setUpdatedDate(new Timestamp(System.currentTimeMillis()));

        // Create ciosData with status
        ObjectNode ciosData = realObjectMapper.createObjectNode();
        ObjectNode content = realObjectMapper.createObjectNode();
        content.put("status", status);
        content.put("name", "Test Content " + externalId);
        ciosData.set("content", content);
        entity.setCiosData(ciosData);

        return entity;
    }

    private JsonNode createMockPartnerResponse() {
        ObjectNode response = realObjectMapper.createObjectNode();
        ObjectNode data = realObjectMapper.createObjectNode();
        data.put("id", partnerId);
        data.put("partnerName", "Test Partner");
        data.put(Constants.TOTAL_COURSES_COUNT, 0);
        response.set(Constants.DATA, data);
        return response;
    }

    private JsonNode createMockTransformData(String externalId, String contentName) {
        ObjectNode transformData = realObjectMapper.createObjectNode();
        ObjectNode content = realObjectMapper.createObjectNode();
        content.put("externalId", externalId);
        content.put("name", contentName);
        content.put("status", Constants.NOT_INITIATED);
        transformData.set(Constants.CONTENT, content);
        return transformData;
    }
}
