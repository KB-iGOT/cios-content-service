package com.igot.cios.plugins;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.igot.cios.entity.CornellContentEntity;
import com.igot.cios.entity.FileInfoEntity;
import com.igot.cios.exception.CiosContentException;
import com.igot.cios.repository.CornellContentRepository;
import com.igot.cios.repository.FileInfoRepository;
import com.igot.cios.util.CbServerProperties;
import com.igot.cios.util.Constants;
import com.igot.cios.util.elasticsearch.service.EsUtilService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.*;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.client.RestTemplate;

import java.sql.Timestamp;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class DataTransformUtilityTest {

    @InjectMocks
    private DataTransformUtility dataTransformUtility;

    private ObjectMapper objectMapper;

    @Mock
    private CbServerProperties cbServerProperties;

    @Mock
    private RestTemplate restTemplate;

    @Mock
    private FileInfoRepository fileInfoRepository;

    @Mock
    private EsUtilService esUtilService;

    @Mock
    private CornellContentRepository cornellContentRepository;


    @BeforeEach
    void setup() {
        objectMapper = new ObjectMapper();
        MockitoAnnotations.openMocks(this);
        ReflectionTestUtils.setField(dataTransformUtility, "objectMapper", objectMapper);
    }

    // ---------------- getAdminAccessToken ----------------
    @Test
    void getAdminAccessToken_success() {
        cbServerProperties.keycloakUrl = "http://keycloak";
        cbServerProperties.ssoUsername = "admin";
        cbServerProperties.ssoPassword = "password";

        Map<String, Object> tokenResponse = new HashMap<>();
        tokenResponse.put("access_token", "test-token");

        ResponseEntity<Map<String, Object>> response =
                new ResponseEntity<>(tokenResponse, HttpStatus.OK);

        doReturn(response)
                .when(restTemplate)
                .exchange(
                        anyString(),
                        eq(HttpMethod.POST),
                        any(HttpEntity.class),
                        ArgumentMatchers.<ParameterizedTypeReference<Map<String, Object>>>any()
                );

        String token = dataTransformUtility.getAdminAccessToken();

        assertEquals("test-token", token);
    }


    @Test
    void getAdminAccessToken_failure() {
        when(restTemplate.exchange(
                anyString(),
                eq(HttpMethod.POST),
                any(HttpEntity.class),
                ArgumentMatchers.<ParameterizedTypeReference<Map<String, Object>>>any()
        )).thenThrow(new RuntimeException("KC down"));

        assertThrows(
                CiosContentException.class,
                () -> dataTransformUtility.getAdminAccessToken()
        );
    }

    @Test
    void createFileInfo_success() {
        Timestamp now = new Timestamp(System.currentTimeMillis());

        when(fileInfoRepository.save(any(FileInfoEntity.class)))
                .thenAnswer(inv -> inv.getArgument(0));

        String fileId = dataTransformUtility.createFileInfo(
                "partner1",
                null,
                "file.csv",
                now,
                now,
                "SUCCESS",
                "gcp.csv",
                "content.csv"
        );

        assertNotNull(fileId);
        verify(fileInfoRepository).save(any(FileInfoEntity.class));
    }

    @Test
    void updateDateFormatFromTimestampForCoursera_success() {
        long timestamp = 1700000000000L;

        String result =
                dataTransformUtility.updateDateFormatFromTimestampForCoursera(timestamp);

        assertNotNull(result);
        assertTrue(result.contains("-"));
    }

    @Test
    void getExistingMappers_success() throws Exception {
        cbServerProperties.keycloakUrl = "http://keycloak";
        ObjectMapper mapper = new ObjectMapper();
        JsonNode responseNode = mapper.readTree("""
            [
              {"name": "username", "id": "id-1"},
              {"name": "email", "id": "id-2"}
            ]
            """);

        ResponseEntity<JsonNode> response =
                new ResponseEntity<>(responseNode, HttpStatus.OK);
        doReturn(response)
                .when(restTemplate)
                .exchange(
                        anyString(),
                        eq(HttpMethod.GET),
                        any(HttpEntity.class),
                        eq(JsonNode.class)
                );

        Map<String, String> result =
                dataTransformUtility.getExistingMappers("token", "client-id");

        assertEquals(2, result.size());
        assertEquals("id-1", result.get("username"));
        assertEquals("id-2", result.get("email"));
    }

    @Test
    void updateProtocolMapper_success() {
        cbServerProperties.keycloakUrl = "http://keycloak";

        Map<String, Object> mapperPayload = new HashMap<>();
        mapperPayload.put("id", "mapper-id");
        mapperPayload.put("name", "firstname");

        doReturn(ResponseEntity.noContent().build())
                .when(restTemplate)
                .exchange(
                        anyString(),
                        eq(HttpMethod.PUT),
                        any(HttpEntity.class),
                        eq(Void.class)
                );
        assertDoesNotThrow(() ->
                dataTransformUtility.updateProtocolMapper(
                        "token",
                        "client-id",
                        mapperPayload
                )
        );
    }

    @Test
    void createProtocolMapper_success() {
        cbServerProperties.keycloakUrl = "http://keycloak";

        Map<String, Object> mapperPayload = new HashMap<>();
        mapperPayload.put("name", "email-mapper");
        mapperPayload.put("protocol", "saml");
        mapperPayload.put("protocolMapper", "saml-user-property-mapper");

        doReturn(new ResponseEntity<>("mapper-id", HttpStatus.CREATED))
                .when(restTemplate)
                .postForEntity(
                        anyString(),
                        any(HttpEntity.class),
                        eq(String.class)
                );

        assertDoesNotThrow(() ->
                dataTransformUtility.createProtocolMapper(
                        "token",
                        "client-id",
                        mapperPayload
                )
        );
        verify(restTemplate, times(1)).postForEntity(
                anyString(),
                any(HttpEntity.class),
                eq(String.class)
        );
    }

    @Test
    void createProtocolMapper_failure_httpError() {
        cbServerProperties.keycloakUrl = "http://keycloak";

        Map<String, Object> mapperPayload = new HashMap<>();
        mapperPayload.put("name", "email-mapper");

        doThrow(new org.springframework.web.client.HttpClientErrorException(
                HttpStatus.NOT_FOUND,
                "404 Not Found: {\"error\":\"Could not find client\"}"
        )).when(restTemplate)
                .postForEntity(
                        anyString(),
                        any(HttpEntity.class),
                        eq(String.class)
                );

        assertThrows(
                CiosContentException.class,
                () -> dataTransformUtility.createProtocolMapper(
                        "token",
                        "invalid-client-id",
                        mapperPayload
                )
        );
    }

    @Test
    void createProtocolMapper_failure_generalException() {
        cbServerProperties.keycloakUrl = "http://keycloak";

        Map<String, Object> mapperPayload = new HashMap<>();
        mapperPayload.put("name", "email-mapper");

        doThrow(new RuntimeException("Connection timeout"))
                .when(restTemplate)
                .postForEntity(
                        anyString(),
                        any(HttpEntity.class),
                        eq(String.class)
                );

        assertThrows(
                CiosContentException.class,
                () -> dataTransformUtility.createProtocolMapper(
                        "token",
                        "client-id",
                        mapperPayload
                )
        );
    }

    @Test
    void dataBulkSave_success_withMultipleEntities() {
        String partnerCode = "TEST_PARTNER";
        String partnerId = "partner-123";
        List<CornellContentEntity> entityList = new ArrayList<>();

        CornellContentEntity entity1 = createMockContentEntity("ext-001", partnerId, partnerCode);
        CornellContentEntity entity2 = createMockContentEntity("ext-002", partnerId, partnerCode);
        entityList.add(entity1);
        entityList.add(entity2);

        when(cornellContentRepository.saveAll(anyList())).thenReturn(entityList);
        when(cornellContentRepository.countByPartnerCode(partnerCode)).thenReturn(2L);
        when(esUtilService.addDocument(anyString(), anyString(), anyMap(), anyString())).thenReturn("doc-id");
        when(cbServerProperties.getElasticCiosContentJsonPath()).thenReturn("/path/to/mapping.json");

        JsonNode partnerResponse = createMockPartnerInfoResponse(partnerId);
        when(restTemplate.exchange(
            anyString(),
            eq(HttpMethod.GET),
            any(HttpEntity.class),
            eq(Map.class)
        )).thenReturn(ResponseEntity.ok(createPartnerApiResponse(partnerResponse)));

        when(restTemplate.postForEntity(anyString(), any(HttpEntity.class), eq(String.class)))
            .thenReturn(ResponseEntity.ok("success"));

        
        ReflectionTestUtils.invokeMethod(dataTransformUtility, "dataBulkSave", entityList, partnerCode);

       
        verify(cornellContentRepository, times(1)).saveAll(entityList);
        verify(esUtilService, times(2)).addDocument(
            eq(Constants.CIOS_CONTENT_INDEX_NAME),
            anyString(),
            anyMap(),
            anyString()
        );
        verify(cornellContentRepository, times(1)).countByPartnerCode(partnerCode);
        verify(restTemplate, times(1)).postForEntity(anyString(), any(HttpEntity.class), eq(String.class));
    }

    @Test
    void dataBulkSave_success_withSingleEntity() {
        String partnerCode = "TEST_PARTNER";
        String partnerId = "partner-123";
        List<CornellContentEntity> entityList = new ArrayList<>();

        CornellContentEntity entity = createMockContentEntity("ext-001", partnerId, partnerCode);
        entityList.add(entity);

        when(cornellContentRepository.saveAll(anyList())).thenReturn(entityList);
        when(cornellContentRepository.countByPartnerCode(partnerCode)).thenReturn(1L);
        when(esUtilService.addDocument(anyString(), anyString(), anyMap(), anyString())).thenReturn("doc-id");
        when(cbServerProperties.getElasticCiosContentJsonPath()).thenReturn("/path/to/mapping.json");

        JsonNode partnerResponse = createMockPartnerInfoResponse(partnerId);
        when(restTemplate.exchange(
            anyString(),
            eq(HttpMethod.GET),
            any(HttpEntity.class),
            eq(Map.class)
        )).thenReturn(ResponseEntity.ok(createPartnerApiResponse(partnerResponse)));

        when(restTemplate.postForEntity(anyString(), any(HttpEntity.class), eq(String.class)))
            .thenReturn(ResponseEntity.ok("success"));

        
        ReflectionTestUtils.invokeMethod(dataTransformUtility, "dataBulkSave", entityList, partnerCode);

       
        verify(cornellContentRepository, times(1)).saveAll(entityList);
        verify(esUtilService, times(1)).addDocument(
            eq(Constants.CIOS_CONTENT_INDEX_NAME),
            eq(partnerId + "_ext-001"),
            anyMap(),
            anyString()
        );
        verify(cornellContentRepository, times(1)).countByPartnerCode(partnerCode);
    }

    @Test
    void dataBulkSave_success_withEmptyList() {
        String partnerCode = "TEST_PARTNER";
        List<CornellContentEntity> entityList = new ArrayList<>();

        when(cornellContentRepository.saveAll(anyList())).thenReturn(entityList);
        when(cornellContentRepository.countByPartnerCode(partnerCode)).thenReturn(0L);

        JsonNode partnerResponse = createMockPartnerInfoResponse("partner-123");
        when(restTemplate.exchange(
            anyString(),
            eq(HttpMethod.GET),
            any(HttpEntity.class),
            eq(Map.class)
        )).thenReturn(ResponseEntity.ok(createPartnerApiResponse(partnerResponse)));

        when(restTemplate.postForEntity(anyString(), any(HttpEntity.class), eq(String.class)))
            .thenReturn(ResponseEntity.ok("success"));

        
        ReflectionTestUtils.invokeMethod(dataTransformUtility, "dataBulkSave", entityList, partnerCode);

       
        verify(cornellContentRepository, times(1)).saveAll(entityList);
        verify(esUtilService, never()).addDocument(anyString(), anyString(), anyMap(), anyString());
        verify(cornellContentRepository, times(1)).countByPartnerCode(partnerCode);
    }

    @Test
    void dataBulkSave_handlesElasticsearchException() {
        
        String partnerCode = "TEST_PARTNER";
        String partnerId = "partner-123";
        List<CornellContentEntity> entityList = new ArrayList<>();

        CornellContentEntity entity1 = createMockContentEntity("ext-001", partnerId, partnerCode);
        CornellContentEntity entity2 = createMockContentEntity("ext-002", partnerId, partnerCode);
        entityList.add(entity1);
        entityList.add(entity2);

        when(cornellContentRepository.saveAll(anyList())).thenReturn(entityList);
        when(cornellContentRepository.countByPartnerCode(partnerCode)).thenReturn(2L);
        when(cbServerProperties.getElasticCiosContentJsonPath()).thenReturn("/path/to/mapping.json");

        // First call succeeds, second call fails
        when(esUtilService.addDocument(anyString(), anyString(), anyMap(), anyString()))
            .thenReturn("doc-id")
            .thenThrow(new RuntimeException("ES connection error"));

        JsonNode partnerResponse = createMockPartnerInfoResponse(partnerId);
        when(restTemplate.exchange(
            anyString(),
            eq(HttpMethod.GET),
            any(HttpEntity.class),
            eq(Map.class)
        )).thenReturn(ResponseEntity.ok(createPartnerApiResponse(partnerResponse)));

        when(restTemplate.postForEntity(anyString(), any(HttpEntity.class), eq(String.class)))
            .thenReturn(ResponseEntity.ok("success"));

        assertDoesNotThrow(() ->
            ReflectionTestUtils.invokeMethod(dataTransformUtility, "dataBulkSave", entityList, partnerCode)
        );

       
        verify(cornellContentRepository, times(1)).saveAll(entityList);
        verify(esUtilService, times(2)).addDocument(anyString(), anyString(), anyMap(), anyString());
        verify(cornellContentRepository, times(1)).countByPartnerCode(partnerCode);
    }

    @Test
    void dataBulkSave_verifyUniqueIdFormat() {
        
        String partnerCode = "TEST_PARTNER";
        String partnerId = "partner-123";
        String externalId = "ext-001";
        List<CornellContentEntity> entityList = new ArrayList<>();

        CornellContentEntity entity = createMockContentEntity(externalId, partnerId, partnerCode);
        entityList.add(entity);

        when(cornellContentRepository.saveAll(anyList())).thenReturn(entityList);
        when(cornellContentRepository.countByPartnerCode(partnerCode)).thenReturn(1L);
        when(esUtilService.addDocument(anyString(), anyString(), anyMap(), anyString())).thenReturn("doc-id");
        when(cbServerProperties.getElasticCiosContentJsonPath()).thenReturn("/path/to/mapping.json");

        JsonNode partnerResponse = createMockPartnerInfoResponse(partnerId);
        when(restTemplate.exchange(
            anyString(),
            eq(HttpMethod.GET),
            any(HttpEntity.class),
            eq(Map.class)
        )).thenReturn(ResponseEntity.ok(createPartnerApiResponse(partnerResponse)));

        when(restTemplate.postForEntity(anyString(), any(HttpEntity.class), eq(String.class)))
            .thenReturn(ResponseEntity.ok("success"));

        
        ReflectionTestUtils.invokeMethod(dataTransformUtility, "dataBulkSave", entityList, partnerCode);

       
        String expectedUniqueId = partnerId + "_" + externalId;
        verify(esUtilService, times(1)).addDocument(
            eq(Constants.CIOS_CONTENT_INDEX_NAME),
            eq(expectedUniqueId),
            anyMap(),
            anyString()
        );
    }

    @Test
    void dataBulkSave_updatesTotalCourseCount() {
        
        String partnerCode = "TEST_PARTNER";
        String partnerId = "partner-123";
        Long expectedCourseCount = 5L;
        List<CornellContentEntity> entityList = new ArrayList<>();

        CornellContentEntity entity = createMockContentEntity("ext-001", partnerId, partnerCode);
        entityList.add(entity);

        when(cornellContentRepository.saveAll(anyList())).thenReturn(entityList);
        when(cornellContentRepository.countByPartnerCode(partnerCode)).thenReturn(expectedCourseCount);
        when(esUtilService.addDocument(anyString(), anyString(), anyMap(), anyString())).thenReturn("doc-id");
        when(cbServerProperties.getElasticCiosContentJsonPath()).thenReturn("/path/to/mapping.json");

        JsonNode partnerResponse = createMockPartnerInfoResponse(partnerId);
        when(restTemplate.exchange(
            anyString(),
            eq(HttpMethod.GET),
            any(HttpEntity.class),
            eq(Map.class)
        )).thenReturn(ResponseEntity.ok(createPartnerApiResponse(partnerResponse)));

        when(restTemplate.postForEntity(anyString(), any(HttpEntity.class), eq(String.class)))
            .thenReturn(ResponseEntity.ok("success"));

        
        ReflectionTestUtils.invokeMethod(dataTransformUtility, "dataBulkSave", entityList, partnerCode);

       
        verify(cornellContentRepository, times(1)).countByPartnerCode(partnerCode);

        // Verify postForEntity was called with updated partner data
        verify(restTemplate, times(1)).postForEntity(
            anyString(),
            argThat(httpEntity -> {
                try {
                    JsonNode body = objectMapper.readTree(objectMapper.writeValueAsString(((HttpEntity<?>) httpEntity).getBody()));
                    return body.path(Constants.DATA).get(Constants.TOTAL_COURSES_COUNT).asLong() == expectedCourseCount;
                } catch (Exception e) {
                    return false;
                }
            }),
            eq(String.class)
        );
    }

    @Test
    void dataBulkSave_callsFlattenContentData() {
        
        String partnerCode = "TEST_PARTNER";
        String partnerId = "partner-123";
        List<CornellContentEntity> entityList = new ArrayList<>();

        CornellContentEntity entity = createMockContentEntity("ext-001", partnerId, partnerCode);
        entityList.add(entity);

        when(cornellContentRepository.saveAll(anyList())).thenReturn(entityList);
        when(cornellContentRepository.countByPartnerCode(partnerCode)).thenReturn(1L);
        when(esUtilService.addDocument(anyString(), anyString(), anyMap(), anyString())).thenReturn("doc-id");
        when(cbServerProperties.getElasticCiosContentJsonPath()).thenReturn("/path/to/mapping.json");

        JsonNode partnerResponse = createMockPartnerInfoResponse(partnerId);
        when(restTemplate.exchange(
            anyString(),
            eq(HttpMethod.GET),
            any(HttpEntity.class),
            eq(Map.class)
        )).thenReturn(ResponseEntity.ok(createPartnerApiResponse(partnerResponse)));

        when(restTemplate.postForEntity(anyString(), any(HttpEntity.class), eq(String.class)))
            .thenReturn(ResponseEntity.ok("success"));

        
        ReflectionTestUtils.invokeMethod(dataTransformUtility, "dataBulkSave", entityList, partnerCode);

        verify(esUtilService, times(1)).addDocument(
            anyString(),
            anyString(),
            argThat(map -> !map.containsKey(Constants.CIOS_DATA)), // After flattening, CIOS_DATA should be removed
            anyString()
        );
    }

    @Test
    void dataBulkSave_updatesPartnerInfo() {
        
        String partnerCode = "TEST_PARTNER";
        String partnerId = "partner-123";
        List<CornellContentEntity> entityList = new ArrayList<>();

        CornellContentEntity entity = createMockContentEntity("ext-001", partnerId, partnerCode);
        entityList.add(entity);

        when(cornellContentRepository.saveAll(anyList())).thenReturn(entityList);
        when(cornellContentRepository.countByPartnerCode(partnerCode)).thenReturn(1L);
        when(esUtilService.addDocument(anyString(), anyString(), anyMap(), anyString())).thenReturn("doc-id");
        when(cbServerProperties.getElasticCiosContentJsonPath()).thenReturn("/path/to/mapping.json");

        JsonNode partnerResponse = createMockPartnerInfoResponse(partnerId);
        when(restTemplate.exchange(
            anyString(),
            eq(HttpMethod.GET),
            any(HttpEntity.class),
            eq(Map.class)
        )).thenReturn(ResponseEntity.ok(createPartnerApiResponse(partnerResponse)));

        when(restTemplate.postForEntity(anyString(), any(HttpEntity.class), eq(String.class)))
            .thenReturn(ResponseEntity.ok("success"));

        
        ReflectionTestUtils.invokeMethod(dataTransformUtility, "dataBulkSave", entityList, partnerCode);

       
        verify(restTemplate, times(1)).postForEntity(
            anyString(),
            any(HttpEntity.class),
            eq(String.class)
        );
    }

    private CornellContentEntity createMockContentEntity(String externalId, String partnerId, String partnerCode) {
        CornellContentEntity entity = new CornellContentEntity();
        entity.setExternalId(externalId);
        entity.setPartnerId(partnerId);
        entity.setPartnerCode(partnerCode);
        entity.setIsActive(false);
        entity.setCreatedDate(new Timestamp(System.currentTimeMillis()));
        entity.setUpdatedDate(new Timestamp(System.currentTimeMillis()));

        ObjectNode ciosData = objectMapper.createObjectNode();
        ObjectNode content = objectMapper.createObjectNode();
        content.put("name", "Test Content " + externalId);
        content.put("status", Constants.NOT_INITIATED);
        ciosData.set(Constants.CONTENT, content);
        entity.setCiosData(ciosData);

        return entity;
    }

    private JsonNode createMockPartnerInfoResponse(String partnerId) {
        ObjectNode response = objectMapper.createObjectNode();
        ObjectNode data = objectMapper.createObjectNode();
        data.put("id", partnerId);
        data.put("partnerName", "Test Partner");
        data.put(Constants.TOTAL_COURSES_COUNT, 0);
        response.set(Constants.DATA, data);
        return response;
    }

    private Map<String, Object> createPartnerApiResponse(JsonNode partnerData) {
        Map<String, Object> apiResponse = new HashMap<>();
        apiResponse.put(Constants.RESULT, objectMapper.convertValue(partnerData, Map.class));
        return apiResponse;
    }
}
