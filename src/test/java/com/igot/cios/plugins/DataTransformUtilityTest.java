package com.igot.cios.plugins;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cios.entity.FileInfoEntity;
import com.igot.cios.exception.CiosContentException;
import com.igot.cios.repository.CornellContentRepository;
import com.igot.cios.repository.FileInfoRepository;
import com.igot.cios.util.CbServerProperties;
import com.igot.cios.util.elasticsearch.service.EsUtilService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.*;
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

    @Mock
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

}
