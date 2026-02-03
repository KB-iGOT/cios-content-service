package com.igot.cios.sso.service;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.igot.cios.dto.SBApiResponse;
import com.igot.cios.plugins.DataTransformUtility;
import com.igot.cios.sso.entity.SSOConfiguration;
import com.igot.cios.sso.repository.SsoRepository;
import com.igot.cios.util.Constants;
import com.igot.cios.util.PayloadValidation;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class SSOServiceImplTest {

    @Mock
    private DataTransformUtility dataTransformUtility;

    @Mock
    private SsoRepository ssoRepository;

    @Mock
    private PayloadValidation payloadValidation;

    @Mock
    private RestTemplate restTemplate;

    private ObjectMapper objectMapper;

    private SSOServiceImpl service;

    private final String partnerId = "partner-123";
    private final String token = "admin-token";
    private final String ssoId = "keycloak-client-uuid";

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        service = new SSOServiceImpl(
                dataTransformUtility,
                ssoRepository,
                objectMapper,
                payloadValidation,
                restTemplate
        );
    }

    // ---------------- CREATE ----------------

    @Test
    void createSsoConfiguration_success() {
        ObjectNode payload = baseCreatePayload();

        when(ssoRepository.findById("p1")).thenReturn(Optional.empty());
        when(dataTransformUtility.getAdminAccessToken()).thenReturn("token");
        when(dataTransformUtility.createSsoConfiguration(any(), any()))
                .thenReturn("kc-client-id");
        when(ssoRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));

        SBApiResponse response =
                service.createSsoConfiguration(payload, "p1");

        assertEquals(HttpStatus.OK, response.getResponseCode());
        verify(dataTransformUtility).createSsoConfiguration(any(), any());
        verify(ssoRepository).save(any());
    }

    @Test
    void createSsoConfiguration_alreadyExists() {
        when(ssoRepository.findById("p1"))
                .thenReturn(Optional.of(new SSOConfiguration()));

        SBApiResponse response =
                service.createSsoConfiguration(baseCreatePayload(), "p1");

        assertEquals(Constants.FAILED, response.getParams().getStatus());
        verify(dataTransformUtility, never()).createSsoConfiguration(any(), any());
    }

    @Test
    void createSsoConfiguration_missingMandatoryFields() {
        ObjectNode payload = objectMapper.createObjectNode();

        SBApiResponse response =
                service.createSsoConfiguration(payload, "p1");

        assertEquals(Constants.FAILED, response.getParams().getStatus());
    }

    @Test
    void updateSsoConfiguration_notExists() {
        when(ssoRepository.findById("p1")).thenReturn(Optional.empty());

        SBApiResponse response =
                service.updateSsoConfiguration(baseUpdatePayload(), "p1");

        assertEquals(Constants.FAILED, response.getParams().getStatus());
    }

    @Test
    void updateSsoConfiguration_missingSsoId() {
        ObjectNode payload = baseUpdatePayload();
        payload.remove(Constants.SSO_ID);

        when(ssoRepository.findById("p1"))
                .thenReturn(Optional.of(new SSOConfiguration()));

        SBApiResponse response =
                service.updateSsoConfiguration(payload, "p1");

        assertEquals(Constants.FAILED, response.getParams().getStatus());
    }

    // ---------------- READ ----------------

    @Test
    void readSsoConfiguration_success() {
        SSOConfiguration config = new SSOConfiguration();
        when(ssoRepository.findById("p1"))
                .thenReturn(Optional.of(config));

        SBApiResponse response = service.readSsoConfiguration("p1");

        assertEquals(HttpStatus.OK, response.getResponseCode());
    }

    @Test
    void readSsoConfiguration_notFound() {
        when(ssoRepository.findById("p1"))
                .thenReturn(Optional.empty());

        SBApiResponse response = service.readSsoConfiguration("p1");

        assertEquals(Constants.FAILED, response.getParams().getStatus());
    }

    // ---------------- HELPERS ----------------

    private ObjectNode baseCreatePayload() {
        ObjectNode node = objectMapper.createObjectNode();
        node.put(Constants.CLIENT_ID, "client");
        node.put(Constants.PARTNER_NAME, "partner");
        node.put(Constants.SSO_PROTOCOL, "saml");
        node.put(Constants.ROOT_URL, "https://example.com");
        node.set(Constants.VALID_REDIRECT_URL, objectMapper.createArrayNode().add("https://example.com/callback"));
        node.put(Constants.ACS_URL, "https://example.com/acs");
        node.put(Constants.STATUS, true);
        node.put(Constants.SIGN_ASSERTIONS, "true");
        node.put(Constants.CLIENT_SIGNATURE_REQUIRED, "false");
        node.put(Constants.ENCRYPT_ASSERTIONS, "false");
        node.put(Constants.SIGNATURE_ALGORITHM, "RSA_SHA256");
        node.put(Constants.INCLUDE_AUTH_STATEMENT, "true");
        node.put(Constants.SIGN_DOCUMENTS, "true");
        node.put(Constants.OPTIMIZE_REDIRECT_SIGNING_KEYLOOKUP, "true");
        node.put(Constants.SAML_SIGNATURE_KEY_NAME, "CERT_SUBJECT");
        node.put(Constants.FORCE_POST_BINDING, "true");
        node.put(Constants.FORCE_NAMEID_FORMAT, "true");
        node.put(Constants.NAMEID_FORMAT, "username");
        return node;
    }

    private ObjectNode baseUpdatePayload() {
        ObjectNode node = baseCreatePayload();
        node.put(Constants.SSO_ID, "kc-id");
        node.put(Constants.CONFIGURATION, Constants.INCOMPLETE);

        ObjectNode mappers = objectMapper.createObjectNode();
        mappers.put(Constants.USERNAME, "uuid");
        node.set(Constants.MAPPERS, mappers);

        return node;
    }

    @Test
    void createSsoConfiguration_success_withMappers() {
        ObjectNode payload = setupValidSsoDetailsForCreate();

        when(ssoRepository.findById("p1")).thenReturn(Optional.empty());
        when(dataTransformUtility.getAdminAccessToken()).thenReturn("token");
        when(dataTransformUtility.createSsoConfiguration(any(), any()))
                .thenReturn("kc-client-id");
        when(ssoRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));

        SBApiResponse response =
                service.createSsoConfiguration(payload, "p1");

        assertEquals(HttpStatus.OK, response.getResponseCode());
        verify(dataTransformUtility).createSsoConfiguration(any(), any());
        verify(ssoRepository).save(any());
    }

    @Test
    void updateSsoConfiguration_success_withMappers() {
        ObjectNode ssoDetails = setupValidSsoDetailsForUpdate();
        SSOConfiguration existingConfig = new SSOConfiguration();
        existingConfig.setCreatedOn(new java.sql.Timestamp(System.currentTimeMillis()));

        when(ssoRepository.findById(partnerId)).thenReturn(Optional.of(existingConfig));
        when(dataTransformUtility.getAdminAccessToken()).thenReturn(token);
        when(dataTransformUtility.getExistingMappers((token), (ssoId))).thenReturn(new HashMap<>());
        doNothing().when(dataTransformUtility).updateSsoConfiguration(anyString(), anyString(), any());
        doNothing().when(payloadValidation).validatePayload(anyString(), any());
        when(ssoRepository.save(any())).thenReturn(existingConfig);

        SBApiResponse response = service.updateSsoConfiguration(ssoDetails, partnerId);

        assertEquals(HttpStatus.OK, response.getResponseCode());
        verify(dataTransformUtility, times(1)).updateSsoConfiguration(anyString(), anyString(), any());
        verify(ssoRepository, times(1)).save(any());
    }

    private ObjectNode setupValidSsoDetailsForCreate() {
        ObjectNode node = objectMapper.createObjectNode();
        node.put(Constants.CLIENT_ID, "test-client");
        node.put(Constants.PARTNER_NAME, "Test Partner");
        node.put(Constants.SSO_PROTOCOL, "saml");
        node.put(Constants.ROOT_URL, "http://localhost:8080");
        node.put(Constants.STATUS, true);
        node.put(Constants.ACS_URL, "http://localhost:8080/acs");
        node.put(Constants.SIGN_ASSERTIONS, "true");
        node.put(Constants.CLIENT_SIGNATURE_REQUIRED, "false");
        node.put(Constants.ENCRYPT_ASSERTIONS, "false");
        node.put(Constants.SIGNATURE_ALGORITHM, "RSA_SHA256");
        node.put(Constants.INCLUDE_AUTH_STATEMENT, "true");
        node.put(Constants.SIGN_DOCUMENTS, "true");
        node.put(Constants.OPTIMIZE_REDIRECT_SIGNING_KEYLOOKUP, "true");
        node.put(Constants.SAML_SIGNATURE_KEY_NAME, "CERT_SUBJECT");
        node.put(Constants.FORCE_POST_BINDING, "true");
        node.put(Constants.FORCE_NAMEID_FORMAT, "true");
        node.put(Constants.NAMEID_FORMAT, "username");
        node.set(Constants.VALID_REDIRECT_URL,
                objectMapper.valueToTree(List.of("http://localhost:8080/callback")));
        node.set(Constants.MAPPERS,
                objectMapper.createObjectNode().put("email", "uuid@karmayogi.com"));
        return node;
    }

    private ObjectNode setupValidSsoDetailsForUpdate() {
        ObjectNode node = setupValidSsoDetailsForCreate();
        node.put(Constants.SSO_ID, ssoId);
        node.put(Constants.CONFIGURATION, Constants.INCOMPLETE);
        return node;
    }

    @Test
    void testSamlConfiguration_missingSsoId() {
        ObjectNode request = objectMapper.createObjectNode();
        request.put(Constants.COURSE_DEEPLINK, "https://example.com/course/123");

        SBApiResponse response = service.testSamlConfiguration(request);

        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(Constants.MISSING_SSO_ID, response.getParams().getErrmsg());
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void testSamlConfiguration_missingCourseDeeplink() {
        ObjectNode request = objectMapper.createObjectNode();
        request.put(Constants.SSO_ID, ssoId);

        SBApiResponse response = service.testSamlConfiguration(request);

        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(Constants.MISSING_COURSE_DEEPLINK, response.getParams().getErrmsg());
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void testSamlConfiguration_invalidCourseDeeplinkUrl() {
        ObjectNode request = objectMapper.createObjectNode();
        request.put(Constants.SSO_ID, ssoId);
        request.put(Constants.COURSE_DEEPLINK, "ftp://invalid.com");

        SBApiResponse response = service.testSamlConfiguration(request);

        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals("Invalid courseDeeplink URL", response.getParams().getErrmsg());
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void testSamlConfiguration_spNotFoundInKeycloak() {
        ObjectNode request = objectMapper.createObjectNode();
        request.put(Constants.SSO_ID, ssoId);
        request.put(Constants.COURSE_DEEPLINK, "https://example.com/course/123");

        when(dataTransformUtility.getAdminAccessToken()).thenReturn(token);
        when(dataTransformUtility.getSsoConfigurationFromKeycloak(token, ssoId)).thenReturn(null);

        SBApiResponse response = service.testSamlConfiguration(request);

        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals("SP not found in Keycloak", response.getParams().getErrmsg());
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void testSamlConfiguration_clientProtocolNotSaml() {
        ObjectNode request = objectMapper.createObjectNode();
        request.put(Constants.SSO_ID, ssoId);
        request.put(Constants.COURSE_DEEPLINK, "https://example.com/course/123");

        ObjectNode client = objectMapper.createObjectNode();
        client.put(Constants.PROTOCOL, "openid-connect");

        when(dataTransformUtility.getAdminAccessToken()).thenReturn(token);
        when(dataTransformUtility.getSsoConfigurationFromKeycloak(token, ssoId)).thenReturn(client);

        SBApiResponse response = service.testSamlConfiguration(request);

        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals("Client protocol is not SAML", response.getParams().getErrmsg());
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void testSamlConfiguration_missingAcsUrl() {
        ObjectNode request = objectMapper.createObjectNode();
        request.put(Constants.SSO_ID, ssoId);
        request.put(Constants.COURSE_DEEPLINK, "https://example.com/course/123");

        ObjectNode client = objectMapper.createObjectNode();
        client.put(Constants.PROTOCOL, Constants.SAML);
        client.set(Constants.ATTRIBUTES, objectMapper.createObjectNode());

        when(dataTransformUtility.getAdminAccessToken()).thenReturn(token);
        when(dataTransformUtility.getSsoConfigurationFromKeycloak(token, ssoId)).thenReturn(client);

        SBApiResponse response = service.testSamlConfiguration(request);

        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals("Missing ACS URL in SP configuration", response.getParams().getErrmsg());
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void testSamlConfiguration_courseDeeplinkDomainMismatch() {
        ObjectNode request = objectMapper.createObjectNode();
        request.put(Constants.SSO_ID, ssoId);
        request.put(Constants.COURSE_DEEPLINK, "https://different.com/course/123");

        JsonNode keycloakClient = setupKeycloakClientResponse();

        when(dataTransformUtility.getAdminAccessToken()).thenReturn(token);
        when(dataTransformUtility.getSsoConfigurationFromKeycloak(token, ssoId)).thenReturn(keycloakClient);

        SBApiResponse response = service.testSamlConfiguration(request);

        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals("Course deeplink does not match SP redirect URI domain", response.getParams().getErrmsg());
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void testSamlConfiguration_spDidNotReturnSamlForm() {
        ObjectNode request = objectMapper.createObjectNode();
        request.put(Constants.SSO_ID, ssoId);
        request.put(Constants.COURSE_DEEPLINK, "https://example.com/course/123");

        JsonNode keycloakClient = setupKeycloakClientResponse();

        when(dataTransformUtility.getAdminAccessToken()).thenReturn(token);
        when(dataTransformUtility.getSsoConfigurationFromKeycloak(token, ssoId)).thenReturn(keycloakClient);
        when(restTemplate.exchange(anyString(), eq(HttpMethod.GET), any(), eq(String.class)))
                .thenReturn(ResponseEntity.status(HttpStatus.NOT_FOUND).body(null));

        SBApiResponse response = service.testSamlConfiguration(request);

        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertTrue(response.getParams().getErrmsg().contains("SP did not return SAML form"));
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void testSamlConfiguration_responseDoesNotContainSamlRequest() {
        ObjectNode request = objectMapper.createObjectNode();
        request.put(Constants.SSO_ID, ssoId);
        request.put(Constants.COURSE_DEEPLINK, "https://example.com/course/123");

        JsonNode keycloakClient = setupKeycloakClientResponse();
        String htmlWithoutSaml = "<html><body><p>No SAML here</p></body></html>";

        when(dataTransformUtility.getAdminAccessToken()).thenReturn(token);
        when(dataTransformUtility.getSsoConfigurationFromKeycloak(token, ssoId)).thenReturn(keycloakClient);
        when(restTemplate.exchange(anyString(), eq(HttpMethod.GET), any(), eq(String.class)))
                .thenReturn(ResponseEntity.ok(htmlWithoutSaml));

        SBApiResponse response = service.testSamlConfiguration(request);

        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertTrue(response.getParams().getErrmsg().contains("Response does not contain SAMLRequest"));
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void testSamlConfiguration_missingSamlFormOrInput() {
        ObjectNode request = objectMapper.createObjectNode();
        request.put(Constants.SSO_ID, ssoId);
        request.put(Constants.COURSE_DEEPLINK, "https://example.com/course/123");

        JsonNode keycloakClient = setupKeycloakClientResponse();
        String htmlWithSamlButNoForm = "<html><body><p>SAMLRequest mentioned but no form</p></body></html>";

        when(dataTransformUtility.getAdminAccessToken()).thenReturn(token);
        when(dataTransformUtility.getSsoConfigurationFromKeycloak(token, ssoId)).thenReturn(keycloakClient);
        when(restTemplate.exchange(anyString(), eq(HttpMethod.GET), any(), eq(String.class)))
                .thenReturn(ResponseEntity.ok(htmlWithSamlButNoForm));

        SBApiResponse response = service.testSamlConfiguration(request);

        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertTrue(response.getParams().getErrmsg().contains("Missing SAML form or SAMLRequest input"));
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void testSamlConfiguration_invalidSamlFormMissingAction() {
        ObjectNode request = objectMapper.createObjectNode();
        request.put(Constants.SSO_ID, ssoId);
        request.put(Constants.COURSE_DEEPLINK, "https://example.com/course/123");

        JsonNode keycloakClient = setupKeycloakClientResponse();
        String htmlWithInvalidForm = """
            <html>
            <body>
            <form method="post">
                <input type="hidden" name="SAMLRequest" value="encodedSamlRequest"/>
            </form>
            </body>
            </html>
            """;

        when(dataTransformUtility.getAdminAccessToken()).thenReturn(token);
        when(dataTransformUtility.getSsoConfigurationFromKeycloak(token, ssoId)).thenReturn(keycloakClient);
        when(restTemplate.exchange(anyString(), eq(HttpMethod.GET), any(), eq(String.class)))
                .thenReturn(ResponseEntity.ok(htmlWithInvalidForm));

        SBApiResponse response = service.testSamlConfiguration(request);

        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertTrue(response.getParams().getErrmsg().contains("Invalid SAML form"));
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void testSamlConfiguration_validSamlRequestWithCorrectIssuer() {
        ObjectNode request = objectMapper.createObjectNode();
        request.put(Constants.SSO_ID, ssoId);
        request.put(Constants.COURSE_DEEPLINK, "https://example.com/course/123");

        JsonNode keycloakClient = setupKeycloakClientResponse();

        String validSamlXml = """
            <?xml version="1.0" encoding="UTF-8"?>
            <samlp:AuthnRequest xmlns:samlp="urn:oasis:names:tc:SAML:2.0:protocol" ID="id123" Version="2.0">
                <saml:Issuer xmlns:saml="urn:oasis:names:tc:SAML:2.0:assertion">test-client</saml:Issuer>
            </samlp:AuthnRequest>
            """;

        String base64EncodedSaml = Base64.getEncoder().encodeToString(validSamlXml.getBytes(StandardCharsets.UTF_8));

        String samlFormHtml = String.format("""
            <html>
            <body>
            <form method="post" action="https://idp.example.com/saml/sso">
                <input type="hidden" name="SAMLRequest" value="%s"/>
            </form>
            </body>
            </html>
            """, base64EncodedSaml);

        when(dataTransformUtility.getAdminAccessToken()).thenReturn(token);
        when(dataTransformUtility.getSsoConfigurationFromKeycloak(token, ssoId)).thenReturn(keycloakClient);
        when(dataTransformUtility.inflateAndDecode(anyString())).thenReturn(validSamlXml);
        when(restTemplate.exchange(anyString(), eq(HttpMethod.GET), any(), eq(String.class)))
                .thenReturn(ResponseEntity.ok(samlFormHtml));

        SBApiResponse response = service.testSamlConfiguration(request);

        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
        assertEquals(HttpStatus.OK, response.getResponseCode());
        assertNotNull(response.getResult());
    }

    @Test
    void testSamlConfiguration_issuerMismatch() {
        ObjectNode request = objectMapper.createObjectNode();
        request.put(Constants.SSO_ID, ssoId);
        request.put(Constants.COURSE_DEEPLINK, "https://example.com/course/123");

        JsonNode keycloakClient = setupKeycloakClientResponse();

        String invalidSamlXml = """
            <?xml version="1.0" encoding="UTF-8"?>
            <samlp:AuthnRequest xmlns:samlp="urn:oasis:names:tc:SAML:2.0:protocol" ID="id123" Version="2.0">
                <saml:Issuer xmlns:saml="urn:oasis:names:tc:SAML:2.0:assertion">wrong-issuer</saml:Issuer>
            </samlp:AuthnRequest>
            """;

        String base64EncodedSaml = Base64.getEncoder().encodeToString(invalidSamlXml.getBytes(StandardCharsets.UTF_8));

        String samlFormHtml = String.format("""
            <html>
            <body>
            <form method="post" action="https://idp.example.com/saml/sso">
                <input type="hidden" name="SAMLRequest" value="%s"/>
            </form>
            </body>
            </html>
            """, base64EncodedSaml);

        when(dataTransformUtility.getAdminAccessToken()).thenReturn(token);
        when(dataTransformUtility.getSsoConfigurationFromKeycloak(token, ssoId)).thenReturn(keycloakClient);
        when(dataTransformUtility.inflateAndDecode(anyString())).thenReturn(invalidSamlXml);
        when(restTemplate.exchange(anyString(), eq(HttpMethod.GET), any(), eq(String.class)))
                .thenReturn(ResponseEntity.ok(samlFormHtml));

        SBApiResponse response = service.testSamlConfiguration(request);

        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertTrue(response.getParams().getErrmsg().contains("Issuer mismatch"));
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void testSamlConfiguration_samlRequestMissingIssuer() {
        ObjectNode request = objectMapper.createObjectNode();
        request.put(Constants.SSO_ID, ssoId);
        request.put(Constants.COURSE_DEEPLINK, "https://example.com/course/123");

        JsonNode keycloakClient = setupKeycloakClientResponse();

        String samlXmlWithoutIssuer = """
            <?xml version="1.0" encoding="UTF-8"?>
            <samlp:AuthnRequest xmlns:samlp="urn:oasis:names:tc:SAML:2.0:protocol" ID="id123" Version="2.0">
            </samlp:AuthnRequest>
            """;

        String base64EncodedSaml = Base64.getEncoder().encodeToString(samlXmlWithoutIssuer.getBytes(StandardCharsets.UTF_8));

        String samlFormHtml = String.format("""
            <html>
            <body>
            <form method="post" action="https://idp.example.com/saml/sso">
                <input type="hidden" name="SAMLRequest" value="%s"/>
            </form>
            </body>
            </html>
            """, base64EncodedSaml);

        when(dataTransformUtility.getAdminAccessToken()).thenReturn(token);
        when(dataTransformUtility.getSsoConfigurationFromKeycloak(token, ssoId)).thenReturn(keycloakClient);
        when(dataTransformUtility.inflateAndDecode(anyString())).thenReturn(samlXmlWithoutIssuer);
        when(restTemplate.exchange(anyString(), eq(HttpMethod.GET), any(), eq(String.class)))
                .thenReturn(ResponseEntity.ok(samlFormHtml));

        SBApiResponse response = service.testSamlConfiguration(request);

        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertTrue(response.getParams().getErrmsg().contains("SAML request missing Issuer element"));
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void testSamlConfiguration_exceptionDuringProcessing() {
        ObjectNode request = objectMapper.createObjectNode();
        request.put(Constants.SSO_ID, ssoId);
        request.put(Constants.COURSE_DEEPLINK, "https://example.com/course/123");

        when(dataTransformUtility.getAdminAccessToken()).thenThrow(new RuntimeException("Token fetch failed"));

        SBApiResponse response = service.testSamlConfiguration(request);

        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertTrue(response.getParams().getErrmsg().contains("Exception while testing SAML"));
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void testSamlConfiguration_restTemplateThrowsException() {
        ObjectNode request = objectMapper.createObjectNode();
        request.put(Constants.SSO_ID, ssoId);
        request.put(Constants.COURSE_DEEPLINK, "https://example.com/course/123");

        JsonNode keycloakClient = setupKeycloakClientResponse();

        when(dataTransformUtility.getAdminAccessToken()).thenReturn(token);
        when(dataTransformUtility.getSsoConfigurationFromKeycloak(token, ssoId)).thenReturn(keycloakClient);
        when(restTemplate.exchange(anyString(), eq(HttpMethod.GET), any(), eq(String.class)))
                .thenThrow(new RuntimeException("Connection timeout"));

        SBApiResponse response = service.testSamlConfiguration(request);

        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertTrue(response.getParams().getErrmsg().contains("Exception during SP redirect"));
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    private JsonNode setupKeycloakClientResponse() {
        ObjectNode client = objectMapper.createObjectNode();
        client.put(Constants.CLIENT_ID, "test-client");
        client.put(Constants.PROTOCOL, Constants.SAML);

        ObjectNode attributes = objectMapper.createObjectNode();
        attributes.put("saml_assertion_consumer_url_post", "https://example.com/acs");
        client.set(Constants.ATTRIBUTES, attributes);

        ArrayNode redirectUris = objectMapper.createArrayNode();
        redirectUris.add("https://example.com/callback");
        redirectUris.add("https://example.com/*");
        client.set("redirectUris", redirectUris);

        return client;
    }

}
