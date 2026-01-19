package com.igot.cios.sso.service;


import com.fasterxml.jackson.databind.ObjectMapper;
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
import org.springframework.http.HttpStatus;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class SSOServiceImplTest {

    @Mock
    private DataTransformUtility dataTransformUtility;

    @Mock
    private SsoRepository ssoRepository;

    @Mock
    private PayloadValidation payloadValidation;

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
                payloadValidation
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
        when(dataTransformUtility.getExistingMappers(eq(token), eq(ssoId))).thenReturn(new HashMap<>());
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


}

