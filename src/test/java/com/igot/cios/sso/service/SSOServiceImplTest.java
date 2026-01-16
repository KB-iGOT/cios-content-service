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

import java.security.Timestamp;
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
        ObjectNode ssoDetails = baseCreatePayload();

        when(ssoRepository.findById("p1")).thenReturn(Optional.empty());
        when(dataTransformUtility.getAdminAccessToken()).thenReturn("token");
        when(dataTransformUtility.createSsoConfiguration(any(), any()))
                .thenReturn("kc-client-id");
        when(ssoRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));

        SBApiResponse response =
                service.createSsoConfiguration(ssoDetails, "p1");

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
        ObjectNode ssoDetails = objectMapper.createObjectNode();

        SBApiResponse response =
                service.createSsoConfiguration(ssoDetails, "p1");

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
        ObjectNode ssoDetails = baseUpdatePayload();
        ssoDetails.remove(Constants.SSO_ID);

        when(ssoRepository.findById("p1"))
                .thenReturn(Optional.of(new SSOConfiguration()));

        SBApiResponse response =
                service.updateSsoConfiguration(ssoDetails, "p1");

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
        return node;
    }

    private ObjectNode baseUpdatePayload() {
        ObjectNode node = baseCreatePayload();
        node.put(Constants.SSO_ID, "kc-id");
        node.put(Constants.CONFIGURATION, Constants.INCOMPLETE);
        node.put(Constants.USER_ATTRIBUTE, "UserName");
        node.put(Constants.ACS_URL, "https://example.com/acs");
        node.put(Constants.ROOT_URL, "https://example.com");
        node.set(Constants.VALID_REDIRECT_URL, objectMapper.createArrayNode().add("https://example.com/callback"));

        ObjectNode mappers = objectMapper.createObjectNode();
        mappers.put(Constants.USERNAME, "uuid");
        node.set(Constants.MAPPERS, mappers);

        return node;
    }


}

