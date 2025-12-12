package com.igot.cios.sso.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.igot.cios.dto.SBApiResponse;
import com.igot.cios.plugins.DataTransformUtility;
import com.igot.cios.sso.entity.SSOConfiguration;
import com.igot.cios.sso.repository.SsoRepository;
import com.igot.cios.util.Constants;
import com.igot.cios.util.PayloadValidation;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class SSOSeviceImpl implements SSOService{

    @Autowired
    private DataTransformUtility dataTransformUtility;

    @Autowired
    private SsoRepository ssoRepository;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private PayloadValidation payloadValidation;

    @Override
    public String disableSsoConfiguration(String id) {
        return null;
    }

    @Override
    public SBApiResponse createSsoConfiguration(JsonNode ssoDetails,String partnerId) {
        SBApiResponse response = SBApiResponse.createDefaultResponse(Constants.API_CB_PLAN_PUBLISH);
        if (ssoRepository.findById(partnerId).isPresent()) {
            response.getParams().setErrmsg("SSO configuration already exists for partner: " + partnerId);
            response.getParams().setStatus(Constants.FAILED);
            return response;
        }
        String missing = validateMandatoryFields(ssoDetails, "clientId", "partnerName", "ssoProtocol");
        if (missing != null) {
            response.getParams().setErrmsg("Missing mandatory fields: " + missing);
            response.getParams().setStatus(Constants.FAILED);
            return response;
        }
        Timestamp currentTime = new Timestamp(System.currentTimeMillis());
        String token = dataTransformUtility.getAdminAccessToken();

        Map<String,Object> client = constructSsoPayload(ssoDetails);
        String id = dataTransformUtility.createSsoConfiguration(token, client);
        if(StringUtils.isBlank(id)){
            response.getParams().setErrmsg("Failed to create SSO configuration in Keycloak");
            response.getParams().setStatus(Constants.FAILED);
            return response;
        }
        ((ObjectNode) ssoDetails).put(Constants.SSO_ID, id);
        ((ObjectNode) ssoDetails).put(Constants.STATUS, true);
        ((ObjectNode) ssoDetails).put(Constants.CONFIGURATION, Constants.INCOMPLETE);
        SSOConfiguration configuration=new SSOConfiguration();
        configuration.setPartnerId(partnerId);
        configuration.setSsoData(ssoDetails);
        configuration.setCreatedOn(currentTime);
        configuration.setUpdatedOn(currentTime);
        SSOConfiguration savedResponse = ssoRepository.save(configuration);
        Map<String, Object> result = objectMapper.convertValue(
                savedResponse,
                new TypeReference<Map<String, Object>>() {}
        );
        response.setResult(result);
        response.setResponseCode(HttpStatus.OK);
        return response;
    }

    @Override
    public SBApiResponse updateSsoConfiguration(JsonNode ssoDetails, String partnerId) {
        SBApiResponse response = SBApiResponse.createDefaultResponse(Constants.API_CB_PLAN_PUBLISH);
        if (!ssoRepository.findById(partnerId).isPresent()) {
            response.getParams().setErrmsg("SSO configuration not exists for partner: " + partnerId);
            response.getParams().setStatus(Constants.FAILED);
            return response;
        }
        if(ssoDetails.get(Constants.CONFIGURATION).asText().equalsIgnoreCase(Constants.COMPLETE)){
            payloadValidation.validatePayload(Constants.SSO_CONFIGURATION_VALIDATION_FILE_JSON, ssoDetails);
        }
        String missing = validateMandatoryFields(ssoDetails, Constants.SSO_ID);
        if (missing != null) {
            response.getParams().setErrmsg("Missing mandatory fields: " + missing);
            response.getParams().setStatus(Constants.FAILED);
            return response;
        }
        Timestamp currentTime = new Timestamp(System.currentTimeMillis());
        String token = dataTransformUtility.getAdminAccessToken();
        Map<String,Object> client = constructSsoPayload(ssoDetails);
        dataTransformUtility.updateSsoConfiguration(token, ssoDetails.get(Constants.SSO_ID).asText(), client);
        SSOConfiguration configuration=new SSOConfiguration();
        configuration.setPartnerId(partnerId);
        configuration.setSsoData(ssoDetails);
        configuration.setCreatedOn(ssoRepository.findById(partnerId).get().getCreatedOn());
        configuration.setUpdatedOn(currentTime);
        SSOConfiguration savedResponse = ssoRepository.save(configuration);
        Map<String, Object> result = objectMapper.convertValue(
                savedResponse,
                new TypeReference<Map<String, Object>>() {}
        );
        response.setResult(result);
        response.setResponseCode(HttpStatus.OK);
        return response;
    }

    @Override
    public SBApiResponse readSsoConfiguration(String id) {
        SBApiResponse response = SBApiResponse.createDefaultResponse(Constants.API_CB_PLAN_PUBLISH);
        SSOConfiguration configuration = ssoRepository.findById(id).orElse(null);
        if (configuration == null) {
            response.getParams().setErrmsg("SSO configuration not exists for partner: " + id);
            response.getParams().setStatus(Constants.FAILED);
            return response;
        }
        Map<String, Object> result = objectMapper.convertValue(
                configuration,
                new TypeReference<Map<String, Object>>() {}
        );
        response.setResult(result);
        response.setResponseCode(HttpStatus.OK);
        return response;
    }

    private String validateMandatoryFields(JsonNode node, String... fields) {
        List<String> missing = new ArrayList<>();
        for (String f : fields) {
            if (!node.hasNonNull(f) || node.get(f).asText().isBlank()) {
                missing.add(f);
            }
        }
        return missing.isEmpty() ? null : String.join(", ", missing);
    }


    private Map<String,Object> constructSsoPayload(JsonNode ssoDetails) {
        Map<String, Object> client = new HashMap<>();
        client.put(Constants.CLIENT_ID, ssoDetails.get(Constants.CLIENT_ID));
        client.put(Constants.NAME, ssoDetails.get(Constants.PARTNER_NAME));
        client.put(Constants.ENABLED, ssoDetails.path(Constants.ENABLED).asBoolean(true));
        client.put(Constants.PROTOCOL, ssoDetails.get(Constants.SSO_PROTOCOL));

        Map<String, String> attrs = new HashMap<>();
        attrs.put(Constants.SAML_ASSERTION_CONSUMER_URL_POST, ssoDetails.path(Constants.ACS_URL).asText(""));
        attrs.put(Constants.SAML_ASSERTION_CONSUMER_URL_REDIRECT, ssoDetails.path(Constants.ACS_URL).asText(""));
        attrs.put(Constants.SAML_SINGLE_LOGOUT_SERVICE_POST_URL, ssoDetails.path(Constants.SSO_URL).asText(""));
        attrs.put(Constants.SAML_SINGLE_LOGOUT_SERVICE_REDIRECT_URL, ssoDetails.path(Constants.SSO_URL).asText(""));

        attrs.put(Constants.SAML_ASSERTION_SIGNATURE, "true");
        attrs.put(Constants.SAML_CLIENT_SIGNATURE, "false");
        attrs.put(Constants.SAML_ENCRYPT, "false");
        attrs.put(Constants.SIGNATURE_ALGORITHM, Constants.SIGNATURE_ALGORITHM_RSA_SHA256);

        client.put(Constants.ATTRIBUTES, attrs);

        List<Map<String, Object>> mappers = new ArrayList<>();

        mappers.add(buildMapper(
                Constants.USERNAME,
                ssoDetails.path(Constants.USER_ATTRIBUTE).asText(Constants.USERNAME),
                Constants.ANONYMOUS
        ));

        mappers.add(buildMapper(
                Constants.LASTNAME,
                ssoDetails.path(Constants.LASTNAME_ATTRIBUTE).asText(Constants.LASTNAME),
                "userId = user.id; " +
                        "parts = userId.split(':'); " +
                        "lastPart = parts[parts.length - 1]; " +
                        "lastPart;"
        ));

        mappers.add(buildMapper(
                Constants.FIRSTNAME,
                ssoDetails.path(Constants.FIRSTNAME_ATTRIBUTE).asText(Constants.FIRSTNAME),
                "userId = user.id; " +
                        "parts = userId.split(':'); " +
                        "firstPart = parts[parts.length - 1]; " +
                        "firstPart;"
        ));

        mappers.add(buildMapper(
                Constants.EMAIL,
                ssoDetails.path(Constants.EMAIL_ATTRIBUTE).asText(Constants.EMAIL),
                "userId = user.id; \n" +
                        "parts = userId.split(':'); \n" +
                        "lastPart = parts[parts.length - 1]; \n" +
                        "email = lastPart + '@karmayogi.com'; \n" +
                        "email;"
        ));
        client.put(Constants.PROTOCOL_MAPPERS, mappers);
        return client;
    }

    private Map<String, Object> buildMapper(String name, String attribute, String script) {

        Map<String, Object> mapper = new HashMap<>();
        mapper.put(Constants.NAME, name);
        mapper.put(Constants.PROTOCOL, Constants.SAML);
        mapper.put(Constants.PROTOCOL_MAPPER, Constants.SAML_JAVASCRIPT_MAPPER);
        mapper.put(Constants.CONSENT_REQUIRED, false);

        Map<String, String> config = new HashMap<>();
        config.put(Constants.SINGLE, "true");
        config.put(Constants.ATTRIBUTE_NAME, attribute);
        config.put(Constants.SCRIPT, script);

        mapper.put(Constants.CONFIG, config);

        return mapper;
    }

}
