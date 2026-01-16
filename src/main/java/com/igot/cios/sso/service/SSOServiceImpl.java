package com.igot.cios.sso.service;

import com.fasterxml.jackson.core.type.TypeReference;
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
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import java.sql.Timestamp;
import java.util.*;


@Service
public class SSOServiceImpl implements SSOService {

    private final DataTransformUtility dataTransformUtility;
    private final SsoRepository ssoRepository;
    private final ObjectMapper objectMapper;
    private final PayloadValidation payloadValidation;
    private static final String UUID_SCRIPT = """
    userId = user.id;
    parts = userId.split(':');
    parts[parts.length - 1];
""";

    private static final String UUID_EMAIL_SCRIPT = """
    userId = user.id;
    parts = userId.split(':');
    lastPart = parts[parts.length - 1];
    lastPart + '@karmayogi.com';
""";

    @Autowired
    public SSOServiceImpl(
            DataTransformUtility dataTransformUtility,
            SsoRepository ssoRepository,
            ObjectMapper objectMapper,
            PayloadValidation payloadValidation
    ) {
        this.dataTransformUtility = dataTransformUtility;
        this.ssoRepository = ssoRepository;
        this.objectMapper = objectMapper;
        this.payloadValidation = payloadValidation;
    }

    @Override
    public SBApiResponse createSsoConfiguration(JsonNode ssoDetails, String partnerId) {
        SBApiResponse response = SBApiResponse.createDefaultResponse(Constants.API_SSO_CREATE);
        if (ssoRepository.findById(partnerId).isPresent()) {
            response.getParams().setErrmsg("SSO configuration already exists for partner: " + partnerId);
            response.getParams().setStatus(Constants.FAILED);
            return response;
        }
        String missing = validateMandatoryFields(ssoDetails, "clientId", "partnerName", "ssoProtocol");
        if (StringUtils.isNoneBlank(missing)) {
            response.getParams().setErrmsg("Missing mandatory fields: " + missing);
            response.getParams().setStatus(Constants.FAILED);
            return response;
        }
        constructSsoDefaultPayload(ssoDetails);
        payloadValidation.validatePayload(Constants.SSO_CONFIGURATION_VALIDATION_FILE_JSON, ssoDetails);
        Timestamp currentTime = new Timestamp(System.currentTimeMillis());
        String token = dataTransformUtility.getAdminAccessToken();

        Map<String, Object> client = constructSsoPayload(ssoDetails, new HashMap<>(), token);
        String id = dataTransformUtility.createSsoConfiguration(token, client);
        if (StringUtils.isBlank(id)) {
            response.getParams().setErrmsg("Failed to create SSO configuration in Keycloak");
            response.getParams().setStatus(Constants.FAILED);
            return response;
        }
        ((ObjectNode) ssoDetails).put(Constants.SSO_ID, id);
        ((ObjectNode) ssoDetails).put(Constants.STATUS, true);
        ((ObjectNode) ssoDetails).put(Constants.CONFIGURATION, Constants.INCOMPLETE);
        SSOConfiguration configuration = new SSOConfiguration();
        configuration.setPartnerId(partnerId);
        configuration.setSsoData(ssoDetails);
        configuration.setCreatedOn(currentTime);
        configuration.setUpdatedOn(currentTime);
        SSOConfiguration savedResponse = ssoRepository.save(configuration);
        Map<String, Object> result = objectMapper.convertValue(
                savedResponse,
                new TypeReference<Map<String, Object>>() {
                }
        );
        response.setResult(result);
        response.setResponseCode(HttpStatus.OK);
        return response;
    }

    @Override
    public SBApiResponse updateSsoConfiguration(JsonNode ssoDetails, String partnerId) {
        SBApiResponse response = SBApiResponse.createDefaultResponse(Constants.API_SSO_UPDATE);
        Optional<SSOConfiguration> existingOpt = ssoRepository.findById(partnerId);
        if (existingOpt.isEmpty()) {
            response.getParams().setErrmsg("SSO configuration not exists for partner: " + partnerId);
            response.getParams().setStatus(Constants.FAILED);
            return response;
        }
        if (ssoDetails.get(Constants.CONFIGURATION).asText().equalsIgnoreCase(Constants.COMPLETE)) {
            payloadValidation.validatePayload(Constants.FINAL_SSO_CONFIGURATION_VALIDATION_FILE_JSON, ssoDetails);
        }
        String missing = validateMandatoryFields(ssoDetails, Constants.SSO_ID);
        if (StringUtils.isNoneBlank(missing)) {
            response.getParams().setErrmsg("Missing mandatory fields: " + missing);
            response.getParams().setStatus(Constants.FAILED);
            return response;
        }
        payloadValidation.validatePayload(Constants.SSO_CONFIGURATION_VALIDATION_FILE_JSON, ssoDetails);
        Timestamp currentTime = new Timestamp(System.currentTimeMillis());
        String token = dataTransformUtility.getAdminAccessToken();
        Map<String, String> existingMapperIds = dataTransformUtility.getExistingMappers(token, ssoDetails.get(Constants.SSO_ID).asText());
        Map<String, Object> client = constructSsoPayload(ssoDetails, existingMapperIds, token);
        dataTransformUtility.updateSsoConfiguration(token, ssoDetails.get(Constants.SSO_ID).asText(), client);
        SSOConfiguration configuration = new SSOConfiguration();
        configuration.setPartnerId(partnerId);
        configuration.setSsoData(ssoDetails);
        configuration.setCreatedOn(existingOpt.get().getCreatedOn());
        configuration.setUpdatedOn(currentTime);
        SSOConfiguration savedResponse = ssoRepository.save(configuration);
        Map<String, Object> result = objectMapper.convertValue(
                savedResponse,
                new TypeReference<Map<String, Object>>() {
                }
        );
        response.setResult(result);
        response.setResponseCode(HttpStatus.OK);
        return response;
    }

    @Override
    public SBApiResponse readSsoConfiguration(String id) {
        SBApiResponse response = SBApiResponse.createDefaultResponse(Constants.API_SSO_READ);
        SSOConfiguration configuration = ssoRepository.findById(id).orElse(null);
        if (configuration == null) {
            response.getParams().setErrmsg("SSO configuration not exists for partner: " + id);
            response.getParams().setStatus(Constants.FAILED);
            return response;
        }
        Map<String, Object> result = objectMapper.convertValue(
                configuration,
                new TypeReference<Map<String, Object>>() {
                }
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


    private Map<String, Object> constructSsoPayload(JsonNode ssoDetails, Map<String, String> existingMapperIds, String token) {
        Map<String, Object> client = new HashMap<>();
        client.put(Constants.CLIENT_ID, ssoDetails.get(Constants.CLIENT_ID));
        client.put(Constants.NAME, ssoDetails.get(Constants.PARTNER_NAME));
        client.put(Constants.ENABLED, ssoDetails.path(Constants.STATUS).asBoolean(true));
        client.put(Constants.PROTOCOL, ssoDetails.get(Constants.SSO_PROTOCOL));
        client.put(Constants.ROOT_URL, ssoDetails.get(Constants.ROOT_URL));
        client.put(
                Constants.REDIRECT_URIS,
                objectMapper.convertValue(
                        ssoDetails.get(Constants.VALID_REDIRECT_URL),
                        List.class
                )
        );
        client.put(Constants.DEFAULT_CLIENT_SCOPES, List.of("web-origins"));
        client.put(Constants.OPTIONAL_CLIENT_SCOPES, Collections.emptyList());
        Map<String, String> attrs = new HashMap<>();
        attrs.put(Constants.SAML_ASSERTION_CONSUMER_URL_POST, ssoDetails.path(Constants.ACS_URL).asText(""));
        attrs.put(Constants.SAML_ASSERTION_CONSUMER_URL_REDIRECT, ssoDetails.path(Constants.ACS_URL).asText(""));
        attrs.put(Constants.SAML_ASSERTION_SIGNATURE, ssoDetails.get(Constants.SIGN_ASSERTIONS).asText());
        attrs.put(Constants.SAML_CLIENT_SIGNATURE, ssoDetails.get(Constants.CLIENT_SIGNATURE_REQUIRED).asText());
        attrs.put(Constants.SAML_ENCRYPT, ssoDetails.get(Constants.ENCRYPT_ASSERTIONS).asText());
        attrs.put(Constants.SAML_SIGNATURE_ALGORITHM, ssoDetails.get(Constants.SIGNATURE_ALGORITHM).asText());
        attrs.put(Constants.ATTRIBUTES_SAML_AUTHSTATEMENT, ssoDetails.get(Constants.INCLUDE_AUTH_STATEMENT).asText());
        attrs.put(Constants.ATTRIBUTES_SAML_SERVER_SIGNATURE, ssoDetails.get(Constants.SIGN_DOCUMENTS).asText());
        attrs.put(Constants.ATTRIBUTES_SAML_SERVER_SIGNATURE_KEYINFO, ssoDetails.get(Constants.OPTIMIZE_REDIRECT_SIGNING_KEYLOOKUP).asText());
        attrs.put(Constants.ATTRIBUTES_SAML_SERVER_SIGNATURE_KEY, ssoDetails.get(Constants.SAML_SIGNATURE_KEY_NAME).asText());
        attrs.put(Constants.SAML_FORCE_POST_BINDING, ssoDetails.get(Constants.FORCE_POST_BINDING).asText());
        attrs.put(Constants.SAML_FORCE_NAME_ID_FORMAT, ssoDetails.get(Constants.FORCE_NAMEID_FORMAT).asText());
        attrs.put(Constants.SAML_NAME_ID_FORMAT, ssoDetails.get(Constants.NAMEID_FORMAT).asText());

        client.put(Constants.ATTRIBUTES, attrs);

        List<Map<String, Object>> mappers = new ArrayList<>();
        JsonNode mapperNode = ssoDetails.path(Constants.MAPPERS);
        if (mapperNode.isObject()) {
            mapperNode.fields().forEachRemaining(entry -> {
                String mapperName = entry.getKey();
                String mapperValue = entry.getValue().asText();

                mappers.add(
                        buildDynamicMapper(mapperName, mapperValue, existingMapperIds)
                );
            });
        }

        client.put(Constants.PROTOCOL_MAPPERS, mappers);

        for (Map<String, Object> mapper : mappers) {
            if (mapper.containsKey(Constants.ID)) {
                dataTransformUtility.updateProtocolMapper(
                        token,
                        ssoDetails.get(Constants.SSO_ID).asText(),
                        mapper
                );
            }
        }
        return client;
    }

    private Map<String, Object> buildDynamicMapper(
            String mapperName,
            String mapperValue,
            Map<String, String> existingMapperIds
    ) {
        // CASE 1: uuid → uuid from userId
        if (Constants.UUID.equalsIgnoreCase(mapperValue)) {
            return buildScriptMapper(
                    mapperName,
                    mapperName,
                    UUID_SCRIPT,
                    existingMapperIds
            );
        }

        // CASE 2: uuid@karmayogi.com → email constructed from uuid
        if (Constants.UUID_EMAIL.equalsIgnoreCase(mapperValue)) {
            return buildScriptMapper(
                    mapperName,
                    mapperName,
                    UUID_EMAIL_SCRIPT,
                    existingMapperIds
            );
        }

        // CASE 3: userFullName → User Property Mapper
        if (Constants.USER_FULLNAME.equalsIgnoreCase(mapperValue)) {
            return buildUserPropertyMapper(
                    mapperName,
                    Constants.FIRSTNAME_KEY,
                    existingMapperIds
            );
        }

        // CASE 4: literal value (anonymous karmayogi, etc.)
        return buildScriptMapper(
                mapperName,
                mapperName,
                "'" + mapperValue + "'",
                existingMapperIds
        );
    }

    private Map<String, Object> buildScriptMapper(
            String name,
            String attributeName,
            String script,
            Map<String, String> existingMapperIds
    ) {
        Map<String, Object> mapper = new HashMap<>();

        if (existingMapperIds.containsKey(name)) {
            mapper.put(Constants.ID, existingMapperIds.get(name));
        }

        mapper.put(Constants.NAME, name);
        mapper.put(Constants.PROTOCOL, Constants.SAML);
        mapper.put(Constants.PROTOCOL_MAPPER, Constants.SAML_JAVASCRIPT_MAPPER);
        mapper.put(Constants.CONSENT_REQUIRED, false);

        Map<String, String> config = new HashMap<>();
        config.put(Constants.SINGLE, Constants.TRUE);
        config.put(Constants.ATTRIBUTE_NAME, attributeName);
        config.put(Constants.ATTRIBUTE_NAMEFORMAT, Constants.BASIC);
        config.put(Constants.SCRIPT, script);

        mapper.put(Constants.CONFIG, config);
        return mapper;
    }

    private Map<String, Object> buildUserPropertyMapper(
            String name,
            String userProperty,
            Map<String, String> existingMapperIds
    ) {
        Map<String, Object> mapper = new HashMap<>();

        if (existingMapperIds.containsKey(name)) {
            mapper.put(Constants.ID, existingMapperIds.get(name));
        }

        mapper.put(Constants.NAME, name);
        mapper.put(Constants.PROTOCOL, Constants.SAML);
        mapper.put(Constants.PROTOCOL_MAPPER, Constants.SAML_USER_PROPERTY_MAPPER);
        mapper.put(Constants.CONSENT_REQUIRED, false);

        Map<String, String> config = new HashMap<>();
        config.put(Constants.USER_ATTRIBUTE, userProperty);
        config.put(Constants.ATTRIBUTE_NAME, name);
        config.put(Constants.FRIENDLY_NAME, name);
        config.put(Constants.ATTRIBUTE_NAMEFORMAT, Constants.BASIC);

        mapper.put(Constants.CONFIG, config);
        return mapper;
    }

    private void constructSsoDefaultPayload(JsonNode ssoDetails) {
        ObjectNode ssoData = (ObjectNode) ssoDetails;
        ssoData.put(Constants.INCLUDE_AUTH_STATEMENT, ssoDetails.path(Constants.INCLUDE_AUTH_STATEMENT).asBoolean(true));
        ssoData.put(Constants.SIGN_DOCUMENTS, ssoDetails.path(Constants.SIGN_DOCUMENTS).asBoolean(true));
        ssoData.put(Constants.OPTIMIZE_REDIRECT_SIGNING_KEYLOOKUP, ssoDetails.path(Constants.OPTIMIZE_REDIRECT_SIGNING_KEYLOOKUP).asBoolean(true));
        ssoData.put(Constants.SIGN_ASSERTIONS, ssoDetails.path(Constants.SIGN_ASSERTIONS).asBoolean(true));
        ssoData.put(Constants.SIGNATURE_ALGORITHM, ssoDetails.path(Constants.SIGNATURE_ALGORITHM).asText(Constants.RSA_SHA256));
        ssoData.put(Constants.SAML_SIGNATURE_KEY_NAME, ssoDetails.path(Constants.SAML_SIGNATURE_KEY_NAME).asText(Constants.CERT_SUBJECT));
        ssoData.put(Constants.FORCE_POST_BINDING, ssoDetails.path(Constants.FORCE_POST_BINDING).asBoolean(true));
        ssoData.put(Constants.ENCRYPT_ASSERTIONS, ssoDetails.path(Constants.ENCRYPT_ASSERTIONS).asBoolean(false));
        ssoData.put(Constants.FORCE_NAMEID_FORMAT, ssoDetails.path(Constants.FORCE_NAMEID_FORMAT).asBoolean(true));
        ssoData.put(Constants.CLIENT_SIGNATURE_REQUIRED, ssoDetails.path(Constants.CLIENT_SIGNATURE_REQUIRED).asBoolean(false));
        ssoData.put(Constants.NAMEID_FORMAT, ssoDetails.path(Constants.NAMEID_FORMAT).asText(Constants.USERNAME));
        ssoData.put(Constants.ROOT_URL, ssoDetails.path(Constants.ROOT_URL).asText(ssoDetails.path(Constants.ACS_URL).asText()));
        JsonNode redirectUrlNode = ssoDetails.path(Constants.VALID_REDIRECT_URL);
        ArrayNode redirectUrls = redirectUrlNode.isArray()
                ? (ArrayNode) redirectUrlNode
                : objectMapper.createArrayNode()
                .add(redirectUrlNode.asText(ssoDetails.path(Constants.ACS_URL).asText()));

        ssoData.set(Constants.VALID_REDIRECT_URL, redirectUrls);
    }


}
