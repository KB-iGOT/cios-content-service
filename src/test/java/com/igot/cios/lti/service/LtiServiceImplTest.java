package com.igot.cios.lti.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.igot.cios.dto.SBApiResponse;
import com.igot.cios.plugins.DataTransformUtility;
import com.igot.cios.util.Constants;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class LtiServiceImplTest {

    private static final String PARTNER_CODE = "IGOT-TESTINGLTI-OGDTD";
    private static final String TOOL_HOST = "https://8.8.8.8";
    private static final String ISSUER_HOST = "https://1.1.1.1";
    private static final String CLIENT_ID = "portal";
    private static final String DEPLOYMENT_ID = "1";
    private static final String ISSUER = ISSUER_HOST + "/auth/realms/sunbird";
    private static final String OIDC_LOGIN_INITIATION_URL = TOOL_HOST + "/enrol/lti/login.php?id=abc123";
    private static final String REDIRECT_LAUNCH_URL = TOOL_HOST + "/enrol/lti/launch.php";

    @Mock
    private DataTransformUtility dataTransformUtility;

    @Mock
    private RestTemplate restTemplate;

    private ObjectMapper mapper;
    private LtiServiceImpl service;

    @BeforeEach
    void setUp() {
        mapper = new ObjectMapper();
        service = spy(new LtiServiceImpl(dataTransformUtility, restTemplate));
    }

    // ---------------- request-level validation ----------------

    @Test
    void testOidcConnection_missingPartnerCode() {
        SBApiResponse response = service.testOidcConnection("", null, null);

        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(Constants.MISSING_PARTNER_CODE, response.getParams().getErrmsg());
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void testOidcConnection_partnerFetchThrows() {
        when(dataTransformUtility.fetchPartnerInfoUsingApi(PARTNER_CODE)).thenThrow(new RuntimeException("cb-pores unreachable"));

        SBApiResponse response = service.testOidcConnection(PARTNER_CODE, null, null);

        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertTrue(response.getParams().getErrmsg().contains(Constants.PARTNER_NOT_FOUND));
        assertEquals(HttpStatus.OK, response.getResponseCode());
        assertEquals(PARTNER_CODE, response.getResult().get(Constants.PARTNER_CODE));
    }

    // ---------------- Check 1: configuration validation ----------------

    @Test
    void testOidcConnection_protocolNotLti() {
        stubPartner(partnerData("SSO", validLtiConfig()));

        SBApiResponse response = service.testOidcConnection(PARTNER_CODE, null, null);

        assertFailedWith(response, Constants.PARTNER_PROTOCOL_NOT_LTI);
    }

    @Test
    void testOidcConnection_missingLtiConfiguration() {
        ObjectNode partnerData = mapper.createObjectNode();
        partnerData.put(Constants.PROTOCOL, Constants.LTI);
        stubPartner(partnerData);

        SBApiResponse response = service.testOidcConnection(PARTNER_CODE, null, null);

        assertFailedWith(response, Constants.LTI_CONFIGURATION_NOT_FOUND);
    }

    @Test
    void testOidcConnection_unsupportedLtiVersion() {
        ObjectNode ltiConfig = validLtiConfig();
        ltiConfig.put(Constants.LTI_VERSION, "1.1");
        stubPartner(partnerData(Constants.LTI, ltiConfig));

        SBApiResponse response = service.testOidcConnection(PARTNER_CODE, null, null);

        assertFailedWith(response, Constants.UNSUPPORTED_LTI_VERSION);
    }

    @Test
    void testOidcConnection_missingOidcLoginInitiationUrl() {
        ObjectNode ltiConfig = validLtiConfig();
        ltiConfig.remove(Constants.OIDC_LOGIN_INITIATION_URL);
        stubPartner(partnerData(Constants.LTI, ltiConfig));

        SBApiResponse response = service.testOidcConnection(PARTNER_CODE, null, null);

        assertFailedWith(response, Constants.MISSING_OIDC_LOGIN_INITIATION_URL);
    }

    @Test
    void testOidcConnection_invalidOidcLoginInitiationUrl_notHttps() {
        ObjectNode ltiConfig = validLtiConfig();
        ltiConfig.put(Constants.OIDC_LOGIN_INITIATION_URL, "http://moodle.example.com/login.php");
        stubPartner(partnerData(Constants.LTI, ltiConfig));

        SBApiResponse response = service.testOidcConnection(PARTNER_CODE, null, null);

        assertFailedWith(response, Constants.INVALID_OIDC_LOGIN_INITIATION_URL);
    }

    @Test
    void testOidcConnection_missingClientId() {
        ObjectNode ltiConfig = validLtiConfig();
        ltiConfig.remove(Constants.CLIENT_ID);
        stubPartner(partnerData(Constants.LTI, ltiConfig));

        SBApiResponse response = service.testOidcConnection(PARTNER_CODE, null, null);

        assertFailedWith(response, Constants.MISSING_CLIENT_ID);
    }

    @Test
    void testOidcConnection_missingDeploymentId() {
        ObjectNode ltiConfig = validLtiConfig();
        ltiConfig.remove(Constants.DEPLOYMENT_ID);
        stubPartner(partnerData(Constants.LTI, ltiConfig));

        SBApiResponse response = service.testOidcConnection(PARTNER_CODE, null, null);

        assertFailedWith(response, Constants.MISSING_DEPLOYMENT_ID);
    }

    @Test
    void testOidcConnection_missingRedirectLaunchUrl() {
        ObjectNode ltiConfig = validLtiConfig();
        ltiConfig.remove(Constants.REDIRECT_LAUNCH_URL);
        stubPartner(partnerData(Constants.LTI, ltiConfig));

        SBApiResponse response = service.testOidcConnection(PARTNER_CODE, null, null);

        assertFailedWith(response, Constants.MISSING_REDIRECT_LAUNCH_URL);
    }

    @Test
    void testOidcConnection_missingToolIssuer() {
        ObjectNode ltiConfig = validLtiConfig();
        ltiConfig.remove(Constants.TOOL_ISSUER);
        stubPartner(partnerData(Constants.LTI, ltiConfig));

        SBApiResponse response = service.testOidcConnection(PARTNER_CODE, null, null);

        assertFailedWith(response, Constants.MISSING_TOOL_ISSUER);
    }

    @Test
    void testOidcConnection_invalidToolIssuer_notHttps() {
        ObjectNode ltiConfig = validLtiConfig();
        ltiConfig.put(Constants.TOOL_ISSUER, "ftp://sunbird.example.com");
        stubPartner(partnerData(Constants.LTI, ltiConfig));

        SBApiResponse response = service.testOidcConnection(PARTNER_CODE, null, null);

        assertFailedWith(response, Constants.INVALID_TOOL_ISSUER);
    }

    @Test
    void testOidcConnection_configFailure_skipsReachabilityAndDiscoveryChecks() {
        ObjectNode ltiConfig = validLtiConfig();
        ltiConfig.remove(Constants.CLIENT_ID);
        stubPartner(partnerData(Constants.LTI, ltiConfig));

        service.testOidcConnection(PARTNER_CODE, null, null);

        verify(restTemplate, never()).getForObject(anyString(), eq(com.fasterxml.jackson.databind.JsonNode.class));
    }

    // ---------------- Check 2: tool reachability ----------------

    @Test
    void testOidcConnection_disallowedHost_loopback() throws IOException {
        ObjectNode ltiConfig = validLtiConfig();
        ltiConfig.put(Constants.OIDC_LOGIN_INITIATION_URL, "https://127.0.0.1/enrol/lti/login.php?id=abc123");
        stubPartner(partnerData(Constants.LTI, ltiConfig));
        stubPassingDiscovery();

        SBApiResponse response = service.testOidcConnection(PARTNER_CODE, null, null);

        assertFailedWith(response, Constants.DISALLOWED_HOST);
        verify(service, never()).openConnection(anyString());
    }

    @Test
    void testOidcConnection_dnsResolutionFailure() {
        ObjectNode ltiConfig = validLtiConfig();
        ltiConfig.put(Constants.OIDC_LOGIN_INITIATION_URL, "https://this-host-does-not-exist.invalid/login.php?id=abc123");
        stubPartner(partnerData(Constants.LTI, ltiConfig));
        stubPassingDiscovery();

        SBApiResponse response = service.testOidcConnection(PARTNER_CODE, null, null);

        assertFailedWith(response, Constants.DNS_RESOLUTION_FAILED);
    }

    @Test
    void testOidcConnection_toolRejectsRequest_404() throws IOException {
        stubPartner(partnerData(Constants.LTI, validLtiConfig()));
        stubPassingDiscovery();
        doReturn(mockConnection(404, null, null)).when(service).openConnection(anyString());

        SBApiResponse response = service.testOidcConnection(PARTNER_CODE, null, null);

        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(Constants.TOOL_REJECTED_REQUEST, response.getParams().getErrmsg());
    }

    @Test
    void testOidcConnection_unexpectedOkInsteadOfRedirect() throws IOException {
        stubPartner(partnerData(Constants.LTI, validLtiConfig()));
        stubPassingDiscovery();
        doReturn(mockConnection(200, null, null)).when(service).openConnection(anyString());

        SBApiResponse response = service.testOidcConnection(PARTNER_CODE, null, null);

        assertFailedWith(response, Constants.UNEXPECTED_RESPONSE_PREFIX);
        assertTrue(response.getParams().getErrmsg().contains("200"));
    }

    @Test
    void testOidcConnection_maxRedirectsExceeded() throws IOException {
        stubPartner(partnerData(Constants.LTI, validLtiConfig()));
        stubPassingDiscovery();
        // Every hop redirects to a host/client_id that never matches the configured issuer.
        HttpURLConnection neverMatchingHop = mockConnection(303, TOOL_HOST + "/enrol/lti/login.php?cookiecheck1=1&client_id=someoneelse", null);
        doReturn(neverMatchingHop).when(service).openConnection(anyString());

        SBApiResponse response = service.testOidcConnection(PARTNER_CODE, null, null);

        assertFailedWith(response, Constants.MAX_REDIRECTS_EXCEEDED);
    }

    @Test
    void testOidcConnection_cookiesForwardedAcrossHops() throws IOException {
        stubPartner(partnerData(Constants.LTI, validLtiConfig()));
        stubPassingDiscovery();

        HttpURLConnection hop0 = mockConnection(
                303,
                TOOL_HOST + "/enrol/lti/login.php?cookiecheck1=1&cookiecheck2=2",
                Map.of("Set-Cookie", List.of(
                        "MoodleSession=abc123; path=/; secure; HttpOnly",
                        "cookiecheck1=1; secure",
                        "cookiecheck2=2; secure"
                ))
        );
        HttpURLConnection hop1 = mockConnection(
                303,
                ISSUER_HOST + "/auth/realms/sunbird/protocol/openid-connect/auth?client_id=" + CLIENT_ID,
                null
        );
        doReturn(hop0).doReturn(hop1).when(service).openConnection(anyString());

        SBApiResponse response = service.testOidcConnection(PARTNER_CODE, null, null);

        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
        verify(hop1).setRequestProperty(eq("Cookie"), argThatContains("MoodleSession=abc123"));
    }

    @Test
    void testOidcConnection_toolRedirectsCorrectly_passes() throws IOException {
        stubPartner(partnerData(Constants.LTI, validLtiConfig()));
        stubPassingDiscovery();
        HttpURLConnection hop0 = mockConnection(303, TOOL_HOST + "/enrol/lti/login.php?cookiecheck1=1&cookiecheck2=2", null);
        HttpURLConnection hop1 = mockConnection(303, ISSUER_HOST + "/auth/realms/sunbird/protocol/openid-connect/auth?client_id=" + CLIENT_ID, null);
        doReturn(hop0).doReturn(hop1).when(service).openConnection(anyString());

        SBApiResponse response = service.testOidcConnection(PARTNER_CODE, null, null);

        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
        assertEquals(Constants.ALL_CHECKS_PASSED, response.getParams().getErrmsg());
        assertEquals(HttpStatus.OK, response.getResponseCode());
    }

    // ---------------- Check 3: platform OIDC discovery ----------------

    @Test
    void testOidcConnection_discoveryUnreachable() throws IOException {
        stubPartner(partnerData(Constants.LTI, validLtiConfig()));
        stubPassingReachability();
        when(restTemplate.getForObject(ISSUER + Constants.WELL_KNOWN_OPENID_CONFIGURATION_PATH, com.fasterxml.jackson.databind.JsonNode.class))
                .thenThrow(new RuntimeException("connection refused"));

        SBApiResponse response = service.testOidcConnection(PARTNER_CODE, null, null);

        assertFailedWith(response, Constants.OIDC_DISCOVERY_UNREACHABLE);
    }

    @Test
    void testOidcConnection_discoveryInvalidResponse() throws IOException {
        stubPartner(partnerData(Constants.LTI, validLtiConfig()));
        stubPassingReachability();
        when(restTemplate.getForObject(ISSUER + Constants.WELL_KNOWN_OPENID_CONFIGURATION_PATH, com.fasterxml.jackson.databind.JsonNode.class))
                .thenReturn(null);

        SBApiResponse response = service.testOidcConnection(PARTNER_CODE, null, null);

        assertFailedWith(response, Constants.OIDC_DISCOVERY_INVALID_RESPONSE);
    }

    @Test
    void testOidcConnection_discoveryMissingIssuerField() throws IOException {
        stubPartner(partnerData(Constants.LTI, validLtiConfig()));
        stubPassingReachability();
        ObjectNode discoveryDoc = mapper.createObjectNode();
        discoveryDoc.put(Constants.AUTHORIZATION_ENDPOINT, ISSUER + "/protocol/openid-connect/auth");
        when(restTemplate.getForObject(ISSUER + Constants.WELL_KNOWN_OPENID_CONFIGURATION_PATH, com.fasterxml.jackson.databind.JsonNode.class))
                .thenReturn(discoveryDoc);

        SBApiResponse response = service.testOidcConnection(PARTNER_CODE, null, null);

        assertFailedWith(response, Constants.OIDC_DISCOVERY_MISSING_ISSUER);
    }

    @Test
    void testOidcConnection_discoveryIssuerMismatch() throws IOException {
        stubPartner(partnerData(Constants.LTI, validLtiConfig()));
        stubPassingReachability();
        ObjectNode discoveryDoc = discoveryDocument("https://wrong-issuer.example.com/auth/realms/other");
        when(restTemplate.getForObject(ISSUER + Constants.WELL_KNOWN_OPENID_CONFIGURATION_PATH, com.fasterxml.jackson.databind.JsonNode.class))
                .thenReturn(discoveryDoc);

        SBApiResponse response = service.testOidcConnection(PARTNER_CODE, null, null);

        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertTrue(response.getParams().getErrmsg().contains(ISSUER));
        assertTrue(response.getParams().getErrmsg().contains("https://wrong-issuer.example.com/auth/realms/other"));
    }

    @Test
    void testOidcConnection_discoveryMissingEndpoints() throws IOException {
        stubPartner(partnerData(Constants.LTI, validLtiConfig()));
        stubPassingReachability();
        ObjectNode discoveryDoc = mapper.createObjectNode();
        discoveryDoc.put(Constants.OIDC_ISSUER_FIELD, ISSUER);
        when(restTemplate.getForObject(ISSUER + Constants.WELL_KNOWN_OPENID_CONFIGURATION_PATH, com.fasterxml.jackson.databind.JsonNode.class))
                .thenReturn(discoveryDoc);

        SBApiResponse response = service.testOidcConnection(PARTNER_CODE, null, null);

        assertFailedWith(response, Constants.OIDC_DISCOVERY_MISSING_ENDPOINTS);
    }

    @Test
    void testOidcConnection_jwksUnreachable() throws IOException {
        stubPartner(partnerData(Constants.LTI, validLtiConfig()));
        stubPassingReachability();
        String jwksUri = ISSUER + "/protocol/openid-connect/certs";
        when(restTemplate.getForObject(ISSUER + Constants.WELL_KNOWN_OPENID_CONFIGURATION_PATH, com.fasterxml.jackson.databind.JsonNode.class))
                .thenReturn(discoveryDocument(ISSUER, jwksUri));
        when(restTemplate.getForObject(jwksUri, com.fasterxml.jackson.databind.JsonNode.class))
                .thenThrow(new RuntimeException("timeout"));

        SBApiResponse response = service.testOidcConnection(PARTNER_CODE, null, null);

        assertFailedWith(response, Constants.OIDC_JWKS_UNREACHABLE);
    }

    @Test
    void testOidcConnection_jwksEmpty() throws IOException {
        stubPartner(partnerData(Constants.LTI, validLtiConfig()));
        stubPassingReachability();
        String jwksUri = ISSUER + "/protocol/openid-connect/certs";
        when(restTemplate.getForObject(ISSUER + Constants.WELL_KNOWN_OPENID_CONFIGURATION_PATH, com.fasterxml.jackson.databind.JsonNode.class))
                .thenReturn(discoveryDocument(ISSUER, jwksUri));
        ObjectNode emptyJwks = mapper.createObjectNode();
        emptyJwks.set(Constants.KEYS, mapper.createArrayNode());
        when(restTemplate.getForObject(jwksUri, com.fasterxml.jackson.databind.JsonNode.class))
                .thenReturn(emptyJwks);

        SBApiResponse response = service.testOidcConnection(PARTNER_CODE, null, null);

        assertFailedWith(response, Constants.OIDC_JWKS_EMPTY);
    }

    @Test
    void testOidcConnection_jwksMalformedKey() throws IOException {
        stubPartner(partnerData(Constants.LTI, validLtiConfig()));
        stubPassingReachability();
        String jwksUri = ISSUER + "/protocol/openid-connect/certs";
        when(restTemplate.getForObject(ISSUER + Constants.WELL_KNOWN_OPENID_CONFIGURATION_PATH, com.fasterxml.jackson.databind.JsonNode.class))
                .thenReturn(discoveryDocument(ISSUER, jwksUri));
        ObjectNode malformedKey = mapper.createObjectNode();
        malformedKey.put(Constants.KTY, "RSA");
        // kid intentionally missing
        ArrayNode keys = mapper.createArrayNode();
        keys.add(malformedKey);
        ObjectNode jwks = mapper.createObjectNode();
        jwks.set(Constants.KEYS, keys);
        when(restTemplate.getForObject(jwksUri, com.fasterxml.jackson.databind.JsonNode.class))
                .thenReturn(jwks);

        SBApiResponse response = service.testOidcConnection(PARTNER_CODE, null, null);

        assertFailedWith(response, Constants.OIDC_JWKS_MALFORMED_KEY);
    }

    @Test
    void testOidcConnection_bothReachabilityAndDiscoveryFail_messagesJoined() throws IOException {
        stubPartner(partnerData(Constants.LTI, validLtiConfig()));
        doReturn(mockConnection(404, null, null)).when(service).openConnection(anyString());
        when(restTemplate.getForObject(ISSUER + Constants.WELL_KNOWN_OPENID_CONFIGURATION_PATH, com.fasterxml.jackson.databind.JsonNode.class))
                .thenThrow(new RuntimeException("unreachable"));

        SBApiResponse response = service.testOidcConnection(PARTNER_CODE, null, null);

        String errmsg = response.getParams().getErrmsg();
        assertTrue(errmsg.contains(Constants.TOOL_REJECTED_REQUEST));
        assertTrue(errmsg.contains(Constants.OIDC_DISCOVERY_UNREACHABLE));
        assertTrue(errmsg.contains("; "));
    }

    // ---------------- helpers ----------------

    private void stubPartner(ObjectNode partnerData) {
        ObjectNode partnerResult = mapper.createObjectNode();
        partnerResult.set(Constants.DATA, partnerData);
        when(dataTransformUtility.fetchPartnerInfoUsingApi(PARTNER_CODE)).thenReturn(partnerResult);
    }

    private ObjectNode partnerData(String protocol, ObjectNode ltiConfig) {
        ObjectNode partnerData = mapper.createObjectNode();
        partnerData.put(Constants.PROTOCOL, protocol);
        partnerData.set(Constants.LTI_CONFIGURATION, ltiConfig);
        return partnerData;
    }

    private ObjectNode validLtiConfig() {
        ObjectNode ltiConfig = mapper.createObjectNode();
        ltiConfig.put(Constants.OIDC_LOGIN_INITIATION_URL, OIDC_LOGIN_INITIATION_URL);
        ltiConfig.put(Constants.REDIRECT_LAUNCH_URL, REDIRECT_LAUNCH_URL);
        ltiConfig.put(Constants.CLIENT_ID, CLIENT_ID);
        ltiConfig.put(Constants.DEPLOYMENT_ID, DEPLOYMENT_ID);
        ltiConfig.put(Constants.TOOL_ISSUER, ISSUER);
        ltiConfig.put(Constants.LTI_VERSION, Constants.LTI_VERSION_1_3);
        return ltiConfig;
    }

    private ObjectNode discoveryDocument(String issuer) {
        return discoveryDocument(issuer, ISSUER + "/protocol/openid-connect/certs");
    }

    private ObjectNode discoveryDocument(String issuer, String jwksUri) {
        ObjectNode node = mapper.createObjectNode();
        node.put(Constants.OIDC_ISSUER_FIELD, issuer);
        node.put(Constants.AUTHORIZATION_ENDPOINT, ISSUER + "/protocol/openid-connect/auth");
        node.put(Constants.TOKEN_ENDPOINT, ISSUER + "/protocol/openid-connect/token");
        node.put(Constants.JWKS_URI, jwksUri);
        return node;
    }

    private ObjectNode validJwks() {
        ObjectNode key = mapper.createObjectNode();
        key.put(Constants.KTY, "RSA");
        key.put(Constants.KID, "key-1");
        ArrayNode keys = mapper.createArrayNode();
        keys.add(key);
        ObjectNode jwks = mapper.createObjectNode();
        jwks.set(Constants.KEYS, keys);
        return jwks;
    }

    private void stubPassingDiscovery() {
        String jwksUri = ISSUER + "/protocol/openid-connect/certs";
        when(restTemplate.getForObject(ISSUER + Constants.WELL_KNOWN_OPENID_CONFIGURATION_PATH, com.fasterxml.jackson.databind.JsonNode.class))
                .thenReturn(discoveryDocument(ISSUER, jwksUri));
        when(restTemplate.getForObject(jwksUri, com.fasterxml.jackson.databind.JsonNode.class))
                .thenReturn(validJwks());
    }

    private void stubPassingReachability() throws IOException {
        HttpURLConnection hop0 = mockConnection(303, TOOL_HOST + "/enrol/lti/login.php?cookiecheck1=1&cookiecheck2=2", null);
        HttpURLConnection hop1 = mockConnection(303, ISSUER_HOST + "/auth/realms/sunbird/protocol/openid-connect/auth?client_id=" + CLIENT_ID, null);
        doReturn(hop0).doReturn(hop1).when(service).openConnection(anyString());
    }

    private HttpURLConnection mockConnection(int status, String location, Map<String, List<String>> headerFields) throws IOException {
        HttpURLConnection connection = mock(HttpURLConnection.class);
        when(connection.getResponseCode()).thenReturn(status);
        when(connection.getHeaderField("Location")).thenReturn(location);
        when(connection.getHeaderFields()).thenReturn(headerFields != null ? headerFields : Collections.emptyMap());
        return connection;
    }

    private void assertFailedWith(SBApiResponse response, String expectedMessageFragment) {
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertTrue(response.getParams().getErrmsg().contains(expectedMessageFragment),
                "Expected errmsg to contain [" + expectedMessageFragment + "] but was [" + response.getParams().getErrmsg() + "]");
        assertEquals(HttpStatus.OK, response.getResponseCode());
    }

    private String argThatContains(String fragment) {
        return org.mockito.ArgumentMatchers.argThat(value -> value != null && value.contains(fragment));
    }
}
