package com.igot.cios.lti.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.igot.cios.dto.SBApiResponse;
import com.igot.cios.plugins.DataTransformUtility;
import com.igot.cios.util.Constants;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import javax.net.ssl.SSLException;
import java.io.IOException;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@Service
public class LtiServiceImpl implements LtiService {

    private static final int MAX_REDIRECT_HOPS = 3;
    private static final int CONNECTION_TIMEOUT_MS = 5000;

    private final DataTransformUtility dataTransformUtility;
    private final RestTemplate restTemplate;

    @Autowired
    public LtiServiceImpl(DataTransformUtility dataTransformUtility, RestTemplate restTemplate) {
        this.dataTransformUtility = dataTransformUtility;
        this.restTemplate = restTemplate;
    }

    @Override
    public SBApiResponse testOidcConnection(String partnerCode, String loginHint, String messageHint) {
        SBApiResponse response = SBApiResponse.createDefaultResponse(Constants.API_LTI_OIDC_TEST);
        if (StringUtils.isBlank(partnerCode)) {
            return failedResponse(response, Constants.MISSING_PARTNER_CODE, HttpStatus.BAD_REQUEST);
        }

        List<String> failureReasons = new ArrayList<>();
        Map<String, Object> result = new HashMap<>();
        result.put(Constants.PARTNER_CODE, partnerCode);

        JsonNode ltiConfig = null;
        String configFailure;
        try {
            JsonNode partnerResult = dataTransformUtility.fetchPartnerInfoUsingApi(partnerCode);
            JsonNode partnerData = partnerResult.path(Constants.DATA);
            ltiConfig = partnerData.path(Constants.LTI_CONFIGURATION);
            configFailure = validateConfiguration(partnerData, ltiConfig, partnerCode);
        } catch (Exception e) {
            log.error("LtiServiceImpl::testOidcConnection: failed to fetch partner details for {}", partnerCode, e);
            configFailure = Constants.PARTNER_NOT_FOUND + partnerCode;
        }

        if (configFailure == null) {
            String oidcLoginInitiationUrl = ltiConfig.path(Constants.OIDC_LOGIN_INITIATION_URL).asText("");
            String targetLinkUri = ltiConfig.path(Constants.REDIRECT_LAUNCH_URL).asText("");
            String clientId = ltiConfig.path(Constants.CLIENT_ID).asText("");
            String deploymentId = ltiConfig.path(Constants.DEPLOYMENT_ID).asText("");
            String toolIssuer = ltiConfig.path(Constants.TOOL_ISSUER).asText("");

            String resolvedLoginHint = StringUtils.defaultIfBlank(loginHint, Constants.DEFAULT_LTI_LOGIN_HINT);

            String oidcLoginUrl = UriComponentsBuilder.fromUriString(oidcLoginInitiationUrl)
                    .queryParam(Constants.ISS, toolIssuer)
                    .queryParam(Constants.LOGIN_HINT, resolvedLoginHint)
                    .queryParam(Constants.TARGET_LINK_URI, targetLinkUri)
                    .queryParam(Constants.CLIENT_ID_QUERY_PARAM, clientId)
                    .queryParam(Constants.LTI_DEPLOYMENT_ID, deploymentId)
                    .queryParam(Constants.LTI_MESSAGE_HINT, StringUtils.defaultIfBlank(messageHint, Constants.DEFAULT_LTI_MESSAGE_HINT))
                    .build()
                    .encode()
                    .toUriString();

            String reachabilityFailure = checkToolReachability(oidcLoginUrl, clientId, toolIssuer, partnerCode);
            if (reachabilityFailure != null) {
                failureReasons.add(reachabilityFailure);
            }

            String discoveryFailure = checkPlatformOidcDiscovery(toolIssuer);
            if (discoveryFailure != null) {
                failureReasons.add(discoveryFailure);
            }
        } else {
            failureReasons.add(configFailure);
        }

        response.setResult(result);
        if (failureReasons.isEmpty()) {
            response.getParams().setStatus(Constants.SUCCESS);
            response.getParams().setErrmsg(Constants.ALL_CHECKS_PASSED);
        } else {
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErrmsg(String.join("; ", failureReasons));
        }
        response.setResponseCode(HttpStatus.OK);
        return response;
    }

    /**
     * Check 1 - validates the partner's LTI configuration: protocol/version,
     * presence and HTTPS-URL-validity of the fields needed to build an OIDC
     * login-initiation request. Pure data validation, no network calls.
     * Returns null when valid, otherwise the failure reason.
     */
    private String validateConfiguration(JsonNode partnerData, JsonNode ltiConfig, String partnerCode) {
        if (!Constants.LTI.equalsIgnoreCase(partnerData.path(Constants.PROTOCOL).asText(""))) {
            return Constants.PARTNER_PROTOCOL_NOT_LTI + partnerCode;
        }
        if (ltiConfig.isMissingNode() || ltiConfig.isNull()) {
            return Constants.LTI_CONFIGURATION_NOT_FOUND + partnerCode;
        }
        if (!Constants.LTI_VERSION_1_3.equals(ltiConfig.path(Constants.LTI_VERSION).asText(""))) {
            return Constants.UNSUPPORTED_LTI_VERSION + partnerCode;
        }

        String oidcLoginInitiationUrl = ltiConfig.path(Constants.OIDC_LOGIN_INITIATION_URL).asText("");
        if (StringUtils.isBlank(oidcLoginInitiationUrl)) {
            return Constants.MISSING_OIDC_LOGIN_INITIATION_URL + partnerCode;
        }
        if (!isValidHttpsUrl(oidcLoginInitiationUrl)) {
            return Constants.INVALID_OIDC_LOGIN_INITIATION_URL + partnerCode;
        }

        if (StringUtils.isBlank(ltiConfig.path(Constants.CLIENT_ID).asText(""))) {
            return Constants.MISSING_CLIENT_ID + partnerCode;
        }
        if (StringUtils.isBlank(ltiConfig.path(Constants.DEPLOYMENT_ID).asText(""))) {
            return Constants.MISSING_DEPLOYMENT_ID + partnerCode;
        }

        String targetLinkUri = ltiConfig.path(Constants.REDIRECT_LAUNCH_URL).asText("");
        if (StringUtils.isBlank(targetLinkUri) || !isValidHttpsUrl(targetLinkUri)) {
            return Constants.MISSING_REDIRECT_LAUNCH_URL + partnerCode;
        }

        String toolIssuer = ltiConfig.path(Constants.TOOL_ISSUER).asText("");
        if (StringUtils.isBlank(toolIssuer)) {
            return Constants.MISSING_TOOL_ISSUER + partnerCode;
        }
        if (!isValidHttpsUrl(toolIssuer)) {
            return Constants.INVALID_TOOL_ISSUER + partnerCode;
        }

        return null;
    }

    private boolean isValidHttpsUrl(String url) {
        try {
            URI uri = URI.create(url);
            return Constants.HTTPS_SCHEME.equalsIgnoreCase(uri.getScheme()) && StringUtils.isNotBlank(uri.getHost());
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    /**
     * Check 2 - walks the tool's OIDC login-initiation redirect chain WITHOUT following
     * redirects automatically, and WITHOUT ever hitting the platform's own auth endpoint
     * (that leg depends on the caller's browser session and can't be verified from the
     * backend). Distinguishes DNS/TLS/timeout/connection failures from the tool being
     * reachable but responding unexpectedly (4xx/5xx, or a 3xx to the wrong place).
     * Returns null when the tool redirects correctly, otherwise the failure reason.
     */
    private String checkToolReachability(String oidcLoginUrl, String clientId, String issuer, String partnerCode) {
        String issuerHost = URI.create(issuer).getHost();

        Map<String, String> cookieJar = new HashMap<>();
        String currentUrl = oidcLoginUrl;
        for (int hop = 0; hop < MAX_REDIRECT_HOPS; hop++) {
            HttpURLConnection connection = null;
            try {
                URL url = new URL(currentUrl);
                InetAddress address = resolveAddress(url.getHost());
                if (isDisallowedAddress(address)) {
                    log.error("LtiServiceImpl::checkToolReachability: refusing to connect to disallowed host {} for partner {}", url.getHost(), partnerCode);
                    return Constants.DISALLOWED_HOST + url.getHost();
                }

                connection = openConnection(currentUrl);
                if (!cookieJar.isEmpty()) {
                    connection.setRequestProperty("Cookie", buildCookieHeader(cookieJar));
                }

                int status = connection.getResponseCode();
                captureCookies(connection, cookieJar);
                String location = connection.getHeaderField("Location");

                if (status >= 400) {
                    return Constants.TOOL_REJECTED_REQUEST;
                }
                if (status < 300 || StringUtils.isBlank(location)) {
                    return Constants.UNEXPECTED_RESPONSE_PREFIX + status + Constants.UNEXPECTED_RESPONSE_GUIDANCE + partnerCode;
                }

                String locationHost = URI.create(location).getHost();
                String locationClientId = UriComponentsBuilder.fromUriString(location).build().getQueryParams().getFirst(Constants.CLIENT_ID_QUERY_PARAM);
                if (issuerHost.equalsIgnoreCase(locationHost) && clientId.equals(locationClientId)) {
                    return null;
                }
                currentUrl = location;
            } catch (UnknownHostException e) {
                return Constants.DNS_RESOLUTION_FAILED + partnerCode;
            } catch (SSLException e) {
                return Constants.TLS_HANDSHAKE_FAILED + partnerCode;
            } catch (SocketTimeoutException e) {
                return Constants.CONNECTION_TIMED_OUT + partnerCode;
            } catch (ConnectException e) {
                return Constants.CONNECTION_REFUSED + partnerCode;
            } catch (IOException | IllegalArgumentException e) {
                log.error("LtiServiceImpl::checkToolReachability: failed to reach OIDC login endpoint for partner {}", partnerCode, e);
                return Constants.OIDC_CONNECTION_TEST_FAILED + partnerCode;
            } finally {
                if (connection != null) {
                    connection.disconnect();
                }
            }
        }
        return Constants.MAX_REDIRECTS_EXCEEDED + partnerCode;
    }

    /**
     * Moodle's login-initiation cookie-check hop requires the session/cookiecheck
     * cookies set on the first redirect to be sent back on the second request -
     * without them it silently falls through to a plain 200 instead of continuing
     * the redirect chain. HttpURLConnection doesn't carry cookies across separate
     * connections, so this is tracked manually for the lifetime of one check.
     */
    private void captureCookies(HttpURLConnection connection, Map<String, String> cookieJar) {
        for (Map.Entry<String, List<String>> header : connection.getHeaderFields().entrySet()) {
            if (header.getKey() != null && header.getKey().equalsIgnoreCase("Set-Cookie")) {
                for (String setCookie : header.getValue()) {
                    String nameValue = setCookie.split(";", 2)[0].trim();
                    int separatorIndex = nameValue.indexOf('=');
                    if (separatorIndex > 0) {
                        cookieJar.put(nameValue.substring(0, separatorIndex), nameValue.substring(separatorIndex + 1));
                    }
                }
            }
        }
    }

    private String buildCookieHeader(Map<String, String> cookieJar) {
        return cookieJar.entrySet().stream()
                .map(entry -> entry.getKey() + "=" + entry.getValue())
                .collect(Collectors.joining("; "));
    }

    private boolean isDisallowedAddress(InetAddress address) {
        return address.isLoopbackAddress() || address.isLinkLocalAddress()
                || address.isSiteLocalAddress() || address.isAnyLocalAddress()
                || address.isMulticastAddress();
    }

    /**
     * Check 3 - fetches the platform's (Keycloak) OIDC discovery document and JWKS using
     * plain HTTPS requests (no admin credentials needed). Confirms the discovery document
     * is internally consistent and the JWKS is well-formed. Does NOT prove the custom LTI
     * claims Moodle expects are actually produced - that requires a real launch.
     * Returns null when valid, otherwise the failure reason.
     */
    private String checkPlatformOidcDiscovery(String issuer) {
        String discoveryUrl = issuer + Constants.WELL_KNOWN_OPENID_CONFIGURATION_PATH;
        JsonNode discoveryDoc;
        try {
            discoveryDoc = restTemplate.getForObject(discoveryUrl, JsonNode.class);
        } catch (Exception e) {
            log.error("LtiServiceImpl::checkPlatformOidcDiscovery: failed to fetch discovery document from {} (toolIssuer={})", discoveryUrl, issuer, e);
            return Constants.OIDC_DISCOVERY_UNREACHABLE;
        }
        if (discoveryDoc == null || discoveryDoc.isMissingNode() || discoveryDoc.isNull()) {
            log.error("LtiServiceImpl::checkPlatformOidcDiscovery: empty/invalid response from {} (toolIssuer={})", discoveryUrl, issuer);
            return Constants.OIDC_DISCOVERY_INVALID_RESPONSE;
        }

        String discoveredIssuer = discoveryDoc.path(Constants.OIDC_ISSUER_FIELD).asText("");
        if (StringUtils.isBlank(discoveredIssuer)) {
            log.error("LtiServiceImpl::checkPlatformOidcDiscovery: no issuer field in response from {} (toolIssuer={}): {}", discoveryUrl, issuer, discoveryDoc);
            return Constants.OIDC_DISCOVERY_MISSING_ISSUER;
        }
        if (!issuer.equals(discoveredIssuer)) {
            log.error("LtiServiceImpl::checkPlatformOidcDiscovery: issuer mismatch, toolIssuer={} discoveredIssuer={}", issuer, discoveredIssuer);
            return Constants.OIDC_DISCOVERY_ISSUER_MISMATCH_EXPECTED + issuer
                    + Constants.OIDC_DISCOVERY_ISSUER_MISMATCH_FOUND + discoveredIssuer;
        }

        String authorizationEndpoint = discoveryDoc.path(Constants.AUTHORIZATION_ENDPOINT).asText("");
        String tokenEndpoint = discoveryDoc.path(Constants.TOKEN_ENDPOINT).asText("");
        String jwksUri = discoveryDoc.path(Constants.JWKS_URI).asText("");
        if (StringUtils.isBlank(authorizationEndpoint) || StringUtils.isBlank(tokenEndpoint) || StringUtils.isBlank(jwksUri)) {
            return Constants.OIDC_DISCOVERY_MISSING_ENDPOINTS;
        }

        JsonNode jwks;
        try {
            jwks = restTemplate.getForObject(jwksUri, JsonNode.class);
        } catch (Exception e) {
            log.error("LtiServiceImpl::checkPlatformOidcDiscovery: failed to fetch JWKS from {}", jwksUri, e);
            return Constants.OIDC_JWKS_UNREACHABLE;
        }
        JsonNode keys = jwks != null ? jwks.path(Constants.KEYS) : null;
        if (keys == null || !keys.isArray() || keys.isEmpty()) {
            return Constants.OIDC_JWKS_EMPTY;
        }
        for (JsonNode key : keys) {
            if (StringUtils.isBlank(key.path(Constants.KTY).asText("")) || StringUtils.isBlank(key.path(Constants.KID).asText(""))) {
                return Constants.OIDC_JWKS_MALFORMED_KEY;
            }
        }
        return null;
    }

    private SBApiResponse failedResponse(SBApiResponse response, String errorMessage, HttpStatus httpStatus) {
        response.getParams().setStatus(Constants.FAILED);
        response.getParams().setErrmsg(errorMessage);
        response.setResponseCode(httpStatus);
        return response;
    }

    InetAddress resolveAddress(String host) throws UnknownHostException {
        return InetAddress.getByName(host);
    }

    HttpURLConnection openConnection(String urlString) throws IOException {
        HttpURLConnection connection = (HttpURLConnection) new URL(urlString).openConnection();
        connection.setInstanceFollowRedirects(false);
        connection.setRequestMethod("GET");
        connection.setConnectTimeout(CONNECTION_TIMEOUT_MS);
        connection.setReadTimeout(CONNECTION_TIMEOUT_MS);
        return connection;
    }
}
