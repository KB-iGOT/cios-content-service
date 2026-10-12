package com.igot.cios.lti.controller;

import com.igot.cios.lti.service.LtiAccessTokenService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * OAuth2 token endpoint for LTI service calls. Not the learner's launch auth;
 * this is Moodle authenticating itself, server to server, before calling AGS.
 */
@RestController
@RequiredArgsConstructor
public class LtiTokenController {

    private static final String JWT_BEARER =
            "urn:ietf:params:oauth:client-assertion-type:jwt-bearer";

    private final LtiAccessTokenService tokenService;

    @PostMapping(value = "/ciosIntegration/v1/lti/oauth2/token",
            consumes = MediaType.APPLICATION_FORM_URLENCODED_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Map<String, Object>> token(
            @RequestParam("grant_type") String grantType,
            @RequestParam(value = "client_assertion_type", required = false) String assertionType,
            @RequestParam(value = "client_assertion", required = false) String clientAssertion,
            @RequestParam(value = "scope", required = false) String scope) {

        if (!"client_credentials".equals(grantType)) {
            return error(HttpStatus.BAD_REQUEST, "unsupported_grant_type",
                    "Only client_credentials is supported");
        }
        if (!JWT_BEARER.equals(assertionType)) {
            return error(HttpStatus.BAD_REQUEST, "invalid_request",
                    "client_assertion_type must be " + JWT_BEARER);
        }
        if (clientAssertion == null || clientAssertion.isBlank()) {
            return error(HttpStatus.BAD_REQUEST, "invalid_request", "client_assertion is required");
        }

        try {
            return ResponseEntity.ok(tokenService.issue(clientAssertion, scope));
        } catch (LtiAccessTokenService.TokenException e) {
            return error(HttpStatus.UNAUTHORIZED, "invalid_client", e.getMessage());
        }
    }

    private ResponseEntity<Map<String, Object>> error(HttpStatus status, String code, String description) {
        return ResponseEntity.status(status).body(Map.of("error", code, "error_description", description));
    }
}
