package com.igot.cios.lti.controller;

import com.igot.cios.lti.config.LtiPlatformProperties;
import com.igot.cios.lti.service.LtiPlatformKeyService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/** Public LTI discovery endpoints. Tools fetch these anonymously. */
@RestController
@RequestMapping("/ciosIntegration/v1/lti/.well-known")
@RequiredArgsConstructor
public class LtiWellKnownController {

    private final LtiPlatformKeyService keyService;
    private final LtiPlatformProperties properties;

    /** Our public signing keys. Private members are stripped. */
    @GetMapping(value = "/jwks.json", produces = MediaType.APPLICATION_JSON_VALUE)
    public Map<String, Object> jwks() {
        return keyService.publicJwks();
    }

    /** OIDC discovery, so a tool can self-configure rather than hand-copying URLs. */
    @GetMapping(value = "/openid-configuration", produces = MediaType.APPLICATION_JSON_VALUE)
    public Map<String, Object> openidConfiguration() {
        return Map.of(
                "issuer", properties.getIssuer(),
                "authorization_endpoint", properties.authorizeUrl(),
                "token_endpoint", properties.tokenUrl(),
                "jwks_uri", properties.jwksUrl(),
                "response_types_supported", List.of("id_token"),
                "subject_types_supported", List.of("public"),
                "id_token_signing_alg_values_supported", List.of("RS256"),
                "scopes_supported", List.of("openid"),
                "claims_supported", List.of("iss", "aud", "sub", "exp", "iat", "nonce"),
                "token_endpoint_auth_methods_supported", List.of("private_key_jwt"));
    }
}
