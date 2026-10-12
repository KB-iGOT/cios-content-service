package com.igot.cios.lti.controller;

import com.igot.cios.lti.LtiClaims;
import com.igot.cios.lti.config.LtiPlatformProperties;
import com.igot.cios.lti.entity.LtiProviderRegistrationEntity;
import com.igot.cios.lti.entity.LtiResourceLinkEntity;
import com.igot.cios.lti.repository.LtiProviderRegistrationRepository;
import com.igot.cios.lti.repository.LtiResourceLinkRepository;
import com.igot.cios.lti.service.LtiIdTokenService;
import com.igot.cios.lti.service.LtiLaunchStateService;
import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.util.UriComponentsBuilder;

import java.io.IOException;
import java.util.UUID;

/**
 * Legs 1 and 3 of the OIDC launch handshake.
 *
 * <p>Hand-rolled rather than delegated to Spring Authorization Server, which
 * targets OAuth 2.1 and does not support the implicit response_type=id_token
 * flow LTI requires.
 *
 * <p><b>Pending:</b> the authorize endpoint must re-validate the learner's
 * Keycloak session before signing. This service has no request-level auth
 * today; userId arrives as a parameter. Wire that in before any shared
 * environment.
 */
@Slf4j
@RestController
@RequestMapping("/ciosIntegration/v1/lti")
@RequiredArgsConstructor
public class LtiLaunchController {

    private final LtiProviderRegistrationRepository registrationRepository;
    private final LtiResourceLinkRepository resourceLinkRepository;
    private final LtiLaunchStateService launchState;
    private final LtiIdTokenService idTokenService;
    private final LtiPlatformProperties properties;

    /**
     * LEG 1: the learner clicked a course. Not part of the spec: our own
     * trigger. Builds the OIDC login initiation request and redirects the
     * browser to Moodle.
     */
    @GetMapping("/launch/{resourceLinkId}")
    public void launch(@PathVariable String resourceLinkId,
                       @RequestParam String userId,
                       HttpServletResponse response) throws IOException {

        LtiResourceLinkEntity link = resourceLinkRepository.findById(resourceLinkId)
                .orElseThrow(() -> new IllegalArgumentException("Unknown resource link: " + resourceLinkId));
        LtiProviderRegistrationEntity reg = registrationRepository.findById(link.getRegistrationId())
                .orElseThrow(() -> new IllegalArgumentException("Unknown registration"));

        String messageHint = UUID.randomUUID().toString();
        launchState.put(messageHint, new LtiLaunchStateService.LaunchState(
                reg.getId(), link.getId(), LtiClaims.MSG_RESOURCE_LINK_REQUEST, userId));

        String url = UriComponentsBuilder.fromUriString(reg.getInitiateLoginUri())
                .queryParam("iss", properties.getIssuer())
                .queryParam("login_hint", userId)
                .queryParam("target_link_uri", link.getTargetLinkUri())
                .queryParam("client_id", reg.getClientId())
                .queryParam("lti_deployment_id", reg.getDeploymentId())
                .queryParam("lti_message_hint", messageHint)
                .build(true)
                .toUriString();

        log.debug("LTI leg 1 redirect -> {}", url);
        response.sendRedirect(url);
    }

    /**
     * LEG 3: Moodle has bounced the browser back with its auth request.
     * Validates the message hint and nonce, then returns the signed id_token as
     * an auto-submitting form (response_mode=form_post).
     */
    @RequestMapping(value = "/oidc/authorize",
            method = {RequestMethod.GET, RequestMethod.POST},
            produces = MediaType.TEXT_HTML_VALUE)
    public String authorize(@RequestParam(name = "client_id", required = false) String clientId,
                            @RequestParam(name = "redirect_uri") String redirectUri,
                            @RequestParam(name = "state") String state,
                            @RequestParam(name = "nonce") String nonce,
                            @RequestParam(name = "lti_message_hint", required = false) String messageHint) {

        if (messageHint == null) {
            return error("Missing lti_message_hint. Moodle did not echo it back.");
        }

        var session = launchState.consume(messageHint);
        if (session.isEmpty()) {
            return error("Unknown, expired, or already-used launch. "
                    + "This is also what a replayed request looks like.");
        }
        if (!launchState.claimNonce(nonce)) {
            return error("Nonce has already been used; rejecting as a replay.");
        }

        LtiLaunchStateService.LaunchState s = session.get();
        LtiProviderRegistrationEntity reg =
                registrationRepository.findById(s.getRegistrationId()).orElse(null);
        if (reg == null) {
            return error("Registration no longer exists.");
        }
        if (clientId != null && !clientId.equals(reg.getClientId())) {
            return error("client_id mismatch: Moodle sent " + clientId
                    + ", registration holds " + reg.getClientId());
        }

        try {
            String idToken;
            if (LtiClaims.MSG_DEEP_LINKING_REQUEST.equals(s.getMessageType())) {
                idToken = idTokenService.deepLinkingToken(reg, s.getUserId(), nonce, messageHint);
            } else {
                LtiResourceLinkEntity link = resourceLinkRepository.findById(s.getResourceLinkId())
                        .orElseThrow(() -> new IllegalStateException("Resource link vanished mid-launch"));
                idToken = idTokenService.resourceLinkToken(reg, link, s.getUserId(), nonce);
            }
            return autoSubmitForm(redirectUri, idToken, state);
        } catch (Exception e) {
            log.error("Failed to mint id_token", e);
            return error("Failed to mint id_token: " + e.getMessage());
        }
    }

    /**
     * response_mode=form_post: the token is delivered by an auto-submitting
     * POST form rather than a URL fragment, keeping it out of browser history,
     * referrer headers and server logs.
     */
    private String autoSubmitForm(String redirectUri, String idToken, String state) {
        return """
               <!doctype html>
               <html><head><title>Launching</title></head>
               <body onload="document.forms[0].submit()">
                 <form method="POST" action="%s">
                   <input type="hidden" name="id_token" value="%s"/>
                   <input type="hidden" name="state" value="%s"/>
                   <noscript><button type="submit">Continue</button></noscript>
                 </form>
               </body></html>
               """.formatted(escape(redirectUri), escape(idToken), escape(state));
    }

    private String error(String message) {
        log.warn("LTI launch rejected: {}", message);
        return "<!doctype html><html><body style=\"font-family:system-ui;padding:2rem\">"
                + "<h2>Launch rejected</h2><p>" + escape(message) + "</p></body></html>";
    }

    private static String escape(String value) {
        if (value == null) {
            return "";
        }
        return value.replace("&", "&amp;").replace("<", "&lt;")
                .replace(">", "&gt;").replace("\"", "&quot;");
    }
}
