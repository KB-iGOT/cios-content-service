package com.igot.cios.lti.controller;

import com.igot.cios.lti.LtiClaims;
import com.igot.cios.lti.config.LtiPlatformProperties;
import com.igot.cios.lti.entity.LtiProviderRegistrationEntity;
import com.igot.cios.lti.entity.LtiResourceLinkEntity;
import com.igot.cios.lti.repository.LtiProviderRegistrationRepository;
import com.igot.cios.lti.repository.LtiResourceLinkRepository;
import com.igot.cios.lti.service.LtiAccessTokenService;
import com.igot.cios.lti.service.LtiLaunchStateService;
import com.nimbusds.jwt.SignedJWT;
import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.util.UriComponentsBuilder;

import java.io.IOException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * Deep Linking: the author-side content picker.
 *
 * <p>The Deep Linking request is not its own endpoint. It is an ordinary OIDC
 * launch with a different message type, so it reuses /oidc/authorize. Only the
 * trigger and the response receiver are new.
 */
@Slf4j
@RestController
@RequestMapping("/ciosIntegration/v1/lti/deeplink")
@RequiredArgsConstructor
public class LtiDeepLinkController {

    private final LtiProviderRegistrationRepository registrationRepository;
    private final LtiResourceLinkRepository resourceLinkRepository;
    private final LtiLaunchStateService launchState;
    private final LtiAccessTokenService tokenService;
    private final LtiPlatformProperties properties;

    /** TRIGGER: an author wants to pick content. Starts an OIDC launch carrying LtiDeepLinkingRequest. */
    @GetMapping("/initiate/{registrationId}")
    public void initiate(@PathVariable String registrationId,
                         @RequestParam String userId,
                         HttpServletResponse response) throws IOException {

        LtiProviderRegistrationEntity reg = registrationRepository.findById(registrationId)
                .orElseThrow(() -> new IllegalArgumentException("Unknown registration"));

        if (reg.getDeepLinkingUri() == null || reg.getDeepLinkingUri().isBlank()) {
            response.setContentType(MediaType.TEXT_HTML_VALUE);
            response.getWriter().write(
                    "<p>This provider has no Deep Linking URL, so it exposes no content picker. "
                            + "Add content manually instead.</p>");
            return;
        }

        String messageHint = UUID.randomUUID().toString();
        launchState.put(messageHint, new LtiLaunchStateService.LaunchState(
                reg.getId(), null, LtiClaims.MSG_DEEP_LINKING_REQUEST, userId));
        launchState.putDeepLinkData(messageHint);

        String url = UriComponentsBuilder.fromUriString(reg.getInitiateLoginUri())
                .queryParam("iss", properties.getIssuer())
                .queryParam("login_hint", userId)
                .queryParam("target_link_uri", reg.getDeepLinkingUri())
                .queryParam("client_id", reg.getClientId())
                .queryParam("lti_deployment_id", reg.getDeploymentId())
                .queryParam("lti_message_hint", messageHint)
                .build(true)
                .toUriString();

        log.debug("Deep Linking initiate -> {}", url);
        response.sendRedirect(url);
    }

    /**
     * RESPONSE: Moodle posts back the author's selections as a signed JWT. Two
     * checks are mandatory: the signature must verify against Moodle's JWKS,
     * and the data claim must echo the token we issued (the spec's CSRF check).
     */
    @PostMapping(value = "/response", produces = MediaType.TEXT_HTML_VALUE)
    public String response(@RequestParam("JWT") String jwtParam) {
        try {
            SignedJWT jwt = SignedJWT.parse(jwtParam);
            var claims = jwt.getJWTClaimsSet();

            String toolIssuer = claims.getIssuer();
            String audience = claims.getAudience() != null && !claims.getAudience().isEmpty()
                    ? claims.getAudience().get(0) : null;

            LtiProviderRegistrationEntity reg = registrationRepository.findByClientId(toolIssuer)
                    .or(() -> audience != null
                            ? registrationRepository.findByClientId(audience)
                            : Optional.empty())
                    .orElse(null);

            if (reg == null) {
                return page("Rejected", "Could not match this response to a registration.");
            }
            if (!tokenService.verifySignature(jwt, reg)) {
                return page("Rejected", "Signature did not verify against the tool's JWKS.");
            }

            String data = claims.getStringClaim(LtiClaims.DL_DATA);
            if (!launchState.consumeDeepLinkData(data)) {
                return page("Rejected", "The data value was missing or did not match one we issued.");
            }

            Object itemsClaim = claims.getClaim(LtiClaims.DL_CONTENT_ITEMS);
            if (!(itemsClaim instanceof List<?> items) || items.isEmpty()) {
                return page("Nothing selected", "The provider returned no content items.");
            }

            StringBuilder added = new StringBuilder();
            for (Object raw : items) {
                if (!(raw instanceof Map<?, ?> item)) {
                    continue;
                }
                if (!"ltiResourceLink".equals(String.valueOf(item.get("type")))) {
                    continue;
                }

                Double scoreMaximum = null;
                if (item.get("lineItem") instanceof Map<?, ?> lineItem
                        && lineItem.get("scoreMaximum") instanceof Number number) {
                    scoreMaximum = number.doubleValue();
                }

                Object titleRaw = item.get("title");
                Timestamp now = Timestamp.from(Instant.now());

                LtiResourceLinkEntity link = new LtiResourceLinkEntity();
                link.setId(UUID.randomUUID().toString());
                link.setRegistrationId(reg.getId());
                link.setPartnerId(reg.getPartnerId());
                link.setExternalId(String.valueOf(String.valueOf(item.get("url")).hashCode()));
                link.setTargetLinkUri(String.valueOf(item.get("url")));
                link.setTitle(titleRaw == null ? "Untitled" : String.valueOf(titleRaw));
                link.setScoreMaximum(scoreMaximum);
                link.setCreatedDate(now);
                link.setUpdatedDate(now);
                resourceLinkRepository.save(link);

                added.append("<li>").append(escape(link.getTitle())).append("</li>");
                log.info("Deep Linking added resource link {} -> {}", link.getTitle(), link.getTargetLinkUri());
            }

            return page("Content added",
                    "<ul>" + added + "</ul>"
                            + "<p>Deep Linking does not carry <em>duration</em> or <em>objectives</em>, "
                            + "so an author must still supply those before publishing to the catalog.</p>");

        } catch (Exception e) {
            log.error("Deep Linking response failed", e);
            return page("Error", escape(e.toString()));
        }
    }

    private String page(String title, String bodyHtml) {
        return """
               <!doctype html><html><head><title>%s</title></head>
               <body style="font-family:system-ui;padding:2rem;max-width:40rem">
                 <h2>%s</h2>%s
               </body></html>
               """.formatted(escape(title), escape(title), bodyHtml);
    }

    private static String escape(String value) {
        if (value == null) {
            return "";
        }
        return value.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;");
    }
}
