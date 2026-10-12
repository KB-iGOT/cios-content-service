package com.igot.cios.lti.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Platform-level LTI configuration.
 *
 * <p>Every value here is <b>global</b> — identical for every registered
 * provider. Only clientId and deploymentId differ per provider, and those live
 * on the registration row.
 *
 * <p>The issuer must be byte-identical everywhere it appears: the iss claim,
 * the URLs given to Moodle, and how the browser reaches this service. A
 * mismatch is the most common cause of a failed launch.
 */
@Component
@Getter
@Setter
public class LtiPlatformProperties {

    @Value("${lti.platform.issuer}")
    private String issuer;

    @Value("${lti.platform.base-path:/ciosIntegration/v1/lti}")
    private String basePath;

    @Value("${lti.platform.id-token.ttl.seconds:300}")
    private long idTokenTtlSeconds;

    @Value("${lti.platform.access-token.ttl.seconds:3600}")
    private long accessTokenTtlSeconds;

    @Value("${lti.platform.launch-state.ttl.seconds:600}")
    private long launchStateTtlSeconds;

    public String authorizeUrl() {
        return issuer + basePath + "/oidc/authorize";
    }

    public String tokenUrl() {
        return issuer + basePath + "/oauth2/token";
    }

    public String jwksUrl() {
        return issuer + basePath + "/.well-known/jwks.json";
    }

    public String deepLinkReturnUrl() {
        return issuer + basePath + "/deeplink/response";
    }

    public String agsContextBase(String contextId) {
        return issuer + basePath + "/ags/contexts/" + contextId;
    }
}
