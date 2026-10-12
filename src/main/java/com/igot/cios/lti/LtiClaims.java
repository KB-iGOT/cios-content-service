package com.igot.cios.lti;

/**
 * Exact LTI 1.3 claim URIs, message types, roles, scopes and media types.
 *
 * <p>These strings are fixed by the 1EdTech specification. A typo produces a
 * launch that fails inside Moodle with an unhelpful error, so they are
 * centralised here rather than inlined at call sites.
 */
public final class LtiClaims {

    private static final String LTI = "https://purl.imsglobal.org/spec/lti/claim/";
    private static final String AGS = "https://purl.imsglobal.org/spec/lti-ags/claim/";
    private static final String DL  = "https://purl.imsglobal.org/spec/lti-dl/claim/";

    public static final String MESSAGE_TYPE        = LTI + "message_type";
    public static final String VERSION             = LTI + "version";
    public static final String DEPLOYMENT_ID       = LTI + "deployment_id";
    public static final String TARGET_LINK_URI     = LTI + "target_link_uri";
    public static final String RESOURCE_LINK       = LTI + "resource_link";
    public static final String ROLES               = LTI + "roles";
    public static final String CONTEXT             = LTI + "context";
    public static final String LAUNCH_PRESENTATION = LTI + "launch_presentation";

    public static final String AGS_ENDPOINT     = AGS + "endpoint";
    public static final String DL_SETTINGS      = DL + "deep_linking_settings";
    public static final String DL_CONTENT_ITEMS = DL + "content_items";
    public static final String DL_DATA          = DL + "data";

    public static final String MSG_RESOURCE_LINK_REQUEST = "LtiResourceLinkRequest";
    public static final String MSG_DEEP_LINKING_REQUEST  = "LtiDeepLinkingRequest";

    public static final String LTI_VERSION_1_3 = "1.3.0";

    public static final String ROLE_LEARNER =
            "http://purl.imsglobal.org/vocab/lis/v2/membership#Learner";

    public static final String SCOPE_LINEITEM =
            "https://purl.imsglobal.org/spec/lti-ags/scope/lineitem";
    public static final String SCOPE_SCORE =
            "https://purl.imsglobal.org/spec/lti-ags/scope/score";

    /** AGS media types. The version skew is correct per spec: score is v1, line items v2. */
    public static final String MT_LINEITEM      = "application/vnd.ims.lis.v2.lineitem+json";
    public static final String MT_LINEITEM_LIST = "application/vnd.ims.lis.v2.lineitemcontainer+json";
    public static final String MT_SCORE         = "application/vnd.ims.lis.v1.score+json";

    /** Redis key prefixes for ephemeral launch state. */
    public static final String REDIS_MESSAGE_HINT = "lti:hint:";
    public static final String REDIS_NONCE        = "lti:nonce:";
    public static final String REDIS_DL_DATA      = "lti:dl:data:";
    public static final String REDIS_ACCESS_TOKEN = "lti:token:";

    private LtiClaims() {
    }
}
