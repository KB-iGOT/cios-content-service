package com.igot.cios.lti.entity;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;
import java.sql.Timestamp;

/**
 * One registered LTI Tool, e.g. a Moodle site.
 *
 * <p>For Moodle this is <b>one row per Moodle site</b>, not per course. Moodle
 * uses a single site-level registration and exposes its published courses
 * through the Deep Linking picker.
 *
 * <p>Note the direction of the URL fields: initiateLoginUri, redirectUris and
 * jwksUri all belong to the <b>Tool</b>. We use them to start a launch and to
 * verify signatures Moodle produces. Our own signing key lives in
 * LtiPlatformKeyEntity.
 */
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@Entity
@Table(name = "lti_provider_registration")
@JsonIgnoreProperties(ignoreUnknown = true)
public class LtiProviderRegistrationEntity implements Serializable {

    @Id
    private String id;

    private String partnerCode;

    private String partnerId;

    private String providerName;

    /** The client_id WE issue to this tool. Unique across registrations. */
    @Column(unique = true)
    private String clientId;

    private String deploymentId;

    @Column(columnDefinition = "TEXT")
    private String initiateLoginUri;

    @Column(columnDefinition = "TEXT")
    private String redirectUris;

    @Column(columnDefinition = "TEXT")
    private String jwksUri;

    /** Null when the tool exposes no content picker. */
    @Column(columnDefinition = "TEXT")
    private String deepLinkingUri;

    private String environment;

    private String status;

    private Boolean isActive;

    private Timestamp createdDate;

    private Timestamp updatedDate;
}
