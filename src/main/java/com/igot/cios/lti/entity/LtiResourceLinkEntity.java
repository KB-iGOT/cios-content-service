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
 * Binds a catalog item to the provider registration that launches it.
 *
 * <p>externalId + partnerId deliberately mirror CornellContentEntity's
 * composite key so the two join naturally.
 *
 * <p>targetLinkUri lives here rather than reusing CornellContentEntity's
 * redirectUrl on purpose: redirectUrl is rendered directly as an href by the
 * portal, and an LTI target link must never be opened as a plain hyperlink.
 * Separate columns make that impossible to get wrong by accident.
 */
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@Entity
@Table(name = "lti_resource_link")
@JsonIgnoreProperties(ignoreUnknown = true)
public class LtiResourceLinkEntity implements Serializable {

    @Id
    private String id;

    private String externalId;

    private String partnerId;

    private String registrationId;

    @Column(columnDefinition = "TEXT")
    private String targetLinkUri;

    private String title;

    private Double scoreMaximum;

    private Timestamp createdDate;

    private Timestamp updatedDate;
}
