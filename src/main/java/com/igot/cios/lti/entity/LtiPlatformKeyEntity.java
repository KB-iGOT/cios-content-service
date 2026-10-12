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
 * iGOT's own RSA signing keypair.
 *
 * <p><b>Global, not per registration.</b> Every tool fetches the same JWKS URL
 * and selects a key by kid. Registering a new provider does not create a key.
 *
 * <p>Multiple rows exist for <b>rotation lifecycle</b>, not multiplicity of
 * tools: typically two or three rows regardless of provider count.
 *
 * <p><b>SECURITY, open item.</b> privateKeyJwk holds key material directly.
 * Production must move this to a secrets manager and keep only a reference.
 * Tracked in the LTI HLD; do not promote as-is.
 */
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@Entity
@Table(name = "lti_platform_key")
@JsonIgnoreProperties(ignoreUnknown = true)
public class LtiPlatformKeyEntity implements Serializable {

    @Id
    private String kid;

    private String algorithm;

    @Column(columnDefinition = "TEXT")
    private String publicKeyJwk;

    @Column(columnDefinition = "TEXT")
    private String privateKeyJwk;

    /** active | rotating | retired */
    private String status;

    private Timestamp createdDate;

    private Timestamp activatedDate;

    private Timestamp retiredDate;
}
