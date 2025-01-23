package com.igot.cios.entity;


import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.JsonNode;
import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.hibernate.annotations.Type;
import com.vladmihalcea.hibernate.type.json.JsonType;

import java.io.Serializable;
import java.sql.Timestamp;


@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@Entity
@Table(name = "cornell_content_entity")
@IdClass(CornellContentEntityId.class)
@JsonIgnoreProperties(ignoreUnknown = true)
public class CornellContentEntity implements Serializable {
    @Id
    private String externalId;
    @Id
    private String partnerId;
    @Type(JsonType.class)
    @Column(columnDefinition = "jsonb")
    private JsonNode ciosData;
    private Boolean isActive;
    private Timestamp createdDate;
    private Timestamp updatedDate;
    @Type(JsonType.class)
    @Column(columnDefinition = "jsonb")
    private JsonNode sourceData;
    private String fileId;
    private String partnerCode;
}
