package com.igot.cios.entity;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.JsonNode;
import com.vladmihalcea.hibernate.type.json.JsonType;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.hibernate.annotations.Type;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@Entity
@Table(name = "cios_log_info")
@JsonIgnoreProperties(ignoreUnknown = true)
public class FileLogInfoEntity {
    @Id
    private String id;

    private String fileId;

    @Type(JsonType.class)
    @Column(columnDefinition = "jsonb")
    private JsonNode logData;

    private boolean isHasFailure = false;

}
