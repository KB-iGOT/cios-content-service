package com.igot.cios.entity;

import com.fasterxml.jackson.databind.JsonNode;
import com.vladmihalcea.hibernate.type.json.JsonBinaryType;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.hibernate.annotations.Type;
import org.hibernate.annotations.TypeDef;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@Entity
@Table(name = "cios_log_info")
@TypeDef(name = "jsonb", typeClass = JsonBinaryType.class)
public class FileLogInfoEntity {
    @Id
    private String id;

    private String fileId;

    @Type(type = "jsonb")
    @Column(columnDefinition = "jsonb")
    private JsonNode logData;

    private boolean isHasFailure = false;

}
