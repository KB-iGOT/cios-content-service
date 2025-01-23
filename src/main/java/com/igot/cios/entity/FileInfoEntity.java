package com.igot.cios.entity;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.sql.Timestamp;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@Entity
@Table(name = "file_information_entity")
public class FileInfoEntity {
    @Id
    private String fileId;

    private String fileName;

    private Timestamp initiatedOn;

    private Timestamp completedOn;

    private String status;

    private String partnerId;

    private String GCPFileName;

    private  String contentUploadedGCPFileName;
}
