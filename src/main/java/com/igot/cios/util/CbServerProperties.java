package com.igot.cios.util;


import lombok.Getter;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
@Getter
@Setter
public class CbServerProperties {

    @Value("${cios.read.api.fixed.url}")
    private String fixedUrl;

    @Value("${service.locator.host}")
    private String serviceLocatorHost;

    @Value("${service.locator.fixedurl}")
    private String serviceLocatorFixedUrl;

    @Value("${spring.kafka.cornell.topic.name}")
    private String topic;

    @Value("${cornell.progress.transformation.source-to-target.spec.path}")
    private String progressPathOfTragetFile;

    @Value("${cornell.enrollment.service.code}")
    private String cornellEnrollmentServiceCode;

    @Value("${cornell.enrollment.list.limit}")
    private String cornellEnrollmentListLimit;

    @Value("${cornell.enrollment.list.course_type}")
    private String cornellEnrollmentListCourseType;

    @Value("${cornell.date.range}")
    private int cornellDateRange;

    @Value("${cb.pores.service.url}")
    private String cbPoresbaseUrl;

    @Value("${partner.read.path}")
    private String partnerReadEndPoint;

    @Value("${partner.create.update.path}")
    private String partnerCreateEndPoint;

    @Value("${elastic.required.field.cios.content.json.path}")
    private String elasticCiosContentJsonPath;

    @Value("${cloud.storage.type.name}")
    private String cloudStorageTypeName;

    @Value("${cloud.storage.secret}")
    private String cloudStorageSecret;

    @Value("${cloud.storage.key}")
    private String cloudStorageKey;

    @Value("${cloud.storage.endpoint}")
    private String cloudStorageEndpoint;

    @Value("${cios.cloud.container.name}")
    private String ciosCloudContainerName;

    @Value("${cios.logs.cloud.folder.name}")
    private String ciosFileLogsCloudFolderName;

    @Value("${kafka.topic.content.onboarding}")
    private String ciosContentOnboardTopic;

    @Value("${cios.content.cloud.folder.name}")
    private String ciosContentFileCloudFolderName;

    @Value("${cornell.partner.code}")
    public String cornellPartnerCode;

    @Value("${coursera.partner.code}")
    public String courseraPartnerCode;

    @Value("${coursera.enrollment.service.code}")
    private String courseraEnrollmentServiceCode;

    @Value("${coursera.enrollment.list.limit}")
    private int courseraEnrollmentListLimit;

    @Value("${coursera.enrollment.list.course_type}")
    private String courseraEnrollmentListCourseType;

    @Value("${coursera.date.range}")
    private Long courseraDateRange;

    @Value("${coursera.date.before}")
    private String courseraDateBefore;

    @Value("${coursera.date.after}")
    private String courseraDateAfter;

    @Value("${cdac.enrollment.service.code}")
    private String cdacEnrollmentServiceCode;

    @Value("${cdac.api.key}")
    public String cdacApiKey;

    @Value("${cdac.partner.code}")
    public String cdacPartnerCode;

    @Value("${sso.url}")
    public String keycloakUrl;

    @Value("${sso.realm}")
    public String ssoRealm;

    @Value("${sso.username}")
    public String ssoUsername;

    @Value("${sso.password}")
    public String ssoPassword;

    @Value("${sso.admin.token.endpoint}")
    public String ssoAdminTokenEndpoint;

    @Value("${sso.config.create.api}")
    public String ssoConfigCreateApi;

    @Value("${sso.config.mapper.read.api}")
    public String ssoConfigMapperReadApi;

    @Value("${sso.config.mapper.update.api}")
    public String ssoConfigMapperUpdateApi;

    @Value("${harvard.sftp.host}")
    private String harvardSftpHost;

    @Value("${harvard.sftp.port}")
    private int harvardSftpPort;

    @Value("${harvard.sftp.username}")
    private String harvardSftpUsername;

    @Value("${harvard.sftp.password}")
    private String harvardSftpPassword;

    @Value("${harvard.sftp.remote.directory}")
    private String harvardSftpRemoteDirectory;

    @Value("${harvard.sftp.completed.directory}")
    private String harvardSftpCompletedDirectory;

    @Value("${harvard.partner.code}")
    public String harvardPartnerCode;
}
