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
}
