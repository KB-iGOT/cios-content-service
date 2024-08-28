package com.igot.cios.util;


import lombok.Getter;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
@Getter
@Setter
public class CbServerProperties {

    @Value("${cios.read.api.base.url}")
    private String baseUrl;

    @Value("${cios.read.api.fixed.url}")
    private String fixedUrl;

    @Value("${kong.api.auth.token}")
    private String token;

    @Value("${service.locator.host}")
    private String serviceLocatorHost;

    @Value("${service.locator.fixedurl}")
    private String serviceLocatorFixedUrl;

    @Value("${spring.kafka.cornell.topic.name}")
    private String topic;

    @Value("${cornell.progress.transformation.source-to-target.spec.path}")
    private String progressPathOfTragetFile;

    @Value("${cornell.host.address}")
    private String cornellHostAddress;

    @Value("${cornell.enrollment.service.code}")
    private String cornellEnrollmentServiceCode;

    @Value("${cornell.enrollment.list.limit}")
    private String cornellEnrollmentListLimit;

    @Value("${cornell.enrollment.list.course_type}")
    private String cornellEnrollmentListCourseType;

    @Value("${cornell.date.range}")
    private int cornellDateRange;

}
