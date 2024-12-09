package com.igot.cios.scheduler;

import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;


@Component
@Slf4j
public class CourseScheduler {

    @Autowired
    CornellSchedulerService cornellSchedulerService;

    @Autowired
    CourseraSchedulerService courseraSchedulerService;

    @Value("${scheduler.enabled}")
    private boolean schedulerEnabled;

    @Value("${coursera.scheduler.enabled}")
    private boolean courseraSchedulerEnabled;

    @Scheduled(cron = "${scheduler.cron}")
    private void callCornellEnrollmentApi() throws JsonProcessingException{
        if (schedulerEnabled) {
            log.info("CourseScheduler :: callCornellEnrollmentApi");
            cornellSchedulerService.loadEnrollment();
        }
    }

    @Scheduled(cron = "${coursera.scheduler.cron}")
    private void callCourseraEnrollmentApi() throws JsonProcessingException {
        if (courseraSchedulerEnabled) {
            log.info("CourseScheduler :: callCourseraEnrollmentApi");
            courseraSchedulerService.loadEnrollment();
        }
    }
}
