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
    CourseSchedulerService courseSchedulerService;

    @Value("${scheduler.enabled}")
    private boolean schedulerEnabled;

    @Scheduled(cron = "${scheduler.cron}")
    private void callEnrollmentApi() throws JsonProcessingException {
        if (schedulerEnabled) {
            log.info("CourseScheduler :: callEnrollmentApi");
            courseSchedulerService.loadCornellEnrollment();
        }
    }
}
