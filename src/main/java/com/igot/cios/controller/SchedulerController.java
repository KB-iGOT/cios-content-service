package com.igot.cios.controller;

import com.igot.cios.dto.SBApiResponse;
import com.igot.cios.scheduler.CdacSchedulerService;
import com.igot.cios.scheduler.CornellSchedulerService;
import com.igot.cios.scheduler.CourseraSchedulerService;
import com.igot.cios.scheduler.HarvardSchedulerService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/ciosIntegration/v1/scheduler")
@Slf4j
public class SchedulerController {

    private final CornellSchedulerService cornellSchedulerService;
    private final CourseraSchedulerService courseraSchedulerService;
    private final CdacSchedulerService cdacSchedulerService;
    private final HarvardSchedulerService harvardSchedulerService;

    public SchedulerController(CornellSchedulerService cornellSchedulerService,
                               CourseraSchedulerService courseraSchedulerService,
                               CdacSchedulerService cdacSchedulerService,
                               HarvardSchedulerService harvardSchedulerService) {
        this.cornellSchedulerService = cornellSchedulerService;
        this.courseraSchedulerService = courseraSchedulerService;
        this.cdacSchedulerService = cdacSchedulerService;
        this.harvardSchedulerService = harvardSchedulerService;
    }

    @GetMapping("/cornell/progress")
    public ResponseEntity<SBApiResponse> triggerCornellEnrollment() {
        log.info("SchedulerController :: triggerCornellEnrollment - Manual trigger received");
        SBApiResponse response = cornellSchedulerService.loadCornellEnrollment();
        return new ResponseEntity<>(response, response.getResponseCode());
    }

    @GetMapping("/coursera/progress")
    public ResponseEntity<SBApiResponse> triggerCourseraEnrollment() {
        log.info("SchedulerController :: triggerCourseraEnrollment - Manual trigger received");
        SBApiResponse response = courseraSchedulerService.loadCourseraEnrollment();
        return new ResponseEntity<>(response, response.getResponseCode());
    }

    @GetMapping("/cdac/progress")
    public ResponseEntity<SBApiResponse> triggerCdacEnrollment() {
        log.info("SchedulerController :: triggerCdacEnrollment - Manual trigger received");
        SBApiResponse response = cdacSchedulerService.loadCdacEnrollment();
        return new ResponseEntity<>(response, response.getResponseCode());
    }

    @GetMapping("/harvard/progress")
    public ResponseEntity<SBApiResponse> triggerHarvardEnrollment() {
        log.info("SchedulerController :: triggerHarvardEnrollment - Manual trigger received");
        SBApiResponse response = harvardSchedulerService.loadHarvardEnrollment();
        return new ResponseEntity<>(response, response.getResponseCode());
    }
}


