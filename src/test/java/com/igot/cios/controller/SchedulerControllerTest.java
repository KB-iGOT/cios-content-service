package com.igot.cios.controller;

import com.igot.cios.dto.ApiRespParam;
import com.igot.cios.dto.SBApiResponse;
import com.igot.cios.scheduler.CdacSchedulerService;
import com.igot.cios.scheduler.CornellSchedulerService;
import com.igot.cios.scheduler.CourseraSchedulerService;
import com.igot.cios.scheduler.HarvardSchedulerService;
import com.igot.cios.util.Constants;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class SchedulerControllerTest {

    @Mock
    private CornellSchedulerService cornellSchedulerService;

    @Mock
    private CourseraSchedulerService courseraSchedulerService;

    @Mock
    private CdacSchedulerService cdacSchedulerService;

    @Mock
    private HarvardSchedulerService harvardSchedulerService;

    @InjectMocks
    private SchedulerController schedulerController;

    private SBApiResponse successResponse;
    private SBApiResponse errorResponse;

    @BeforeEach
    void setUp() {
        successResponse = new SBApiResponse();
        successResponse.setId("test.api");
        successResponse.setVer(Constants.API_VERSION_1);
        successResponse.setTs("2026-01-28T10:00:00");
        successResponse.setResponseCode(HttpStatus.OK);
        ApiRespParam successParams = new ApiRespParam();
        successParams.setStatus(Constants.SUCCESS);
        successResponse.setParams(successParams);

        errorResponse = new SBApiResponse();
        errorResponse.setId("test.api");
        errorResponse.setVer(Constants.API_VERSION_1);
        errorResponse.setTs("2026-01-28T10:00:00");
        errorResponse.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
        ApiRespParam errorParams = new ApiRespParam();
        errorParams.setStatus(Constants.FAILED);
        errorParams.setErrmsg("Test error message");
        errorResponse.setParams(errorParams);
    }

    @Test
    void testTriggerCornellEnrollment_success() {
        when(cornellSchedulerService.loadCornellEnrollment()).thenReturn(successResponse);

        ResponseEntity<SBApiResponse> response = schedulerController.triggerCornellEnrollment();

        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertNotNull(response.getBody());
        assertEquals(successResponse, response.getBody());
        assertEquals(Constants.SUCCESS, response.getBody().getParams().getStatus());
        verify(cornellSchedulerService, times(1)).loadCornellEnrollment();
    }

    @Test
    void testTriggerCornellEnrollment_error() {
        when(cornellSchedulerService.loadCornellEnrollment()).thenReturn(errorResponse);

        ResponseEntity<SBApiResponse> response = schedulerController.triggerCornellEnrollment();

        assertNotNull(response);
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getStatusCode());
        assertNotNull(response.getBody());
        assertEquals(errorResponse, response.getBody());
        assertEquals(Constants.FAILED, response.getBody().getParams().getStatus());
        assertEquals("Test error message", response.getBody().getParams().getErrmsg());
        verify(cornellSchedulerService, times(1)).loadCornellEnrollment();
    }

    @Test
    void testTriggerCourseraEnrollment_success() {
        when(courseraSchedulerService.loadCourseraEnrollment()).thenReturn(successResponse);

        ResponseEntity<SBApiResponse> response = schedulerController.triggerCourseraEnrollment();

        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertNotNull(response.getBody());
        assertEquals(successResponse, response.getBody());
        assertEquals(Constants.SUCCESS, response.getBody().getParams().getStatus());
        verify(courseraSchedulerService, times(1)).loadCourseraEnrollment();
    }

    @Test
    void testTriggerCourseraEnrollment_error() {
        when(courseraSchedulerService.loadCourseraEnrollment()).thenReturn(errorResponse);

        ResponseEntity<SBApiResponse> response = schedulerController.triggerCourseraEnrollment();

        assertNotNull(response);
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getStatusCode());
        assertNotNull(response.getBody());
        assertEquals(errorResponse, response.getBody());
        assertEquals(Constants.FAILED, response.getBody().getParams().getStatus());
        assertEquals("Test error message", response.getBody().getParams().getErrmsg());
        verify(courseraSchedulerService, times(1)).loadCourseraEnrollment();
    }

    @Test
    void testTriggerCdacEnrollment_success() {
        when(cdacSchedulerService.loadCdacEnrollment()).thenReturn(successResponse);

        ResponseEntity<SBApiResponse> response = schedulerController.triggerCdacEnrollment();

        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertNotNull(response.getBody());
        assertEquals(successResponse, response.getBody());
        assertEquals(Constants.SUCCESS, response.getBody().getParams().getStatus());
        verify(cdacSchedulerService, times(1)).loadCdacEnrollment();
    }

    @Test
    void testTriggerCdacEnrollment_error() {
        when(cdacSchedulerService.loadCdacEnrollment()).thenReturn(errorResponse);

        ResponseEntity<SBApiResponse> response = schedulerController.triggerCdacEnrollment();

        assertNotNull(response);
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getStatusCode());
        assertNotNull(response.getBody());
        assertEquals(errorResponse, response.getBody());
        assertEquals(Constants.FAILED, response.getBody().getParams().getStatus());
        assertEquals("Test error message", response.getBody().getParams().getErrmsg());
        verify(cdacSchedulerService, times(1)).loadCdacEnrollment();
    }

    @Test
    void testTriggerHarvardEnrollment_success() {
        when(harvardSchedulerService.loadHarvardEnrollment()).thenReturn(successResponse);

        ResponseEntity<SBApiResponse> response = schedulerController.triggerHarvardEnrollment();

        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertNotNull(response.getBody());
        assertEquals(successResponse, response.getBody());
        assertEquals(Constants.SUCCESS, response.getBody().getParams().getStatus());
        verify(harvardSchedulerService, times(1)).loadHarvardEnrollment();
    }

    @Test
    void testTriggerHarvardEnrollment_error() {
        when(harvardSchedulerService.loadHarvardEnrollment()).thenReturn(errorResponse);

        ResponseEntity<SBApiResponse> response = schedulerController.triggerHarvardEnrollment();

        assertNotNull(response);
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getStatusCode());
        assertNotNull(response.getBody());
        assertEquals(errorResponse, response.getBody());
        assertEquals(Constants.FAILED, response.getBody().getParams().getStatus());
        assertEquals("Test error message", response.getBody().getParams().getErrmsg());
        verify(harvardSchedulerService, times(1)).loadHarvardEnrollment();
    }

    @Test
    void testTriggerHarvardEnrollment_withResult() {
        SBApiResponse harvardResponse = new SBApiResponse();
        harvardResponse.setId("harvard.enrollment");
        harvardResponse.setVer(Constants.API_VERSION_1);
        harvardResponse.setTs("2026-01-28T10:00:00");
        harvardResponse.setResponseCode(HttpStatus.OK);
        harvardResponse.put("totalFiles", 10);
        harvardResponse.put("processedFiles", 8);
        harvardResponse.put("failedFiles", 2);
        ApiRespParam params = new ApiRespParam();
        params.setStatus(Constants.SUCCESS);
        harvardResponse.setParams(params);

        when(harvardSchedulerService.loadHarvardEnrollment()).thenReturn(harvardResponse);

        ResponseEntity<SBApiResponse> response = schedulerController.triggerHarvardEnrollment();

        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertNotNull(response.getBody());
        assertEquals(10, response.getBody().get("totalFiles"));
        assertEquals(8, response.getBody().get("processedFiles"));
        assertEquals(2, response.getBody().get("failedFiles"));
        verify(harvardSchedulerService, times(1)).loadHarvardEnrollment();
    }

    @Test
    void testConstructor() {
        SchedulerController controller = new SchedulerController(
                cornellSchedulerService,
                courseraSchedulerService,
                cdacSchedulerService,
                harvardSchedulerService
        );

        assertNotNull(controller);
    }

    @Test
    void testResponseCodePropagation_differentStatusCodes() {
        SBApiResponse okResponse = new SBApiResponse();
        okResponse.setResponseCode(HttpStatus.OK);
        ApiRespParam okParams = new ApiRespParam();
        okParams.setStatus(Constants.SUCCESS);
        okResponse.setParams(okParams);
        when(cornellSchedulerService.loadCornellEnrollment()).thenReturn(okResponse);

        ResponseEntity<SBApiResponse> response1 = schedulerController.triggerCornellEnrollment();
        assertEquals(HttpStatus.OK, response1.getStatusCode());

        SBApiResponse errorResponse500 = new SBApiResponse();
        errorResponse500.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
        ApiRespParam errorParams = new ApiRespParam();
        errorParams.setStatus(Constants.FAILED);
        errorResponse500.setParams(errorParams);
        when(cornellSchedulerService.loadCornellEnrollment()).thenReturn(errorResponse500);

        ResponseEntity<SBApiResponse> response2 = schedulerController.triggerCornellEnrollment();
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response2.getStatusCode());
    }

    @Test
    void testAllEndpoints_verifyServiceCalls() {
        when(cornellSchedulerService.loadCornellEnrollment()).thenReturn(successResponse);
        when(courseraSchedulerService.loadCourseraEnrollment()).thenReturn(successResponse);
        when(cdacSchedulerService.loadCdacEnrollment()).thenReturn(successResponse);
        when(harvardSchedulerService.loadHarvardEnrollment()).thenReturn(successResponse);

        schedulerController.triggerCornellEnrollment();
        schedulerController.triggerCourseraEnrollment();
        schedulerController.triggerCdacEnrollment();
        schedulerController.triggerHarvardEnrollment();

        verify(cornellSchedulerService, times(1)).loadCornellEnrollment();
        verify(courseraSchedulerService, times(1)).loadCourseraEnrollment();
        verify(cdacSchedulerService, times(1)).loadCdacEnrollment();
        verify(harvardSchedulerService, times(1)).loadHarvardEnrollment();
        verifyNoMoreInteractions(cornellSchedulerService, courseraSchedulerService,
                                 cdacSchedulerService, harvardSchedulerService);
    }
}
