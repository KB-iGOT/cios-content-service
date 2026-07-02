package com.igot.cios.scheduler;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.igot.cios.dto.SBApiResponse;
import com.igot.cios.kafka.KafkaProducer;
import com.igot.cios.plugins.DataTransformUtility;
import com.igot.cios.util.CbServerProperties;
import com.igot.cios.util.Constants;
import com.igot.cios.util.PayloadValidation;
import com.igot.cios.util.transactional.cassandrautils.CassandraOperation;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import java.lang.reflect.Method;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class CornellSchedulerServiceTest {

    @Mock
    private ObjectMapper objectMapper;

    @Mock
    private KafkaProducer kafkaProducer;

    @Mock
    private PayloadValidation payloadValidation;

    @Mock
    private RestTemplate restTemplate;

    @Mock
    private CbServerProperties cbServerProperties;

    @Mock
    private CassandraOperation cassandraOperation;

    @Mock
    private DataTransformUtility dataTransformUtility;

    @InjectMocks
    private CornellSchedulerService cornellSchedulerService;

    private ObjectMapper realObjectMapper;

    @BeforeEach
    void setUp() {
        realObjectMapper = new ObjectMapper();
    }

    @Test
    void testUpdateDateFormatFromTimestamp() throws Exception {
        Method method = CornellSchedulerService.class.getDeclaredMethod(
                "updateDateFormatFromTimestamp", Long.class);
        method.setAccessible(true);

        Long timestamp = 1706359845000L;
        String result = (String) method.invoke(cornellSchedulerService, timestamp);

        // Verify date format (should be ISO 8601: yyyy-MM-dd'T'HH:mm:ss'Z')
        assertNotNull(result);
        assertFalse(result.isEmpty());
        assertTrue(result.matches("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}Z"),
            "Expected ISO 8601 format but got: " + result);

        // Verify year is reasonable (2024-2026 range)
        String year = result.substring(0, 4);
        int yearInt = Integer.parseInt(year);
        assertTrue(yearInt >= 2024 && yearInt <= 2026, "Year should be in reasonable range, got: " + yearInt);

        // Verify month is valid (01-12)
        String month = result.substring(5, 7);
        int monthInt = Integer.parseInt(month);
        assertTrue(monthInt >= 1 && monthInt <= 12, "Month should be between 01-12, got: " + monthInt);

        // Verify day is valid (01-31)
        String day = result.substring(8, 10);
        int dayInt = Integer.parseInt(day);
        assertTrue(dayInt >= 1 && dayInt <= 31, "Day should be between 01-31, got: " + dayInt);
    }

    @Test
    void testUpdateDateFormatFromTimestamp_nullTimestamp() throws Exception {
        Method method = CornellSchedulerService.class.getDeclaredMethod(
                "updateDateFormatFromTimestamp", Long.class);
        method.setAccessible(true);

        Exception exception = assertThrows(Exception.class, () ->
            method.invoke(cornellSchedulerService, (Long) null)
        );
        assertNotNull(exception);
    }

    @Test
    void testLoadCornellEnrollment_jsonProcessingException() throws JsonProcessingException {
        when(cbServerProperties.getCornellEnrollmentServiceCode()).thenReturn("CORNELL_SERVICE");
        when(cbServerProperties.getCornellEnrollmentListLimit()).thenReturn(Integer.valueOf("100"));
        when(cbServerProperties.getCornellEnrollmentListCourseType()).thenReturn("online");
        when(cbServerProperties.getCornellDateRange()).thenReturn(7);

        when(objectMapper.writeValueAsString(any())).thenThrow(new JsonProcessingException("Error") {});

        SBApiResponse result = cornellSchedulerService.loadCornellEnrollment();

        // Verify error response structure
        assertNotNull(result);
        assertNotNull(result.getId());
        assertEquals("cornell.enrollment", result.getId());
        assertNotNull(result.getVer());
        assertEquals(Constants.API_VERSION_1, result.getVer());
        assertNotNull(result.getTs());
        assertNotNull(result.getParams());
        assertEquals(Constants.FAILED, result.getParams().getStatus());
        assertNotNull(result.getParams().getErrmsg());
        assertTrue(result.getParams().getErrmsg().contains("Failed to load Cornell enrollment"));
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, result.getResponseCode());

        // Verify no enrollments in error case
        assertFalse(result.containsKey("enrollments") && result.get("enrollments") != null);
    }

    @Test
    void testFormUrlMapForEnrollment() throws Exception {
        when(cbServerProperties.getCornellDateRange()).thenReturn(7);
        when(cbServerProperties.getCornellEnrollmentListCourseType()).thenReturn("online");

        Method method = CornellSchedulerService.class.getDeclaredMethod(
                "formUrlMapForEnrollment",
                int.class,
                int.class);
        method.setAccessible(true);

        @SuppressWarnings("unchecked")
        Map<String, String> result =
                (Map<String, String>) method.invoke(cornellSchedulerService, 0, 100);

        // Verify all required fields are present
        assertNotNull(result);
        assertEquals(4, result.size());

        // Verify offset
        assertEquals("0", result.get("offset"));

        // Verify limit
        assertEquals("100", result.get("limit"));

        // Verify course_type
        assertEquals("online", result.get("course_type"));

        // Verify completion_range
        String completionRange = result.get("completion_range");
        assertNotNull(completionRange);
        assertTrue(completionRange.matches("\\d{8}:\\d{8}"));

        String[] dates = completionRange.split(":");
        assertEquals(2, dates.length);
        assertEquals(8, dates[0].length());
        assertEquals(8, dates[1].length());
    }

    @Test
    void testPerformEnrollmentCall_failureResponse() throws Exception {
        String partnerCode = "cornell";
        String requestBody = "{\"serviceCode\":\"CORNELL_SERVICE\"}";

        when(cbServerProperties.getServiceLocatorHost()).thenReturn("http://localhost");
        when(cbServerProperties.getServiceLocatorFixedUrl()).thenReturn("/api/v1/service");

        when(restTemplate.exchange(anyString(), any(HttpMethod.class), any(HttpEntity.class), eq(Object.class)))
                .thenReturn(ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build());

        Method method = CornellSchedulerService.class.getDeclaredMethod(
                "performEnrollmentCall", String.class, String.class);
        method.setAccessible(true);

        Exception exception = assertThrows(Exception.class, () ->
            method.invoke(cornellSchedulerService, partnerCode, requestBody)
        );

        // Verify exception details
        assertNotNull(exception);
        assertNotNull(exception.getCause());

        // Verify REST call was attempted
        verify(restTemplate, times(1)).exchange(anyString(), any(HttpMethod.class), any(HttpEntity.class), eq(Object.class));
    }

    @Test
    void testCallEnrollmentAPI_successfulFlow() throws Exception {
        String partnerCode = "cornell";
        String partnerId = "partner-id-123";

        ObjectNode contentData = realObjectMapper.createObjectNode();
        contentData.put("courseid", "ext-course-123");
        contentData.put("userid", "user123@example.com");
        contentData.put("completedon", "1706359845000");

        when(cbServerProperties.getTopic()).thenReturn("test-topic");

        ObjectNode ciosResponse = realObjectMapper.createObjectNode();
        ObjectNode content = realObjectMapper.createObjectNode();
        content.put("contentId", "internal-course-123");
        ciosResponse.set("content", content);
        when(dataTransformUtility.callCiosReadApi(anyString(), anyString())).thenReturn(ciosResponse);

        List<Map<String, Object>> cassandraData = new ArrayList<>();
        Map<String, Object> enrollmentRecord = new HashMap<>();
        enrollmentRecord.put(Constants.PROGRESS, 50);
        enrollmentRecord.put("userid", "user123");
        enrollmentRecord.put(Constants.COURSEID, "internal-course-123");
        cassandraData.add(enrollmentRecord);
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), any(), any()))
                .thenReturn(cassandraData);

        Method method = CornellSchedulerService.class.getDeclaredMethod(
                "callEnrollmentAPI", String.class, String.class, JsonNode.class);
        method.setAccessible(true);

        assertDoesNotThrow(() -> method.invoke(cornellSchedulerService, partnerCode, partnerId, contentData));

        verify(kafkaProducer, times(1)).push(anyString(), any());
        verify(payloadValidation, times(1)).validatePayload(anyString(), any());
        verify(dataTransformUtility, times(1)).callCiosReadApi(anyString(), anyString());
        verify(cassandraOperation, times(1)).getRecordsByProperties(anyString(), anyString(), any(), any());
    }

    @Test
    void testCallEnrollmentAPI_courseAlreadyCompleted() throws Exception {
        String partnerCode = "cornell";
        String partnerId = "partner-id-123";

        ObjectNode contentData = realObjectMapper.createObjectNode();
        contentData.put("courseid", "ext-course-123");
        contentData.put("userid", "user123@example.com");
        contentData.put("completedon", "1706359845000");

        ObjectNode ciosResponse = realObjectMapper.createObjectNode();
        ObjectNode content = realObjectMapper.createObjectNode();
        content.put("contentId", "internal-course-123");
        ciosResponse.set("content", content);
        when(dataTransformUtility.callCiosReadApi(anyString(), anyString())).thenReturn(ciosResponse);

        List<Map<String, Object>> cassandraData = new ArrayList<>();
        Map<String, Object> enrollmentRecord = new HashMap<>();
        enrollmentRecord.put(Constants.PROGRESS, 100);
        cassandraData.add(enrollmentRecord);
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), any(), any()))
                .thenReturn(cassandraData);

        Method method = CornellSchedulerService.class.getDeclaredMethod(
                "callEnrollmentAPI", String.class, String.class, JsonNode.class);
        method.setAccessible(true);

        assertDoesNotThrow(() -> method.invoke(cornellSchedulerService, partnerCode, partnerId, contentData));

        // Verify no kafka push for already completed course
        verify(kafkaProducer, never()).push(anyString(), any());
        verify(payloadValidation, never()).validatePayload(anyString(), any());

        // But other services should be called
        verify(dataTransformUtility, times(1)).callCiosReadApi(anyString(), anyString());
        verify(cassandraOperation, times(1)).getRecordsByProperties(anyString(), anyString(), any(), any());
    }

    @Test
    void testCallEnrollmentAPI_userNotEnrolled() throws Exception {
        String partnerCode = "cornell";
        String partnerId = "partner-id-123";

        ObjectNode contentData = realObjectMapper.createObjectNode();
        contentData.put("courseid", "ext-course-123");
        contentData.put("userid", "user123@example.com");
        contentData.put("completedon", "1706359845000");

        ObjectNode ciosResponse = realObjectMapper.createObjectNode();
        ObjectNode content = realObjectMapper.createObjectNode();
        content.put("contentId", "internal-course-123");
        ciosResponse.set("content", content);
        when(dataTransformUtility.callCiosReadApi(anyString(), anyString())).thenReturn(ciosResponse);

        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), any(), any()))
                .thenReturn(new ArrayList<>());

        Method method = CornellSchedulerService.class.getDeclaredMethod(
                "callEnrollmentAPI", String.class, String.class, JsonNode.class);
        method.setAccessible(true);

        assertDoesNotThrow(() -> method.invoke(cornellSchedulerService, partnerCode, partnerId, contentData));

        // Verify no kafka push when user is not enrolled
        verify(kafkaProducer, never()).push(anyString(), any());
        verify(payloadValidation, never()).validatePayload(anyString(), any());

        // Verify services were called up to enrollment check
        verify(dataTransformUtility, times(1)).callCiosReadApi(anyString(), anyString());
        verify(cassandraOperation, times(1)).getRecordsByProperties(anyString(), anyString(), any(), any());
    }

    @Test
    void testCallEnrollmentAPI_exception() throws Exception {
        String partnerCode = "cornell";
        String partnerId = "partner-id-123";

        ObjectNode contentData = realObjectMapper.createObjectNode();
        contentData.put("courseid", "ext-course-123");
        contentData.put("userid", "user123@example.com");

        when(dataTransformUtility.callCiosReadApi(anyString(), anyString()))
                .thenThrow(new RuntimeException("API call failed"));

        Method method = CornellSchedulerService.class.getDeclaredMethod(
                "callEnrollmentAPI", String.class, String.class, JsonNode.class);
        method.setAccessible(true);

        Exception exception = assertThrows(Exception.class, () ->
            method.invoke(cornellSchedulerService, partnerCode, partnerId, contentData)
        );

        // Verify exception was thrown
        assertNotNull(exception);
        assertNotNull(exception.getCause());
        assertTrue(exception.getCause() instanceof RuntimeException);
        assertEquals("API call failed", exception.getCause().getMessage());

        // Verify no kafka push happened
        verify(kafkaProducer, never()).push(anyString(), any());
        verify(payloadValidation, never()).validatePayload(anyString(), any());
    }
}
