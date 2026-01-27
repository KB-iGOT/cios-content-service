package com.igot.cios.scheduler;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
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

        assertNotNull(result);
        assertTrue(result.matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}"));
    }

    @Test
    void testUpdateDateFormatFromTimestamp_nullTimestamp() throws Exception {
        Method method = CornellSchedulerService.class.getDeclaredMethod(
                "updateDateFormatFromTimestamp", Long.class);
        method.setAccessible(true);

        assertThrows(Exception.class, () -> method.invoke(cornellSchedulerService, (Long) null));
    }

    @Test
    void testLoadCornellEnrollment_success() throws JsonProcessingException {
        when(cbServerProperties.getCornellEnrollmentServiceCode()).thenReturn("CORNELL_SERVICE");
        when(cbServerProperties.getCornellEnrollmentListLimit()).thenReturn("100");
        when(cbServerProperties.getCornellEnrollmentListCourseType()).thenReturn("online");
        when(cbServerProperties.getCornellDateRange()).thenReturn(7);
        cbServerProperties.cornellPartnerCode = "cornell";
        when(cbServerProperties.getServiceLocatorHost()).thenReturn("http://localhost");
        when(cbServerProperties.getServiceLocatorFixedUrl()).thenReturn("/api/v1/service");

        String payload = "{\"serviceCode\":\"CORNELL_SERVICE\"}";
        when(objectMapper.writeValueAsString(any())).thenReturn(payload);

        ObjectNode responseNode = realObjectMapper.createObjectNode();
        ArrayNode enrollmentsNode = realObjectMapper.createArrayNode();
        responseNode.set(Constants.ENROLLMENTS, enrollmentsNode);

        when(objectMapper.valueToTree(any())).thenReturn(responseNode);
        when(restTemplate.exchange(anyString(), any(HttpMethod.class), any(HttpEntity.class), eq(Object.class)))
                .thenReturn(ResponseEntity.ok(responseNode));

        JsonNode partnerInfo = realObjectMapper.createObjectNode().put(Constants.ID, "partner-id-123");
        when(dataTransformUtility.fetchPartnerInfoUsingApi(anyString())).thenReturn(partnerInfo);

        JsonNode result = cornellSchedulerService.loadCornellEnrollment();

        assertNotNull(result);
        verify(objectMapper, times(1)).writeValueAsString(any());
        verify(restTemplate, times(1)).exchange(anyString(), any(HttpMethod.class), any(HttpEntity.class), eq(Object.class));
    }

    @Test
    void testLoadCornellEnrollment_jsonProcessingException() throws JsonProcessingException {
        when(cbServerProperties.getCornellEnrollmentServiceCode()).thenReturn("CORNELL_SERVICE");
        when(cbServerProperties.getCornellEnrollmentListLimit()).thenReturn("100");
        when(cbServerProperties.getCornellEnrollmentListCourseType()).thenReturn("online");
        when(cbServerProperties.getCornellDateRange()).thenReturn(7);

        when(objectMapper.writeValueAsString(any())).thenThrow(new JsonProcessingException("Error") {});

        assertThrows(RuntimeException.class, () -> cornellSchedulerService.loadCornellEnrollment());
    }

    @Test
    void testFormUrlMapForEnrollment() throws Exception {
        when(cbServerProperties.getCornellDateRange()).thenReturn(7);
        when(cbServerProperties.getCornellEnrollmentListLimit()).thenReturn("100");
        when(cbServerProperties.getCornellEnrollmentListCourseType()).thenReturn("online");

        Method method = CornellSchedulerService.class.getDeclaredMethod("formUrlMapForEnrollment");
        method.setAccessible(true);

        Map<String, String> result = (Map<String, String>) method.invoke(cornellSchedulerService);

        assertNotNull(result);
        assertEquals("0", result.get("offset"));
        assertEquals("100", result.get("limit"));
        assertEquals("online", result.get("course_type"));
        assertTrue(result.containsKey("completion_range"));
        assertTrue(result.get("completion_range").matches("\\d{8}:\\d{8}"));
    }

    @Test
    void testPerformEnrollmentCall_success() throws Exception {
        String partnerCode = "cornell";
        String requestBody = "{\"serviceCode\":\"CORNELL_SERVICE\"}";

        when(cbServerProperties.getServiceLocatorHost()).thenReturn("http://localhost");
        when(cbServerProperties.getServiceLocatorFixedUrl()).thenReturn("/api/v1/service");

        ObjectNode responseNode = realObjectMapper.createObjectNode();
        ArrayNode enrollmentsArray = realObjectMapper.createArrayNode();
        ObjectNode enrollment = realObjectMapper.createObjectNode();
        enrollment.put("courseid", "course-123");
        enrollment.put("userid", "user@example.com");
        enrollment.put("completedon", "1706359845000");
        enrollmentsArray.add(enrollment);
        responseNode.set(Constants.ENROLLMENTS, enrollmentsArray);

        when(objectMapper.valueToTree(any())).thenReturn(responseNode);
        when(restTemplate.exchange(anyString(), any(HttpMethod.class), any(HttpEntity.class), eq(Object.class)))
                .thenReturn(ResponseEntity.ok(responseNode));

        ObjectNode partnerInfo = realObjectMapper.createObjectNode();
        partnerInfo.put(Constants.ID, "partner-id-123");
        ObjectNode transformSpec = realObjectMapper.createObjectNode();
        partnerInfo.set(Constants.TRANSFORM_PROGRESS_JSON, transformSpec);
        when(dataTransformUtility.fetchPartnerInfoUsingApi(anyString())).thenReturn(partnerInfo);

        List<Object> contentJson = new ArrayList<>();
        when(objectMapper.convertValue(any(JsonNode.class), any(TypeReference.class))).thenReturn(contentJson);

        ObjectNode transformedData = realObjectMapper.createObjectNode();
        transformedData.put("courseid", "course-123");
        transformedData.put("userid", "user@example.com");
        transformedData.put("completedon", "1706359845000");
        when(dataTransformUtility.transformData(any(), any())).thenReturn(transformedData);

        ObjectNode ciosResponse = realObjectMapper.createObjectNode();
        ObjectNode content = realObjectMapper.createObjectNode();
        content.put("contentId", "internal-course-123");
        ciosResponse.set("content", content);
        when(dataTransformUtility.callCiosReadApi(anyString(), anyString())).thenReturn(ciosResponse);

        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), any(), any()))
                .thenReturn(new ArrayList<>());

        Method method = CornellSchedulerService.class.getDeclaredMethod(
                "performEnrollmentCall", String.class, String.class);
        method.setAccessible(true);

        JsonNode result = (JsonNode) method.invoke(cornellSchedulerService, partnerCode, requestBody);

        assertNotNull(result);
        verify(restTemplate, times(1)).exchange(anyString(), any(HttpMethod.class), any(HttpEntity.class), eq(Object.class));
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

        assertThrows(Exception.class, () -> method.invoke(cornellSchedulerService, partnerCode, requestBody));
    }

    @Test
    void testCallEnrollmentAPI_successfulFlow() throws Exception {
        String partnerCode = "cornell";
        String partnerId = "partner-id-123";

        ObjectNode contentData = realObjectMapper.createObjectNode();
        contentData.put("courseid", "ext-course-123");
        contentData.put("userid", "user123@example.com");
        contentData.put("completedon", "1706359845000");
        contentData.put("status", "completed");

        when(cbServerProperties.getTopic()).thenReturn("test-topic");

        ObjectNode partnerInfo = realObjectMapper.createObjectNode();
        ObjectNode transformSpec = realObjectMapper.createObjectNode();
        partnerInfo.set(Constants.TRANSFORM_PROGRESS_JSON, transformSpec);
        when(dataTransformUtility.fetchPartnerInfoUsingApi(partnerId)).thenReturn(partnerInfo);

        List<Object> contentJson = new ArrayList<>();
        when(objectMapper.convertValue(any(JsonNode.class), any(TypeReference.class))).thenReturn(contentJson);

        ObjectNode transformedData = realObjectMapper.createObjectNode();
        transformedData.put("courseid", "ext-course-123");
        transformedData.put("userid", "user123@example.com");
        transformedData.put("completedon", "1706359845000");
        when(dataTransformUtility.transformData(any(), any())).thenReturn(transformedData);

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
    }

    @Test
    void testCallEnrollmentAPI_courseAlreadyCompleted() throws Exception {
        String partnerCode = "cornell";
        String partnerId = "partner-id-123";

        ObjectNode contentData = realObjectMapper.createObjectNode();
        contentData.put("courseid", "ext-course-123");
        contentData.put("userid", "user123@example.com");
        contentData.put("completedon", "1706359845000");

        ObjectNode partnerInfo = realObjectMapper.createObjectNode();
        ObjectNode transformSpec = realObjectMapper.createObjectNode();
        partnerInfo.set(Constants.TRANSFORM_PROGRESS_JSON, transformSpec);
        when(dataTransformUtility.fetchPartnerInfoUsingApi(partnerId)).thenReturn(partnerInfo);

        List<Object> contentJson = new ArrayList<>();
        when(objectMapper.convertValue(any(JsonNode.class), any(TypeReference.class))).thenReturn(contentJson);

        ObjectNode transformedData = realObjectMapper.createObjectNode();
        transformedData.put("courseid", "ext-course-123");
        transformedData.put("userid", "user123@example.com");
        transformedData.put("completedon", "1706359845000");
        when(dataTransformUtility.transformData(any(), any())).thenReturn(transformedData);

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

        verify(kafkaProducer, never()).push(anyString(), any());
    }

    @Test
    void testCallEnrollmentAPI_userNotEnrolled() throws Exception {
        String partnerCode = "cornell";
        String partnerId = "partner-id-123";

        ObjectNode contentData = realObjectMapper.createObjectNode();
        contentData.put("courseid", "ext-course-123");
        contentData.put("userid", "user123@example.com");
        contentData.put("completedon", "1706359845000");

        ObjectNode partnerInfo = realObjectMapper.createObjectNode();
        ObjectNode transformSpec = realObjectMapper.createObjectNode();
        partnerInfo.set(Constants.TRANSFORM_PROGRESS_JSON, transformSpec);
        when(dataTransformUtility.fetchPartnerInfoUsingApi(partnerId)).thenReturn(partnerInfo);

        List<Object> contentJson = new ArrayList<>();
        when(objectMapper.convertValue(any(JsonNode.class), any(TypeReference.class))).thenReturn(contentJson);

        ObjectNode transformedData = realObjectMapper.createObjectNode();
        transformedData.put("courseid", "ext-course-123");
        transformedData.put("userid", "user123@example.com");
        transformedData.put("completedon", "1706359845000");
        when(dataTransformUtility.transformData(any(), any())).thenReturn(transformedData);

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

        verify(kafkaProducer, never()).push(anyString(), any());
    }

    @Test
    void testCallEnrollmentAPI_exception() throws Exception {
        String partnerCode = "cornell";
        String partnerId = "partner-id-123";

        ObjectNode contentData = realObjectMapper.createObjectNode();
        contentData.put("courseid", "ext-course-123");

        when(dataTransformUtility.fetchPartnerInfoUsingApi(partnerId))
                .thenThrow(new RuntimeException("API call failed"));

        Method method = CornellSchedulerService.class.getDeclaredMethod(
                "callEnrollmentAPI", String.class, String.class, JsonNode.class);
        method.setAccessible(true);

        assertThrows(Exception.class, () -> method.invoke(cornellSchedulerService, partnerCode, partnerId, contentData));
    }
}
