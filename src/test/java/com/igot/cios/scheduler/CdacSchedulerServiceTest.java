// java
package com.igot.cios.scheduler;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.igot.cios.exception.CiosContentException;
import com.igot.cios.kafka.KafkaProducer;
import com.igot.cios.plugins.DataTransformUtility;
import com.igot.cios.util.CbServerProperties;
import com.igot.cios.util.Constants;
import com.igot.cios.util.PayloadValidation;
import com.igot.cios.util.transactional.cassandrautils.CassandraOperation;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.client.RestTemplate;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class CdacSchedulerServiceTest {

    private CdacSchedulerService service;

    @Mock
    private ObjectMapper objectMapper;

    @Mock
    private KafkaProducer kafkaProducer;

    @Mock
    private PayloadValidation payloadValidation;

    @Mock
    private RestTemplate restTemplate;

    @Mock
    private CbServerProperties cbProps;

    @Mock
    private CassandraOperation cassandraOperation;

    @Mock
    private DataTransformUtility dataTransformUtility;

    private final ObjectMapper realMapper = new ObjectMapper();

    @BeforeEach
    void setUp() throws Exception {
        service = new CdacSchedulerService();

        ReflectionTestUtils.setField(service, "objectMapper", objectMapper);
        ReflectionTestUtils.setField(service, "kafkaProducer", kafkaProducer);
        ReflectionTestUtils.setField(service, "payloadValidation", payloadValidation);
        ReflectionTestUtils.setField(service, "restTemplate", restTemplate);
        ReflectionTestUtils.setField(service, "cbServerProperties", cbProps);
        ReflectionTestUtils.setField(service, "cassandraOperation", cassandraOperation);
        ReflectionTestUtils.setField(service, "dataTransformUtility", dataTransformUtility);

        // Shared stubs marked lenient to avoid UnnecessaryStubbing in tests that don't use them
        lenient().when(cbProps.getCdacEnrollmentServiceCode()).thenReturn("svc-code");
        lenient().when(cbProps.getCdacApiKey()).thenReturn("api-key");
        lenient().when(cbProps.getServiceLocatorHost()).thenReturn("http://sl-host");
        lenient().when(cbProps.getServiceLocatorFixedUrl()).thenReturn("/fixed");
        lenient().when(cbProps.getCornellDateRange()).thenReturn(1);
        lenient().when(cbProps.getTopic()).thenReturn("cios-topic");

        // Prevent NPE in performEnrollmentCall: delegate valueToTree to a real mapper
        lenient().when(objectMapper.valueToTree(any()))
                .thenAnswer(inv -> {
                    Object arg = inv.getArgument(0);
                    return arg == null ? realMapper.nullNode() : realMapper.valueToTree(arg);
                });

        // Field used directly in code
        ReflectionTestUtils.setField(cbProps, "cdacPartnerCode", "partner-xyz");
    }

    @Test
    void updateDateFormatFromInputString_valid_convertsToUtcIso() {
        String input = "2023-03-10 15:30:00";
        String result = (String) ReflectionTestUtils.invokeMethod(service, "updateDateFormatFromInputString", input);
        assertNotNull(result);
        assertTrue(result.contains("T") && result.endsWith("Z"));
    }

    @Test
    void updateDateFormatFromInputString_invalid_throwsBadRequest() {
        String bad = "not-a-date";
        CiosContentException ex = assertThrows(CiosContentException.class, () ->
                ReflectionTestUtils.invokeMethod(service, "updateDateFormatFromInputString", bad));
        assertTrue(ex.getMessage().contains("Invalid date format"));
    }

    @Test
    void loadCdacEnrollment_success_processesEachRecord_andPushesToKafka() throws Exception {
        when(objectMapper.writeValueAsString(ArgumentMatchers.any())).thenReturn("{}");

        List<Map<String, Object>> body = List.of(
                Map.of(
                        Constants.COURSEID, "external-course-1",
                        Constants.USER_ID, "student@example.com",
                        "completedon", "2023-03-10 15:30:00"
                )
        );
        ResponseEntity<Object> okResponse = ResponseEntity.ok(body);
        when(restTemplate.exchange(
                anyString(), eq(HttpMethod.POST), ArgumentMatchers.<HttpEntity<?>>any(), eq(Object.class)))
                .thenReturn(okResponse);

        ObjectNode partnerNode = realMapper.createObjectNode();
        partnerNode.put("id", "partner-id-1");
        when(dataTransformUtility.fetchPartnerInfoUsingApi("partner-xyz")).thenReturn(partnerNode);

        ObjectNode readApiRes = realMapper.createObjectNode();
        ObjectNode contentNode = realMapper.createObjectNode();
        contentNode.put(Constants.CONTENTID, "internal-course-1");
        readApiRes.set(Constants.CONTENT, contentNode);
        when(dataTransformUtility.callCiosReadApi("external-course-1", "partner-id-1")).thenReturn(readApiRes);

        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), anyMap(), isNull()))
                .thenReturn(Collections.emptyList());

        service.loadCdacEnrollment();

        verify(objectMapper, times(1)).writeValueAsString(ArgumentMatchers.any());
        verify(restTemplate, times(1)).exchange(anyString(), eq(HttpMethod.POST), ArgumentMatchers.<HttpEntity<?>>any(), eq(Object.class));
        verify(dataTransformUtility, times(1)).fetchPartnerInfoUsingApi("partner-xyz");
        verify(dataTransformUtility, times(1)).callCiosReadApi("external-course-1", "partner-id-1");
        verify(payloadValidation, times(1)).validatePayload(eq(Constants.PROGRESS_DATA_VALIDATION_FILE), ArgumentMatchers.any());
        verify(kafkaProducer, times(1)).push(eq("cios-topic"), ArgumentMatchers.any());
    }

    @Test
    void loadCdacEnrollment_whenRestReturnsNon2xx_throwsCiosContentException() throws Exception {
        when(objectMapper.writeValueAsString(ArgumentMatchers.any())).thenReturn("{}");
        ResponseEntity<Object> err = ResponseEntity.status(500).body(null);
        when(restTemplate.exchange(anyString(), eq(HttpMethod.POST), ArgumentMatchers.<HttpEntity<?>>any(), eq(Object.class)))
                .thenReturn(err);

        CiosContentException ex = assertThrows(CiosContentException.class, () -> service.loadCdacEnrollment());
        assertTrue(ex.getMessage().contains("Status code: 500"));
        verify(restTemplate, times(1)).exchange(anyString(), eq(HttpMethod.POST), ArgumentMatchers.<HttpEntity<?>>any(), eq(Object.class));
    }

    @Test
    void loadCdacEnrollment_whenObjectMapperSerializationFails_throwsCiosContentException() throws Exception {
        when(objectMapper.writeValueAsString(ArgumentMatchers.any()))
                .thenThrow(new JsonProcessingException("ser-fail") {});
        CiosContentException ex = assertThrows(CiosContentException.class, () -> service.loadCdacEnrollment());
        assertTrue(ex.getMessage().contains("ser-fail"));
    }

    @Test
    void loadCdacEnrollment_whenCallEnrollmentAPI_throws_insideProcessing_isHandledAndWraps() throws Exception {
        when(objectMapper.writeValueAsString(ArgumentMatchers.any())).thenReturn("{}");

        List<Map<String, Object>> body = List.of(
                Map.of(
                        Constants.COURSEID, "external-course-err",
                        Constants.USER_ID, "student@example.com",
                        "completedon", "2023-03-10 15:30:00"
                )
        );
        when(restTemplate.exchange(anyString(), eq(HttpMethod.POST), ArgumentMatchers.<HttpEntity<?>>any(), eq(Object.class)))
                .thenReturn(ResponseEntity.ok(body));

        ObjectNode partnerNode = realMapper.createObjectNode();
        partnerNode.put("id", "partner-id-err");
        when(dataTransformUtility.fetchPartnerInfoUsingApi("partner-xyz")).thenReturn(partnerNode);

        when(dataTransformUtility.callCiosReadApi(anyString(), anyString()))
                .thenThrow(new RuntimeException("backend failure"));

        // Removed unused Cassandra stubbing to avoid UnnecessaryStubbingException

        RuntimeException ex = assertThrows(RuntimeException.class, () -> service.loadCdacEnrollment());
        assertTrue(ex.getMessage().contains("backend failure"));
    }
}