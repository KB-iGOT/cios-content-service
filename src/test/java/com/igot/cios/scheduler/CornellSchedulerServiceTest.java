package com.igot.cios.scheduler;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
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
import org.springframework.http.*;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.client.RestTemplate;

import java.util.*;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class CornellSchedulerServiceTest {

    private CornellSchedulerService service;

    @Mock private ObjectMapper objectMapper;
    @Mock private KafkaProducer kafkaProducer;
    @Mock private PayloadValidation payloadValidation;
    @Mock private RestTemplate restTemplate;
    @Mock private CbServerProperties cbProps;
    @Mock private CassandraOperation cassandraOperation;
    @Mock private DataTransformUtility dataTransformUtility;

    private final ObjectMapper realMapper = new ObjectMapper();

    @BeforeEach
    void setUp() {
        service = new CornellSchedulerService();

        ReflectionTestUtils.setField(service, "objectMapper", objectMapper);
        ReflectionTestUtils.setField(service, "kafkaProducer", kafkaProducer);
        ReflectionTestUtils.setField(service, "payloadValidation", payloadValidation);
        ReflectionTestUtils.setField(service, "restTemplate", restTemplate);
        ReflectionTestUtils.setField(service, "cbServerProperties", cbProps);
        ReflectionTestUtils.setField(service, "cassandraOperation", cassandraOperation);
        ReflectionTestUtils.setField(service, "dataTransformUtility", dataTransformUtility);

        // Common property stubs
        lenient().when(cbProps.getCornellEnrollmentServiceCode()).thenReturn("svc-code");
        lenient().when(cbProps.getServiceLocatorHost()).thenReturn("http://sl-host");
        lenient().when(cbProps.getServiceLocatorFixedUrl()).thenReturn("/fixed");
        lenient().when(cbProps.getCornellDateRange()).thenReturn(1);
        lenient().when(cbProps.getCornellEnrollmentListLimit()).thenReturn("100");
        lenient().when(cbProps.getCornellEnrollmentListCourseType()).thenReturn("MOOC");
        lenient().when(cbProps.getTopic()).thenReturn("cornell-topic");

        // Prevent NPE when converting response bodies to JsonNode
        lenient().when(objectMapper.valueToTree(any()))
                .thenAnswer(inv -> {
                    Object arg = inv.getArgument(0);
                    return arg == null ? realMapper.nullNode() : realMapper.valueToTree(arg);
                });

        // Field is accessed directly in code (not a getter)
        ReflectionTestUtils.setField(cbProps, "cornellPartnerCode", "partner-xyz");
    }

    // ---------- Private helper coverage ----------

    @Test
    void updateDateFormatFromTimestamp_formatsAs_MM_dd_yyyy() {
        String formatted = (String) ReflectionTestUtils.invokeMethod(
                service, "updateDateFormatFromTimestamp", 1700000000000L);
        assertNotNull(formatted);
        assertTrue(Pattern.compile("\\d{2}/\\d{2}/\\d{4}").matcher(formatted).matches());
    }

    @Test
    void formUrlMapForEnrollment_buildsExpectedKeysAndRange() {
        Map<String, String> map = (Map<String, String>) ReflectionTestUtils.invokeMethod(
                service, "formUrlMapForEnrollment");

        assertEquals("0", map.get("offset"));
        assertEquals("100", map.get("limit"));
        assertEquals("MOOC", map.get("course_type"));

        String range = map.get("completion_range");
        assertNotNull(range);
        assertEquals(17, range.length()); // yyyyMMdd:yyyyMMdd
        assertTrue(range.contains(":"));
    }

    // ---------- Public flow: positive ----------

    @Test
    void loadCornellEnrollment_success_processesRecords_andPublishes() throws Exception {
        when(objectMapper.writeValueAsString(any())).thenReturn("{}");

        List<Map<String, Object>> body = List.of(
                Map.of(
                        "courseid", "ext-1",
                        Constants.USER_ID, "alice@example.com",
                        "completedon", "1700000000000"
                )
        );
        ResponseEntity<Object> ok = ResponseEntity.ok(body);
        when(restTemplate.exchange(anyString(), eq(HttpMethod.POST), ArgumentMatchers.<HttpEntity<?>>any(), eq(Object.class)))
                .thenReturn(ok);

        ObjectNode partnerInfo = realMapper.createObjectNode().put("id", "partner-id-1");
        when(dataTransformUtility.fetchPartnerInfoUsingApi("partner-xyz")).thenReturn(partnerInfo);

        ObjectNode readApiRes = realMapper.createObjectNode();
        readApiRes.set("content", realMapper.createObjectNode().put("contentId", "int-1"));
        when(dataTransformUtility.callCiosReadApi("ext-1", "partner-id-1")).thenReturn(readApiRes);

        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), anyMap(), isNull()))
                .thenReturn(Collections.emptyList());

        JsonNode result = service.loadCornellEnrollment();
        assertNotNull(result);

        verify(objectMapper, times(1)).writeValueAsString(any());
        verify(restTemplate, times(1)).exchange(anyString(), eq(HttpMethod.POST), any(HttpEntity.class), eq(Object.class));
        verify(dataTransformUtility, times(1)).fetchPartnerInfoUsingApi("partner-xyz");
        verify(dataTransformUtility, times(1)).callCiosReadApi("ext-1", "partner-id-1");
        verify(payloadValidation, times(1)).validatePayload(eq(Constants.PROGRESS_DATA_VALIDATION_FILE), any());
        verify(kafkaProducer, times(1)).push(eq("cornell-topic"), any());
    }

    // ---------- Public flow: negatives ----------

    @Test
    void loadCornellEnrollment_whenSerializationFails_throwsCiosContentException() throws Exception {
        when(objectMapper.writeValueAsString(any()))
                .thenThrow(new JsonProcessingException("ser-fail") {});

        CiosContentException ex = assertThrows(CiosContentException.class, () -> service.loadCornellEnrollment());
        assertNotNull(ex.getMessage());
    }

    @Test
    void performEnrollmentCall_whenRestReturnsNon2xx_throwsCiosContentException() throws Exception {
        when(objectMapper.writeValueAsString(any())).thenReturn("{}");
        ResponseEntity<Object> err = ResponseEntity.status(502).body(null);
        when(restTemplate.exchange(anyString(), eq(HttpMethod.POST), ArgumentMatchers.<HttpEntity<?>>any(), eq(Object.class)))
                .thenReturn(err);

        assertThrows(CiosContentException.class, () -> service.loadCornellEnrollment());
        verify(restTemplate, times(1)).exchange(anyString(), eq(HttpMethod.POST), any(HttpEntity.class), eq(Object.class));
    }

    @Test
    void performEnrollmentCall_whenExistingEnrollment_skipPublish() throws Exception {
        when(objectMapper.writeValueAsString(any())).thenReturn("{}");

        List<Map<String, Object>> body = List.of(
                Map.of(
                        "courseid", "ext-2",
                        Constants.USER_ID, "bob@example.com",
                        "completedon", "1700000000000"
                )
        );
        when(restTemplate.exchange(anyString(), eq(HttpMethod.POST), ArgumentMatchers.<HttpEntity<?>>any(), eq(Object.class)))
                .thenReturn(ResponseEntity.ok(body));

        ObjectNode partnerInfo = realMapper.createObjectNode().put("id", "partner-id-2");
        when(dataTransformUtility.fetchPartnerInfoUsingApi("partner-xyz")).thenReturn(partnerInfo);

        ObjectNode readApiRes = realMapper.createObjectNode();
        readApiRes.set("content", realMapper.createObjectNode().put("contentId", "int-2"));
        when(dataTransformUtility.callCiosReadApi("ext-2", "partner-id-2")).thenReturn(readApiRes);

        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), anyMap(), isNull()))
                .thenReturn(List.of(Map.of("dummy", "val"))); // existing

        JsonNode result = service.loadCornellEnrollment();
        assertNotNull(result);

        verify(payloadValidation, never()).validatePayload(anyString(), any());
        verify(kafkaProducer, never()).push(anyString(), any());
    }

    @Test
    void performEnrollmentCall_whenCallCiosReadApiThrows_wrapsIntoCiosContentException() throws Exception {
        when(objectMapper.writeValueAsString(any())).thenReturn("{}");

        List<Map<String, Object>> body = List.of(
                Map.of(
                        "courseid", "ext-err",
                        Constants.USER_ID, "eve@example.com",
                        "completedon", "1700000000000"
                )
        );
        when(restTemplate.exchange(anyString(), eq(HttpMethod.POST), ArgumentMatchers.<HttpEntity<?>>any(), eq(Object.class)))
                .thenReturn(ResponseEntity.ok(body));

        ObjectNode partnerInfo = realMapper.createObjectNode().put("id", "partner-id-err");
        when(dataTransformUtility.fetchPartnerInfoUsingApi("partner-xyz")).thenReturn(partnerInfo);

        when(dataTransformUtility.callCiosReadApi(anyString(), anyString()))
                .thenThrow(new RuntimeException("backend failure"));

        assertThrows(CiosContentException.class, () -> service.loadCornellEnrollment());
        verify(kafkaProducer, never()).push(anyString(), any());
    }
}