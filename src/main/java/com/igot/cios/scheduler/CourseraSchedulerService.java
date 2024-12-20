package com.igot.cios.scheduler;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.igot.cios.dto.RequestBodyDTO;
import com.igot.cios.exception.CiosContentException;
import com.igot.cios.kafka.KafkaProducer;
import com.igot.cios.plugins.DataTransformUtility;
import com.igot.cios.util.CbServerProperties;
import com.igot.cios.util.Constants;
import com.igot.cios.util.PayloadValidation;
import com.igot.cios.util.transactional.cassandrautils.CassandraOperation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.web.client.RestTemplate;

import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.*;

@Slf4j
@Service
public class CourseraSchedulerService implements SchedulerInterface {
    @Autowired
    private ObjectMapper objectMapper;
    @Autowired
    KafkaProducer kafkaProducer;
    @Autowired
    PayloadValidation payloadValidation;
    @Autowired
    RestTemplate restTemplate;
    @Autowired
    private CbServerProperties cbServerProperties;
    @Autowired
    private CassandraOperation cassandraOperation;
    @Autowired
    private DataTransformUtility dataTransformUtility;

    @Override
    public JsonNode loadEnrollment() {
        log.info("Coursera Scheduler Service::loadEnrollment()");
        int start = 0;
        int limit = cbServerProperties.getCourseraEnrollmentListLimit();
        String payload = null;
        ArrayNode allEnrollmentData = objectMapper.createArrayNode();
        int total = 0;
        while (start == 0 || start < total) {
            RequestBodyDTO requestBodyDTO = new RequestBodyDTO();
            requestBodyDTO.setServiceCode(cbServerProperties.getCourseraEnrollmentServiceCode());
            requestBodyDTO.setUrlMap(formUrlMapForEnrollment(start, limit));
            try {
                payload = objectMapper.writeValueAsString(requestBodyDTO);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
            JsonNode response = performEnrollmentCall(cbServerProperties.courseraPartnerCode, payload);
            total = response.get("count").asInt();
            JsonNode enrollmentData = response.get("data");
            if (enrollmentData != null && enrollmentData.isArray()) {
                allEnrollmentData.addAll((ArrayNode) enrollmentData);
            }
            start += limit;
        }
        allEnrollmentData.forEach(eachContentData -> {
            JsonNode contentPartnerInfo = dataTransformUtility.fetchPartnerInfoUsingApi(cbServerProperties.courseraPartnerCode);
            String partnerId = contentPartnerInfo.get("id").asText();
            callEnrollmentAPI(cbServerProperties.courseraPartnerCode, partnerId, eachContentData);
        });
        return allEnrollmentData;
    }

    private Map<String, String> formUrlMapForEnrollment(int start, int limit) {
        log.info("Coursera Scheduler Service::formUrlMapForEnrollment");
        LocalDateTime currentDateTime = LocalDateTime.now();
        LocalDateTime previousDate = currentDateTime.minusDays(cbServerProperties.getCourseraDateRange());
        ZonedDateTime zonedDateTime = previousDate.atZone(ZoneId.of("UTC"));
        long timestamp = zonedDateTime.toInstant().getEpochSecond();
        long currentMillis = System.currentTimeMillis();
        long timestampWithoutMillis = (currentMillis / 1000) * 1000;
        Map<String, String> urlMap = new HashMap<>();
        urlMap.put("limit", String.valueOf(cbServerProperties.getCourseraEnrollmentListLimit()));
        urlMap.put("start", String.valueOf(start));
        urlMap.put(cbServerProperties.getCourseraDateBefore(), String.valueOf(timestampWithoutMillis));
        urlMap.put(cbServerProperties.getCourseraDateAfter(), String.valueOf(timestamp));
        return urlMap;
    }

    @Override
    public JsonNode performEnrollmentCall(String partnerCode, String requestBody) {
        log.info("coursera scheduler service: performEnrollmentCall for partner code {}, request body {}", partnerCode,requestBody);
        String url = cbServerProperties.getServiceLocatorHost() + cbServerProperties.getServiceLocatorFixedUrl();
        log.info("CourseraSchedulerService :: performEnrollmentCall url {}", url);
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<Object> entity = new HttpEntity<>(requestBody, headers);
        ResponseEntity<Object> response = restTemplate.exchange(
                url,
                HttpMethod.POST,
                entity,
                Object.class
        );
        if (response.getStatusCode().is2xxSuccessful()) {
            log.info("CourseraSchedulerService :: performEnrollmentCall response {}");
            JsonNode jsonData = objectMapper.valueToTree(response.getBody());
            if(!jsonData.isMissingNode()){
                return jsonData;
            }else{
                log.error("Failed to retrieve response data: for partner code {}", partnerCode);
            }
        } else {
            throw new RuntimeException("Failed to retrieve externalId. Status code: " + response.getStatusCodeValue());
        }
        return null;
    }

    @Override
    public void callEnrollmentAPI(String partnerCode, String partnerId, JsonNode transformData) {
        try {
            log.info("CourseSchedulerService::callCourseraEnrollmentAPI");
            if (transformData.get("contentType").asText().equalsIgnoreCase(cbServerProperties.getCourseraEnrollmentListCourseType()) && transformData.get("isCompleted").asBoolean()) {
                String extCourseId = transformData.get("courseid").asText();
                JsonNode result = dataTransformUtility.callCiosReadApi(extCourseId, partnerId);
                String courseId = result.path("content").get("contentId").asText();
                String[] parts = transformData.get("userid").asText().split("@");
                ((ObjectNode) transformData).put("userid", parts[0]);
                String userId = transformData.get("userid").asText();
                log.info("courseId  and userid {} {}", courseId, userId);
                Map<String, Object> propertyMap = new HashMap<>();
                propertyMap.put("userid", userId);
                propertyMap.put("courseid", courseId);
                propertyMap.put("progress", 100);
                List<Map<String, Object>> listOfMasterData = cassandraOperation.getRecordsByProperties(Constants.KEYSPACE_SUNBIRD_COURSES, Constants.TABLE_USER_EXTERNAL_ENROLMENTS, propertyMap, null);
                if (CollectionUtils.isEmpty(listOfMasterData)) {
                    Long date = Long.valueOf(transformData.get("completedon").asText());
                    String formatedDate = updateDateFormatFromTimestamp(date);
                    ((ObjectNode) transformData).put("completedon", formatedDate);
                    ((ObjectNode) transformData).put("partnerCode", partnerCode);
                    ((ObjectNode) transformData).put("partnerId", partnerId);
                    payloadValidation.validatePayload(Constants.PROGRESS_DATA_VALIDATION_FILE, transformData);
                    kafkaProducer.push(cbServerProperties.getTopic(), transformData);
                } else {
                    log.info("Progress updated 100 for user {}", userId);
                }
            }
        } catch (Exception e) {
            log.error("error while processing", e);
            throw new CiosContentException(Constants.ERROR, e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    private String updateDateFormatFromTimestamp(Long completedon) {
        Date date = new Date(completedon);
        SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy");
        return sdf.format(date);
    }
}
