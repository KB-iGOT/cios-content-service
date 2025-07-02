package com.igot.cios.scheduler;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


@Slf4j
@Service
public class CdacSchedulerService {
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

    private void callEnrollmentAPI(String partnerCode, String partnerId, JsonNode transformData) {
        try {
            log.info("CdacSchedulerService::callEnrollmentAPI");
            String extCourseId = transformData.get(Constants.COURSEID).asText();
            JsonNode result = dataTransformUtility.callCiosReadApi(extCourseId,partnerId);
            String courseId = result.path(Constants.CONTENT).get(Constants.CONTENTID).asText();
            String[] parts = transformData.get(Constants.USER_ID).asText().split("@");
            ((ObjectNode) transformData).put(Constants.USER_ID, parts[0]);
            String userId = transformData.get(Constants.USER_ID).asText();
            log.info("courseId  and userid {} {}", courseId, userId);
            Map<String, Object> propertyMap = new HashMap<>();
            propertyMap.put(Constants.USER_ID, userId);
            propertyMap.put(Constants.COURSEID, courseId);
            propertyMap.put(Constants.PROGRESS, 100);
            List<Map<String, Object>> listOfMasterData = cassandraOperation.getRecordsByProperties(Constants.KEYSPACE_SUNBIRD_COURSES, Constants.TABLE_USER_EXTERNAL_ENROLMENTS, propertyMap, null);
            if (CollectionUtils.isEmpty(listOfMasterData)) {
                String formatedDate = updateDateFormatFromInputString(transformData.get("completedon").asText());
                ((ObjectNode) transformData).put(Constants.COMPLETED_ON, formatedDate);
                ((ObjectNode) transformData).put(Constants.PARTNERCODE, partnerCode);
                ((ObjectNode) transformData).put(Constants.PARTNERID, partnerId);
                JsonNode fieldNode = transformData.get(Constants.COURSEID);
                if (fieldNode != null && fieldNode.isInt()) {
                    ((ObjectNode) transformData).put(Constants.COURSEID, String.valueOf(fieldNode.intValue()));
                }
                payloadValidation.validatePayload(Constants.PROGRESS_DATA_VALIDATION_FILE, transformData);
                kafkaProducer.push(cbServerProperties.getTopic(), transformData);
            } else {
                log.info("Progress updated 100 for user {}", userId);
            }
        } catch (Exception e) {
            log.error("error while processing", e);
            throw new CiosContentException(Constants.ERROR, e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    private String updateDateFormatFromInputString(String completedon) {
        try {
            DateTimeFormatter inputFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
            LocalDateTime localDateTime = LocalDateTime.parse(completedon, inputFormatter);

            ZonedDateTime utcZonedDateTime = localDateTime
                    .atZone(ZoneId.of("Asia/Kolkata"))
                    .withZoneSameInstant(ZoneId.of("UTC"));

            DateTimeFormatter outputFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
                    .withZone(ZoneId.of("UTC"));
            return outputFormatter.format(utcZonedDateTime);  // Format output
        } catch (RuntimeException e) {
            throw new RuntimeException("Invalid date format: " + completedon, e);
        }
    }

    public JsonNode loadCdacEnrollment() {
        log.info("CdacSchedulerService :: loadCdacEnrollment()");
        RequestBodyDTO requestBodyDTO = new RequestBodyDTO();
        requestBodyDTO.setServiceCode(cbServerProperties.getCdacEnrollmentServiceCode());
        requestBodyDTO.setUrlMap(formUrlMapForEnrollment());
        requestBodyDTO.setHeaderMap(formHeaderMap());
        String payload = null;
        try {
            payload = objectMapper.writeValueAsString(requestBodyDTO);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        return performEnrollmentCall(cbServerProperties.cdacPartnerCode,payload);
    }

    private Map<String, String> formHeaderMap() {
        Map<String, String> headerMap = new HashMap<>();
        headerMap.put("API-Key", cbServerProperties.getCdacApiKey());
        return headerMap;
    }

    private Map<String, String> formUrlMapForEnrollment() {
        DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        LocalDateTime now = LocalDateTime.now();
        LocalDateTime fromDateTime = now.minusDays(cbServerProperties.getCornellDateRange());
        String fromdate = fromDateTime.format(FORMATTER);
        String todate = now.format(FORMATTER);
        Map<String, String> urlMap = new HashMap<>();
        urlMap.put(Constants.FROM_DATE, fromdate);
        urlMap.put(Constants.TO_DATE, todate);
        return urlMap;
    }

    private JsonNode performEnrollmentCall(String partnerCode, String requestBody) {
        log.info("CdacSchedulerService :: performEnrollmentCall partnerCode {} and requestBody {}", partnerCode,requestBody);
        String url = cbServerProperties.getServiceLocatorHost() + cbServerProperties.getServiceLocatorFixedUrl();
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
            JsonNode jsonData = objectMapper.valueToTree(response.getBody());
            if(!jsonData.isMissingNode()){
                JsonNode contentPartnerResponse = dataTransformUtility.fetchPartnerInfoUsingApi(partnerCode);
                String partnerId = contentPartnerResponse.get("id").asText();
                jsonData.forEach(
                        eachContentData -> {
                            callEnrollmentAPI(partnerCode, partnerId, eachContentData);
                        });
            }else{
                log.error("Failed to retrieve response data: for partner code {}", partnerCode);
            }
            return jsonData;
        } else {
            throw new RuntimeException("Failed to retrieve externalId. Status code: " + response.getStatusCodeValue());
        }
    }
}
