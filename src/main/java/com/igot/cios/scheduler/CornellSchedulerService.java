package com.igot.cios.scheduler;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.igot.cios.dto.RequestBodyDTO;
import com.igot.cios.dto.SBApiResponse;
import com.igot.cios.exception.CiosContentException;
import com.igot.cios.kafka.KafkaProducer;
import com.igot.cios.plugins.DataTransformUtility;
import com.igot.cios.util.CbServerProperties;
import com.igot.cios.util.Constants;
import com.igot.cios.util.PayloadValidation;
import com.igot.cios.util.transactional.cassandrautils.CassandraOperation;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.web.client.RestTemplate;
import com.fasterxml.jackson.core.type.TypeReference;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;


@Slf4j
@Service
@RequiredArgsConstructor
public class CornellSchedulerService{

    private final ObjectMapper objectMapper;
    private final KafkaProducer kafkaProducer;
    private final PayloadValidation payloadValidation;
    private final RestTemplate restTemplate;
    private final CbServerProperties cbServerProperties;
    private final CassandraOperation cassandraOperation;
    private final DataTransformUtility dataTransformUtility;

    private void callEnrollmentAPI(String partnerCode, String partnerId, JsonNode transformData) {
        try {
            log.info("CornellSchedulerService::callEnrollmentAPI");
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
            List<Map<String, Object>> listOfMasterData = cassandraOperation.getRecordsByProperties(Constants.KEYSPACE_SUNBIRD_COURSES, Constants.TABLE_USER_EXTERNAL_ENROLMENTS, propertyMap, null);
            if (!CollectionUtils.isEmpty(listOfMasterData)) {
                if (!listOfMasterData.get(0).get(Constants.PROGRESS).equals(100)) {
                Long date = Long.valueOf(transformData.get(Constants.COMPLETED_ON).asText());
                String formatedDate = updateDateFormatFromTimestamp(date);
                ((ObjectNode) transformData).put(Constants.COMPLETED_ON, formatedDate);
                ((ObjectNode) transformData).put(Constants.PARTNER_CODE, partnerCode);
                ((ObjectNode) transformData).put(Constants.PARTNER_ID, partnerId);
                payloadValidation.validatePayload(Constants.PROGRESS_DATA_VALIDATION_FILE, transformData);
                kafkaProducer.push(cbServerProperties.getTopic(), transformData);
                } else {
                    log.info("course already completed for user {} courseid {}", userId, courseId);
                }
            } else {
                log.info("Progress updated 100 for user {}", userId);
            }
        } catch (Exception e) {
            log.error("error while processing", e);
            throw new CiosContentException(Constants.ERROR, e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    private String updateDateFormatFromTimestamp(Long completedon) {
        Date date = new Date(completedon);
        SimpleDateFormat sdf = new SimpleDateFormat(Constants.CORNELL_DATE_FORMAT);
        return sdf.format(date);
    }

    public SBApiResponse loadCornellEnrollment() {
        log.info("CornellSchedulerService :: loadCornellEnrollment()");
        SBApiResponse apiResponse = SBApiResponse.createDefaultResponse("cornell.enrollment");

        try {
            int start = 0;
            int limit = cbServerProperties.getCornellEnrollmentListLimit();
            ArrayNode allEnrollmentData = objectMapper.createArrayNode();
            int total = 0;
            while (start == 0 || start < total) {
                RequestBodyDTO requestBodyDTO = new RequestBodyDTO();
                requestBodyDTO.setServiceCode(cbServerProperties.getCornellEnrollmentServiceCode());
                requestBodyDTO.setUrlMap(formUrlMapForEnrollment(start, limit));
                String payload = objectMapper.writeValueAsString(requestBodyDTO);
                JsonNode response = performEnrollmentCall(cbServerProperties.courseraPartnerCode, payload);
                total = response.path(Constants.COUNT).asInt();
                JsonNode enrollmentData = response.path(Constants.DATA);
                if (enrollmentData != null && !enrollmentData.isMissingNode() && enrollmentData.isArray()) {
                    allEnrollmentData.addAll((ArrayNode) enrollmentData);
                }
                start += limit;
            }
            JsonNode contentPartnerInfo = dataTransformUtility.fetchPartnerInfoUsingApi(cbServerProperties.cornellPartnerCode);
            String partnerId = contentPartnerInfo.get(Constants.ID).asText();
            allEnrollmentData.forEach(eachContentData -> {
                callEnrollmentAPI(cbServerProperties.cornellPartnerCode, partnerId, eachContentData);
            });
            apiResponse.setResponseCode(HttpStatus.OK);
            return apiResponse;
        } catch (Exception e) {
            log.error("Error in loadCornellEnrollment", e);
            apiResponse.getParams().setErrmsg("Failed to load Cornell enrollment: " + e.getMessage());
            apiResponse.getParams().setStatus(Constants.FAILED);
            apiResponse.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
            return apiResponse;
        }
    }

    private Map<String, String> formUrlMapForEnrollment(int start, int limit) {
        DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd");
        LocalDate today = LocalDate.now();
        LocalDate startDate = today.minusDays(cbServerProperties.getCornellDateRange()); // Adjust the range as needed
        String completionRange = startDate.format(FORMATTER) + ":" + today.format(FORMATTER);
        log.info("Completion Range {}", completionRange);
        Map<String, String> urlMap = new HashMap<>();
        urlMap.put("offset", String.valueOf(start));
        urlMap.put("limit", String.valueOf(limit));
        urlMap.put("course_type", cbServerProperties.getCornellEnrollmentListCourseType());
        urlMap.put("completion_range", completionRange);
        return urlMap;
    }

    private JsonNode performEnrollmentCall(String partnerCode, String requestBody) {
        log.info("CornellSchedulerService :: performEnrollmentCall partnerCode {} and requestBody {}", partnerCode,requestBody);
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
            log.info("CornellSchedulerService :: performEnrollmentCall response");
            JsonNode jsonData = objectMapper.valueToTree(response.getBody());
            if(!jsonData.isMissingNode()){
                return jsonData;
            }else{
                log.error("Failed to retrieve response data: for partner code {}", partnerCode);
            }
        } else {
            throw new CiosContentException(Constants.ERROR,"Failed to retrieve response from Cornell API " + response.getStatusCode(), HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return null;
    }
}
