package com.igot.cios.scheduler;

import com.fasterxml.jackson.databind.JsonNode;

public interface SchedulerInterface {
    JsonNode loadEnrollment();
    JsonNode performEnrollmentCall(String partnerCode,String requestBody);
    void callEnrollmentAPI(String partnerCode, JsonNode rawContentData);
}
