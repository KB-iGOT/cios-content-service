package com.igot.cios.sso.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.igot.cios.dto.SBApiResponse;


public interface SSOService {
    SBApiResponse createSsoConfiguration(JsonNode ssoDetails, String partnerId);
    SBApiResponse updateSsoConfiguration(JsonNode ssoDetails,String partnerId);
    SBApiResponse readSsoConfiguration(String id);
    SBApiResponse testSamlConfiguration(JsonNode request);
}
