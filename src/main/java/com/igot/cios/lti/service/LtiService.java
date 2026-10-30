package com.igot.cios.lti.service;

import com.igot.cios.dto.SBApiResponse;

public interface LtiService {

    SBApiResponse testOidcConnection(String partnerCode, String loginHint, String messageHint);
}
