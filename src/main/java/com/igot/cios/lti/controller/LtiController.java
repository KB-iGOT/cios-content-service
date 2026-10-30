package com.igot.cios.lti.controller;

import com.igot.cios.dto.SBApiResponse;
import com.igot.cios.lti.service.LtiService;
import com.igot.cios.util.Constants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/ciosIntegration/lti")
@Slf4j
public class LtiController {

    private final LtiService ltiService;

    @Autowired
    public LtiController(LtiService ltiService) {
        this.ltiService = ltiService;
    }

    @GetMapping("/ssotest/{partnerCode}")
    public ResponseEntity<SBApiResponse> testOidcConnection(
            @PathVariable String partnerCode,
            @RequestParam(required = false) String loginHint,
            @RequestParam(required = false) String messageHint
            ) {
        SBApiResponse response = ltiService.testOidcConnection(partnerCode, loginHint, messageHint);
        return new ResponseEntity<>(response, response.getResponseCode());
    }
}
