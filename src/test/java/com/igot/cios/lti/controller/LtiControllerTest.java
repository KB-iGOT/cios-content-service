package com.igot.cios.lti.controller;

import com.igot.cios.dto.SBApiResponse;
import com.igot.cios.lti.service.LtiService;
import com.igot.cios.util.Constants;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.web.servlet.MockMvc;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@ExtendWith(SpringExtension.class)
@WebMvcTest(LtiController.class)
class LtiControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private LtiService ltiService;

    @Test
    void testOidcConnection_delegatesToServiceAndReturnsOk() throws Exception {
        SBApiResponse response = SBApiResponse.createDefaultResponse(Constants.API_LTI_OIDC_TEST);
        response.getParams().setStatus(Constants.SUCCESS);
        response.getParams().setErrmsg(Constants.ALL_CHECKS_PASSED);
        response.setResponseCode(HttpStatus.OK);
        Map<String, Object> result = new HashMap<>();
        result.put(Constants.PARTNER_CODE, "IGOT-TESTINGLTI-OGDTD");
        response.setResult(result);

        Mockito.when(ltiService.testOidcConnection("IGOT-TESTINGLTI-OGDTD", null, null))
                .thenReturn(response);

        mockMvc.perform(get("/ciosIntegration/lti/ssotest/{partnerCode}", "IGOT-TESTINGLTI-OGDTD"))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.params.status").value(Constants.SUCCESS))
                .andExpect(jsonPath("$.params.errmsg").value(Constants.ALL_CHECKS_PASSED))
                .andExpect(jsonPath("$.result.partnerCode").value("IGOT-TESTINGLTI-OGDTD"));
    }

    @Test
    void testOidcConnection_passesLoginHintAndMessageHintThrough() throws Exception {
        SBApiResponse response = SBApiResponse.createDefaultResponse(Constants.API_LTI_OIDC_TEST);
        response.setResponseCode(HttpStatus.OK);

        Mockito.when(ltiService.testOidcConnection(eq("IGOT-TESTINGLTI-OGDTD"), eq("demo-learner-1"), eq("test-hint")))
                .thenReturn(response);

        mockMvc.perform(get("/ciosIntegration/lti/ssotest/{partnerCode}", "IGOT-TESTINGLTI-OGDTD")
                        .param("loginHint", "demo-learner-1")
                        .param("messageHint", "test-hint"))
                .andExpect(status().isOk());

        Mockito.verify(ltiService).testOidcConnection("IGOT-TESTINGLTI-OGDTD", "demo-learner-1", "test-hint");
    }

    @Test
    void testOidcConnection_propagatesFailureResponseCode() throws Exception {
        SBApiResponse response = SBApiResponse.createDefaultResponse(Constants.API_LTI_OIDC_TEST);
        response.getParams().setStatus(Constants.FAILED);
        response.getParams().setErrmsg("Partner not found for code: UNKNOWN-PARTNER");
        response.setResponseCode(HttpStatus.OK);

        Mockito.when(ltiService.testOidcConnection(eq("UNKNOWN-PARTNER"), isNull(), isNull()))
                .thenReturn(response);

        mockMvc.perform(get("/ciosIntegration/lti/ssotest/{partnerCode}", "UNKNOWN-PARTNER"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.params.status").value(Constants.FAILED))
                .andExpect(jsonPath("$.params.errmsg").value("Partner not found for code: UNKNOWN-PARTNER"));
    }
}
