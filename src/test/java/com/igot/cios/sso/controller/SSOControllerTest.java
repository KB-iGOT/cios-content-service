package com.igot.cios.sso.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.igot.cios.dto.SBApiResponse;
import com.igot.cios.sso.service.SSOService;
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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@ExtendWith(SpringExtension.class)
@WebMvcTest(SSOController.class)
class SSOControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private SSOService ssoService;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void createSsoConfiguration_success() throws Exception {
        ObjectNode payload = objectMapper.createObjectNode();
        payload.put("clientId", "client1");

        SBApiResponse response = SBApiResponse.createDefaultResponse("create");
        response.setResponseCode(HttpStatus.OK);

        Mockito.when(ssoService.createSsoConfiguration(any(), eq("p1")))
                .thenReturn(response);

        mockMvc.perform(post("/sso/create/{id}", "p1")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(payload)))
                .andExpect(status().isOk());
    }


    @Test
    void updateSsoConfiguration_success() throws Exception {
        ObjectNode payload = objectMapper.createObjectNode();
        payload.put("ssoId", "kc-id");

        SBApiResponse response = SBApiResponse.createDefaultResponse("update");
        response.setResponseCode(HttpStatus.OK);

        Mockito.when(ssoService.updateSsoConfiguration(any(), eq("p1")))
                .thenReturn(response);

        mockMvc.perform(post("/sso/update/{id}", "p1")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(payload)))
                .andExpect(status().isOk());
    }

    @Test
    void readSsoConfiguration_success() throws Exception {
        SBApiResponse response = SBApiResponse.createDefaultResponse("read");
        response.setResponseCode(HttpStatus.OK);

        Mockito.when(ssoService.readSsoConfiguration("p1"))
                .thenReturn(response);

        mockMvc.perform(get("/sso/read/{id}", "p1"))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON));
    }
}
