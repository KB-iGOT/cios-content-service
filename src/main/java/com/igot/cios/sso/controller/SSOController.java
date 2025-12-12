package com.igot.cios.sso.controller;

import com.fasterxml.jackson.databind.JsonNode;
import com.igot.cios.dto.SBApiResponse;
import com.igot.cios.sso.service.SSOService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/sso")
@Slf4j
public class SSOController {
    @Autowired
    SSOService service;

    @PostMapping("/create/{id}")
    public ResponseEntity<SBApiResponse> create(@RequestBody JsonNode ssoDetails, @PathVariable String id) {
        SBApiResponse response = service.createSsoConfiguration(ssoDetails,id);
        return new ResponseEntity<>(response, response.getResponseCode());
    }

    @PostMapping("/update/{id}")
    public ResponseEntity<SBApiResponse> update(@RequestBody JsonNode ssoDetails, @PathVariable String id) {
        SBApiResponse response = service.updateSsoConfiguration(ssoDetails,id);
        return new ResponseEntity<>(response, response.getResponseCode());
    }

    @GetMapping("/read/{id}")
    public ResponseEntity<SBApiResponse> read(@PathVariable String id) {
        SBApiResponse response = service.readSsoConfiguration(id);
        return new ResponseEntity<>(response, HttpStatus.OK);
    }

    @DeleteMapping("/delete/{id}")
    public ResponseEntity<String> delete(@PathVariable String id) {
        String response = service.disableSsoConfiguration(id);
        return new ResponseEntity<>(response, HttpStatus.OK);
    }

}
