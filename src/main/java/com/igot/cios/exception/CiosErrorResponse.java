package com.igot.cios.exception;

import lombok.Builder;
import lombok.Value;

import java.util.Map;

@Value
@Builder
public class CiosErrorResponse {

    private String code;
    private String message;
    private Map<String, String> errors;
    private String httpStatusCode;
}