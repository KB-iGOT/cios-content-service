package com.igot.cios.exception;

import lombok.Getter;
import lombok.Setter;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

import java.util.Map;

@Getter
@Setter
@Component
public class CiosContentException extends RuntimeException {
    private String code;
    private String message;
    private HttpStatus httpStatusCode;
    private Map<String, String> errors;

    public CiosContentException() {
    }

    public CiosContentException(String code, String message) {
        this.code = code;
        this.message = message;
    }

    public CiosContentException(String code, String message, HttpStatus httpStatusCode) {
        this.code = code;
        this.message = message;
        this.httpStatusCode = httpStatusCode;
    }

    public CiosContentException(String message, HttpStatus httpStatusCode) {
        this.message = message;
        this.httpStatusCode = httpStatusCode;
    }

    public CiosContentException(Map<String, String> errors) {
        this.message = errors.toString();
        this.errors = errors;
    }

}