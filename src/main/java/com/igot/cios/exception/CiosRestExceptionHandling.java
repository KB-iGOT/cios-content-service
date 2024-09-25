package com.igot.cios.exception;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
@Slf4j
public class CiosRestExceptionHandling {

    @ExceptionHandler(Exception.class)
    public ResponseEntity handleException(Exception ex) {
        log.debug("RestExceptionHandler::handleException::" + ex);
        HttpStatus status = HttpStatus.INTERNAL_SERVER_ERROR;
        CiosErrorResponse errorResponse = null;
        if (ex instanceof CiosContentException) {
            CiosContentException esUtilException = (CiosContentException) ex;
            status = HttpStatus.BAD_REQUEST;
            errorResponse = CiosErrorResponse.builder()
                    .code(esUtilException.getCode())
                    .message(esUtilException.getMessage())
                    .httpStatusCode(esUtilException.getHttpStatusCode() != null
                            ? String.valueOf(esUtilException.getHttpStatusCode())
                            : String.valueOf(status.value()))
                    .build();
            if (StringUtils.isNotBlank(esUtilException.getMessage())) {
                log.error(esUtilException.getMessage());
            }

            return new ResponseEntity<>(errorResponse, status);
        }
        errorResponse = CiosErrorResponse.builder()
                .code(ex.getMessage()).build();
        return new ResponseEntity<>(errorResponse, status);
    }

}