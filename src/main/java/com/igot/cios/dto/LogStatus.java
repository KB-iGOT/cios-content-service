package com.igot.cios.dto;

import lombok.*;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

@Getter
@Setter
@Data
@Component
public class LogStatus {
    private boolean hasFailures;
    private List<LinkedHashMap<String, String>> successLogs;
    private List<LinkedHashMap<String, String>> errorLogs;

    public LogStatus() {
        this.hasFailures = false;
        this.successLogs = new ArrayList<>();
        this.errorLogs = new ArrayList<>();
    }

    public void clearLogs() {
        this.successLogs.clear();
        this.errorLogs.clear();
        this.hasFailures = false;
    }
}
