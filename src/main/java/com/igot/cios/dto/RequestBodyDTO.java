package com.igot.cios.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class RequestBodyDTO {
    private String serviceCode;
    private Map<String, String> urlMap;
    private String secureVendorName;
}
