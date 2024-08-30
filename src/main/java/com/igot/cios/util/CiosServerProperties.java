package com.igot.cios.util;

import lombok.Getter;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
@Getter
@Setter
public class CiosServerProperties {

    @Value("${cb.pores.service.url}")
    private String partnerServiceUrl;

    @Value("${partner.read.path}")
    private String partnerReadEndPoint;

    @Value("${partner.create.update.path}")
    private String partnerCreateEndPoint;

    @Value("${sb.api.key}")
    private String sbApiKey;
}
