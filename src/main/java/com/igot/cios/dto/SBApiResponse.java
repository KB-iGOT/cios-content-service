package com.igot.cios.dto;

import com.igot.cios.util.Constants;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.joda.time.DateTime;
import org.springframework.http.HttpStatus;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class SBApiResponse {
    private String id;
    private String ver;
    private String ts;
    private ApiRespParam params;
    private HttpStatus responseCode;
    private transient Map<String, Object> result = new HashMap<>();

    public Object get(String key) {
        return result.get(key);
    }

    public void put(String key, Object vo) {
        result.put(key, vo);
    }

    public void putAll(Map<String, Object> map) {
        result.putAll(map);
    }

    public boolean containsKey(String key) {
        return result.containsKey(key);
    }

    public static SBApiResponse createDefaultResponse(String api) {
        SBApiResponse response = new SBApiResponse();
        response.setId(api);
        response.setVer(Constants.API_VERSION_1);
        response.setParams(new ApiRespParam(UUID.randomUUID().toString()));
        response.getParams().setStatus(Constants.SUCCESS);
        response.setResponseCode(HttpStatus.OK);
        response.setTs(DateTime.now().toString());
        return response;
    }
}
