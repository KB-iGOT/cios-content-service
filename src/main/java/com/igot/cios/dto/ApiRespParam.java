package com.igot.cios.dto;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class ApiRespParam {
    private String resmsgid;
    private String msgid;
    private String err;
    private String status;
    private String errmsg;
    public ApiRespParam(String string) {
    }
}