package com.igot.cios.plugins;

public enum ContentSource {
    CORNELL,
    UPGRAD;


    public static ContentSource fromPartnerCode(String partnerCode) {
        switch (partnerCode) {
            case "CORNELL":
                return CORNELL;
            case "UPGRADE":
                return UPGRAD;
            default:
                throw new RuntimeException("Unknown partner code: " + partnerCode);
        }
    }
}
