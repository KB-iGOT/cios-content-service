package com.igot.cios.plugins;

public enum ContentSource {
    CORNELL,
    UPGRAD;


    public static ContentSource fromOrgId(String orgId) {
        switch (orgId) {
            case "G00345":
                return CORNELL;
            case "1234":
                return UPGRAD;
            default:
                throw new RuntimeException("Unknown provider org id: " + orgId);
        }
    }
}
