package com.shen.wen.bing.common;

import lombok.Builder;

import java.util.Properties;

@Builder
public class PulsarAdminConfig {
    private String serviceUrl;
    private String authPlugin;
    private String authParams;

    public Properties toProperties() {
        final Properties properties = new Properties();
        if (serviceUrl != null) {
            properties.put("serviceUrl", serviceUrl);
        }

        if (authPlugin != null) {
            properties.put("authPlugin", authPlugin);
        }

        if (authParams != null) {
            properties.put("authParams", authParams);
        }

        return properties;
    }

}
