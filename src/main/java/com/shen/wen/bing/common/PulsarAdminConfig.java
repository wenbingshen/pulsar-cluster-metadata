package com.shen.wen.bing.common;

import lombok.Builder;

import java.util.Properties;

@Builder
public class PulsarAdminConfig {
    private final String serviceUrl;
    private final String authPlugin;
    private final String authParams;
    private final String cluster;

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

        if (cluster != null) {
            properties.put("cluster", cluster);
        }

        return properties;
    }

}
