package com.shen.wen.bing.utils;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminBuilder;
import org.apache.pulsar.client.api.PulsarClientException;

import java.util.Properties;

public class PulsarAdminBuilderUtil {

    public static PulsarAdmin build(final Properties adminConfig) throws PulsarClientException {
        PulsarAdminBuilder pulsarAdminBuilder = PulsarAdmin.builder()
                .serviceHttpUrl(adminConfig.getProperty("serviceUrl"));
        if (adminConfig.containsKey("authPlugin")) {
            pulsarAdminBuilder.authentication(adminConfig.getProperty("authPlugin"),
                    adminConfig.getProperty("authParams"));
        }

        return pulsarAdminBuilder.build();
    }
}
