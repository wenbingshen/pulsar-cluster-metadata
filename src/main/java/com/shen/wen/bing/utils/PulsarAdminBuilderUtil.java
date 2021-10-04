package com.shen.wen.bing.utils;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.PulsarClientException;

import java.util.Properties;

public class PulsarAdminBuilderUtil {

    public static PulsarAdmin build(final Properties adminConfig) throws PulsarClientException {
        return PulsarAdmin.builder().serviceHttpUrl(adminConfig.getProperty("serviceUrl"))
                .authentication(adminConfig.getProperty("authPlugin"), adminConfig.getProperty("authParams")).build();

    }
}
