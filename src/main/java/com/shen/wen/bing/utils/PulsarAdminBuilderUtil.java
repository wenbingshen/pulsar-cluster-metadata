package com.shen.wen.bing.utils;

import com.google.common.collect.Maps;
import java.util.Map;
import java.util.Properties;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminBuilder;
import org.apache.pulsar.client.api.PulsarClientException;

public class PulsarAdminBuilderUtil {
    public static final Map<String, PulsarAdmin> clusterAndAdmin = Maps.newConcurrentMap();

    public static PulsarAdmin build(final Properties adminConfig) throws PulsarClientException {
        PulsarAdminBuilder pulsarAdminBuilder = PulsarAdmin.builder()
                .serviceHttpUrl(adminConfig.getProperty("serviceUrl"));
        if (adminConfig.containsKey("authPlugin")) {
            pulsarAdminBuilder.authentication(adminConfig.getProperty("authPlugin"),
                    adminConfig.getProperty("authParams"));
        }

        PulsarAdmin pulsarAdmin = pulsarAdminBuilder.build();

        clusterAndAdmin.put(adminConfig.getProperty("cluster"), pulsarAdmin);
        return pulsarAdmin;
    }
}
