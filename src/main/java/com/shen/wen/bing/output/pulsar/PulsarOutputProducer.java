package com.shen.wen.bing.output.pulsar;

import com.shen.wen.bing.utils.PulsarAdminBuilderUtil;
import java.util.concurrent.TimeUnit;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;

@Slf4j
public class PulsarOutputProducer {

    private static final String DEFAULT_TENANT = "public";
    private static final String DEFAULT_NAMESPACE = "default";
    private static final String DEFAULT_TOPIC = "pulsar-metadata-out";

    private final Properties pulsarConfig;
    private final String cluster;
    private final String tenant;
    private final String namespace;
    private final String topic;
    private final String fullTopic;
    private final PulsarAdmin admin;
    private PulsarClient pulsarClient;
    private Producer<String> producer;

    public PulsarOutputProducer(Properties pulsarConfig) {
        this.pulsarConfig = pulsarConfig;
        this.cluster = pulsarConfig.getProperty("cluster");
        this.tenant = pulsarConfig.getProperty(cluster + ".tenant", DEFAULT_TENANT);
        this.namespace = pulsarConfig.getProperty(cluster + ".namespace", DEFAULT_NAMESPACE);
        this.topic = pulsarConfig.getProperty(cluster + ".topic", DEFAULT_TOPIC);
        this.fullTopic = tenant + "/" + namespace + "/" + topic;
        this.admin = PulsarAdminBuilderUtil.clusterAndAdmin.get(cluster);
    }

    public void init() {
        createOutputTopic();
        try {
            ClientBuilder clientBuilder = PulsarClient.builder()
                    .serviceUrl(pulsarConfig.getProperty(cluster + ".serviceUrl"))
                    .operationTimeout(10000, TimeUnit.MILLISECONDS);
            if (pulsarConfig.containsKey(cluster + ".authPlugin")) {
                clientBuilder.authentication(pulsarConfig.getProperty(cluster + ".authPlugin"),
                        pulsarConfig.getProperty(cluster + ".authParams"));
            }
            this.pulsarClient = clientBuilder.build();
            this.producer = this.pulsarClient.newProducer(Schema.STRING)
                    .topic(fullTopic)
                    .create();
        } catch (PulsarClientException e) {
            log.error("Create cluster {} pulsarOutputProducer failed", cluster, e);
            System.exit(1);
        }

    }

    protected void createOutputTopic() {
        try {
            admin.topics().createNonPartitionedTopic(fullTopic);
        } catch (PulsarAdminException e) {
            if (e instanceof PulsarAdminException.ConflictException) {
                log.warn("Topic {} exists, skip create it", fullTopic);
            } else {
                log.error("Create topic {} failed", fullTopic, e);
                System.exit(1);
            }
        }
    }

}
