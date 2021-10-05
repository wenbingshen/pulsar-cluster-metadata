package com.shen.wen.bing.task;

import com.shen.wen.bing.common.Metadata;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;

@Slf4j
public class BrokersTask implements PulsarTask{
    private final PulsarAdmin admin;
    private final String cluster;
    private final Metadata metadata;

    public BrokersTask(PulsarAdmin admin,
                       String cluster,
                       Metadata metadata) {
        this.admin = admin;
        this.cluster = cluster;
        this.metadata = metadata;
    }
    @Override
    public void safeRun() {
        while (true) {
            try {
                List<String> activeBrokers = admin.brokers().getActiveBrokers(cluster);
                metadata.setBrokers(cluster, activeBrokers);
                log.info("Metadata cluster {}, brokers {}", cluster, metadata.getBrokers(cluster));
            } catch (PulsarAdminException e) {
                log.error("Get active brokers for cluster {} failed", cluster, e);
            }
        }
    }
}
