package com.shen.wen.bing.task;

import com.shen.wen.bing.common.Metadata;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;

@Slf4j
public class TenantsTask implements PulsarTask{

    private final PulsarAdmin admin;
    private final String cluster;
    private final Metadata metadata;

    public TenantsTask(PulsarAdmin admin,
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
                List<String> tenants = admin.tenants().getTenants();
                metadata.setTenants(cluster, tenants);
                log.info("Metadata cluster {}, tenants {}", cluster, tenants);
            } catch (PulsarAdminException e) {
                log.error("Get tenants for cluster {} failed", cluster, e);
            }
        }
    }
}
