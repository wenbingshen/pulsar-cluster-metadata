package com.shen.wen.bing.task;

import com.shen.wen.bing.common.Metadata;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;

@Slf4j
public class NamespacesTask implements PulsarTask {

    private final PulsarAdmin admin;
    private final String cluster;
    private final Metadata metadata;

    public NamespacesTask(PulsarAdmin admin,
                          String cluster,
                          Metadata metadata) {
        this.admin = admin;
        this.cluster = cluster;
        this.metadata = metadata;
    }

    @Override
    public void safeRun() {
        List<String> allNamespaces = new ArrayList<>();
        metadata.getTenants(cluster).forEach(tenant -> {
            try {
                List<String> namespaces = admin.namespaces().getNamespaces(tenant);
                allNamespaces.addAll(namespaces);
                log.info("Metadata cluster {}, tenant {}, namespaces {}", cluster, tenant, namespaces);
            } catch (PulsarAdminException e) {
                log.error("Get namespaces for cluster {} tenant {} failed", cluster, tenant, e);
            }
        });
        metadata.setNamespaces(cluster, allNamespaces);
    }
}
