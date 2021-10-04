package com.shen.wen.bing.task;

import com.shen.wen.bing.common.Metadata;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class TopicsTask implements PulsarTask {
    private final PulsarAdmin admin;
    private final String cluster;
    private final Metadata metadata;

    public TopicsTask(PulsarAdmin admin,
                      String cluster,
                      Metadata metadata) {
        this.admin = admin;
        this.cluster = cluster;
        this.metadata = metadata;
    }

    @Override
    public void safeRun() {
        while (true) {
            List<String> allTopics = new ArrayList<>();
            metadata.getNamespaces(cluster).forEach(namespace -> {
                try {
                    List<String> topics = admin.topics().getList(namespace);
                    allTopics.addAll(topics);
                    log.info("Metadata cluster {}, namespace {}, topics {}", cluster, namespace, topics);
                    System.out.println("Metadata cluster " + cluster + ", namespace " + namespace + ", topics " + topics);
                } catch (PulsarAdminException e) {
                    log.error("Get topic list for cluster {} namespace {} failed", cluster, namespace, e);
                }
            });
            metadata.setTopics(cluster, allTopics);
        }
    }
}
