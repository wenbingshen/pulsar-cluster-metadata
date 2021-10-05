package com.shen.wen.bing.task.details;

import com.shen.wen.bing.common.Metadata;
import com.shen.wen.bing.task.PulsarTask;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.policies.data.TopicStats;

@Slf4j
public class TopicStatsTask implements PulsarTask {
    private final PulsarAdmin admin;
    private final String cluster;
    private final Metadata metadata;

    public TopicStatsTask(PulsarAdmin admin,
                      String cluster,
                      Metadata metadata) {
        this.admin = admin;
        this.cluster = cluster;
        this.metadata = metadata;
    }

    @Override
    public void safeRun() {
        List<TopicStats> topicStatsList = new ArrayList<>();
        metadata.getTopics(cluster).forEach(topic -> {
            try {
                TopicStats stats = admin.topics().getStats(topic);
                topicStatsList.add(stats);
            } catch (PulsarAdminException e) {
                log.error("Get topic stats for cluster {} topic {} failed", cluster, topic, e);
            }
        });
        if (!topicStatsList.isEmpty()) {
            metadata.setTopicStats(cluster, topicStatsList);
            log.info("Metadata cluster {}, topicStats {}", cluster, metadata.getTopicStats(cluster));
        }
    }
}
