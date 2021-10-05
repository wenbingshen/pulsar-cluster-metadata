package com.shen.wen.bing.task;

import com.shen.wen.bing.common.Metadata;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.policies.data.RawBookieInfo;

@Slf4j
public class BookiesTask implements PulsarTask {
    private final PulsarAdmin admin;
    private final String cluster;
    private final Metadata metadata;

    public BookiesTask(PulsarAdmin admin,
                       String cluster,
                       Metadata metadata) {
        this.admin = admin;
        this.cluster = cluster;
        this.metadata = metadata;
    }

    @Override
    public void safeRun() {
        try {
            List<RawBookieInfo> bookies = admin.bookies().getBookies().getBookies();
            metadata.setBookies(cluster, bookies);
            log.info("Metadata cluster {}, bookies {}", cluster, bookies);
        } catch (PulsarAdminException e) {
            log.error("Get bookies for cluster {} failed", cluster, e);
        }
    }
}
