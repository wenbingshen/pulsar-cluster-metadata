package com.shen.wen.bing.server;

import com.shen.wen.bing.common.PulsarAdminConfig;
import com.shen.wen.bing.task.BookiesTask;
import com.shen.wen.bing.task.BrokersTask;
import com.shen.wen.bing.task.PulsarTask;
import com.shen.wen.bing.task.details.TopicStatsTask;
import com.shen.wen.bing.utils.PulsarAdminBuilderUtil;
import com.shen.wen.bing.common.Metadata;
import com.shen.wen.bing.task.NamespacesTask;
import com.shen.wen.bing.task.TenantsTask;
import com.shen.wen.bing.task.TopicsTask;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.PulsarClientException;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class PulsarTaskBuilder {
    private static final String CLUSTER_SPLIT = ",";

    public static List<PulsarTask> buildPulsarTask(Properties pulsarConfig, Metadata metadata)
            throws PulsarClientException {
        String clusterStr = pulsarConfig.getProperty("cluster");
        String[] clusters = clusterStr.split(CLUSTER_SPLIT);
        ArrayList<PulsarTask> taskLists = new ArrayList<>();

        for (String cluster : clusters) {
            PulsarAdminConfig.PulsarAdminConfigBuilder pulsarAdminConfigBuilder = PulsarAdminConfig.builder()
                    .serviceUrl(pulsarConfig.getProperty(cluster + ".serviceUrl"));
            if (pulsarConfig.containsKey(cluster + ".authPlugin")) {
                pulsarAdminConfigBuilder
                        .authPlugin(pulsarConfig.getProperty(cluster + ".authPlugin"))
                        .authParams(pulsarConfig.getProperty(cluster + ".authParams"));
            }
            Properties pulsarAdminConfig = pulsarAdminConfigBuilder.build().toProperties();

            PulsarAdmin admin = PulsarAdminBuilderUtil.build(pulsarAdminConfig);

            boolean bookiesEnable = Boolean.parseBoolean(
                    pulsarConfig.getProperty(cluster + ".bookiesTask.enable", "false"));
            if (bookiesEnable) {
                taskLists.add(new BookiesTask(admin, cluster, metadata));
            }

            boolean brokersEnable = Boolean.parseBoolean(
                    pulsarConfig.getProperty(cluster + ".brokersTask.enable", "false"));
            if (brokersEnable) {
                taskLists.add(new BrokersTask(admin, cluster, metadata));
            }

            boolean tenantsEnable = Boolean.parseBoolean(
                    pulsarConfig.getProperty(cluster + ".tenantsTask.enable", "false"));
            if (tenantsEnable) {
                taskLists.add(new TenantsTask(admin, cluster, metadata));
            }

            boolean namespacesEnable = Boolean.parseBoolean(
                    pulsarConfig.getProperty(cluster + ".namespacesTask.enable", "false"));
            if (tenantsEnable && namespacesEnable) {
                taskLists.add(new NamespacesTask(admin, cluster, metadata));
            }

            boolean topicsEnable = Boolean.parseBoolean(
                    pulsarConfig.getProperty(cluster + ".topicsTask.enable", "false"));
            if (tenantsEnable && namespacesEnable && topicsEnable) {
                taskLists.add(new TopicsTask(admin, cluster, metadata));
            }

            boolean topicStatsEnable = Boolean.parseBoolean(
                    pulsarConfig.getProperty(cluster + ".topicStatsTask.enable", "false"));
            if (topicsEnable && topicStatsEnable) {
                taskLists.add(new TopicStatsTask(admin, cluster, metadata));
            }
        }

        return taskLists;
    }


}
