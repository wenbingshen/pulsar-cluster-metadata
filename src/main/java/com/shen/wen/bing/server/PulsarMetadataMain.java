package com.shen.wen.bing.server;

import com.shen.wen.bing.common.Metadata;
import com.shen.wen.bing.task.BookiesTask;
import com.shen.wen.bing.task.BrokersTask;
import com.shen.wen.bing.task.NamespacesTask;
import com.shen.wen.bing.task.PulsarTask;
import com.shen.wen.bing.task.TenantsTask;
import com.shen.wen.bing.task.TopicsTask;
import com.shen.wen.bing.task.details.TopicStatsTask;
import com.shen.wen.bing.utils.PulsarConfigUtil;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.pulsar.client.api.PulsarClientException;

@Slf4j
public class PulsarMetadataMain {
    private static Properties pulsarConfig;

    public static void main(String[] args) throws PulsarClientException, InterruptedException {
        if (args.length < 1) {
            log.error("Please put a pulsar client config file, current input args is empty");
            System.exit(1);
        }
        pulsarConfig = PulsarConfigUtil.loadConfig(args[0]);
        log.info("load config");

        // TODO
        // validate Config，e.g. duplicate cluster name

        Metadata metadata = new Metadata();
        log.info("init metadata");

        List<PulsarTask> pulsarTasks = PulsarTaskBuilder.buildPulsarTask(pulsarConfig, metadata);
        log.info("build pulsar tasks");

        // TODO
        // init pulsar output

        int numTaskThreads = Math.max(1, Integer.parseInt(pulsarConfig.getProperty("num.task.threads",
                String.valueOf(Runtime.getRuntime().availableProcessors() / 2))));

        ScheduledExecutorService pulsarTasksExecutor = OrderedScheduler.newSchedulerBuilder()
                .numThreads(numTaskThreads)
                .name("pulsar-tasks-executor")
                .build();

        Runtime.getRuntime().addShutdownHook(new Thread(()-> {
            if (!pulsarTasksExecutor.isShutdown()) {
                pulsarTasksExecutor.shutdown();
                log.info("pulsarTasksExecutor shutdown now.");
            }
            metadata.clear();
            log.info("pulsar metadata cleared.");
        }));

        String tasksCheckInterval = pulsarConfig.getProperty("pulsar.tasks.check.interval.ms", "15000");

        pulsarTasks.forEach(pulsarTask -> {
            long checkInterval = checkInterval(pulsarTask, tasksCheckInterval);
            pulsarTasksExecutor.scheduleAtFixedRate(
                    pulsarTask,
                    checkInterval,
                    checkInterval,
                    TimeUnit.MILLISECONDS);
        });
        log.info("all pulsar tasks running now");

        Thread.currentThread().join();
    }

    private static long checkInterval(PulsarTask pulsarTask, String tasksCheckInterval) {
        long checkInterval = Long.parseLong(tasksCheckInterval);
        if (pulsarTask instanceof BookiesTask) {
            checkInterval = Long.parseLong(
                    pulsarConfig.getProperty("bookies.tasks.check.interval.ms", tasksCheckInterval));
        } else if (pulsarTask instanceof BrokersTask) {
            checkInterval = Long.parseLong(
                    pulsarConfig.getProperty("brokers.tasks.check.interval.ms", tasksCheckInterval));
        } else if (pulsarTask instanceof TenantsTask) {
            checkInterval = Long.parseLong(
                    pulsarConfig.getProperty("tenants.tasks.check.interval.ms", tasksCheckInterval));
        } else if (pulsarTask instanceof NamespacesTask) {
            checkInterval = Long.parseLong(
                    pulsarConfig.getProperty("namespaces.tasks.check.interval.ms", tasksCheckInterval));
        } else if (pulsarTask instanceof TopicsTask) {
            checkInterval = Long.parseLong(pulsarConfig.getProperty("topics.tasks.check.interval.ms", tasksCheckInterval));
        } else if (pulsarTask instanceof TopicStatsTask) {
            checkInterval = Long.parseLong(pulsarConfig.getProperty("topicStats.tasks.check.interval.ms", tasksCheckInterval));
        }

        return checkInterval;
    }
}
