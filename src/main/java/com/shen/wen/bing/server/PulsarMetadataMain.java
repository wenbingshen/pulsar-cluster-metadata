package com.shen.wen.bing.server;

import com.shen.wen.bing.common.Metadata;
import com.shen.wen.bing.task.PulsarTask;
import com.shen.wen.bing.utils.PulsarConfigUtil;
import java.util.List;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.PulsarClientException;

@Slf4j
public class PulsarMetadataMain {

    public static void main(String[] args) throws PulsarClientException, InterruptedException {
        if (args.length < 1) {
            log.error("Please put a pulsar client config file, current input args is empty");
            System.exit(1);
        }
        Properties pulsarConfig = PulsarConfigUtil.loadConfig(args[0]);
        log.info("load config");

        // 校验Config，如cluster name重复

        Metadata metadata = new Metadata();
        log.info("init metadata");
        System.out.println("init metadata");

        List<PulsarTask> pulsarTasks = PulsarTaskBuilder.buildPulsarTask(pulsarConfig, metadata);
        log.info("build pulsar tasks");
        System.out.println("build pulsar tasks");

        Runtime.getRuntime().addShutdownHook(new Thread(metadata::clear));

        pulsarTasks.forEach(pulsarTask -> {
            new Thread(pulsarTask).start();
        });
        System.out.println("all pulsar tasks running now");
        log.info("all pulsar tasks running now");

        Thread.currentThread().join();
    }
}
