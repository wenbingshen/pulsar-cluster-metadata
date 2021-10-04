package com.shen.wen.bing.task;

import com.shen.wen.bing.common.Metadata;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface PulsarTask extends Runnable{
    PulsarAdmin admin = null;
    String cluster = null;
    Metadata metadata = null;

    Logger LOGGER = LoggerFactory.getLogger(PulsarTask.class);

    @Override
    default void run(){
        try {
            safeRun();
        } catch (Throwable t) {
            LOGGER.error("Unexpected throwable caught ", t);
        }
    };

    void safeRun();

}
