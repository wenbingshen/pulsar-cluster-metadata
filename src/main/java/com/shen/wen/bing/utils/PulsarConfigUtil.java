package com.shen.wen.bing.utils;

import lombok.extern.slf4j.Slf4j;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

@Slf4j
public class PulsarConfigUtil {

    public static Properties loadConfig(String configFile) {
        Properties properties = new Properties();
        try {
            FileInputStream inputStream = new FileInputStream(configFile);
            properties.load(inputStream);
        } catch (IOException e) {
            log.error("Load config file {} failed", configFile, e);
            System.exit(1);
        }
        return properties;
    }

}
