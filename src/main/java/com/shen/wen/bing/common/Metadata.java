package com.shen.wen.bing.common;

import com.google.common.collect.Maps;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import lombok.ToString;
import org.apache.pulsar.common.policies.data.RawBookieInfo;
import org.apache.pulsar.common.policies.data.TopicStats;

@ToString
public class Metadata {

    private final Map<String, List<RawBookieInfo>> bookies;
    private final Map<String, List<String>> brokers;
    private final Map<String, List<String>> tenants;
    private final Map<String, List<String>> namespaces;
    private final Map<String, List<String>> topics;
    private final Map<String, List<TopicStats>> topicStats;

    public Metadata() {
        this.bookies = Maps.newConcurrentMap();
        this.brokers = Maps.newConcurrentMap();
        this.tenants = Maps.newConcurrentMap();
        this.namespaces = Maps.newConcurrentMap();
        this.topics = Maps.newConcurrentMap();
        this.topicStats = Maps.newConcurrentMap();
    }

    public List<RawBookieInfo> getBookies(String cluster) {
        if (bookies.containsKey(cluster)) {
            return bookies.get(cluster);
        }
        return Collections.emptyList();
    }

    public List<String> getBrokers(String cluster) {
        if (brokers.containsKey(cluster)) {
            return brokers.get(cluster);
        }
        return Collections.emptyList();
    }

    public List<String> getTenants(String cluster) {
        if (tenants.containsKey(cluster)) {
            return tenants.get(cluster);
        }
        return Collections.emptyList();
    }

    public List<String> getNamespaces(String cluster) {
        if (namespaces.containsKey(cluster)) {
            return namespaces.get(cluster);
        }
        return Collections.emptyList();
    }

    public List<String> getTopics(String cluster) {
        if (topics.containsKey(cluster)) {
            return topics.get(cluster);
        }
        return Collections.emptyList();
    }

    public List<TopicStats> getTopicStats(String cluster) {
        if (topicStats.containsKey(cluster)) {
            return topicStats.get(cluster);
        }
        return Collections.emptyList();
    }

    public void setBookies(String cluster, List<RawBookieInfo> newBookiesInfo) {
        bookies.put(cluster, newBookiesInfo);
    }

    public void setBrokers(String cluster, List<String> newBrokers) {
        brokers.put(cluster, newBrokers);
    }

    public void setTenants(String cluster, List<String> newTenants) {
        tenants.put(cluster, newTenants);
    }

    public void setNamespaces(String cluster, List<String> newNamespaces) {
        namespaces.put(cluster, newNamespaces);
    }

    public void setTopics(String cluster, List<String> newTopics) {
        topics.put(cluster, newTopics);
    }

    public void setTopicStats(String cluster, List<TopicStats> topicStatsList) {
        topicStats.put(cluster, topicStatsList);
    }

    public void clear() {
        bookies.clear();
        brokers.clear();
        tenants.clear();
        namespaces.clear();
        topics.clear();
        topicStats.clear();
    }
}
