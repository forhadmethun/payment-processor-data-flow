package com.tokenizer.rest.config;

import io.dropwizard.Configuration;

public class AppConfiguration extends Configuration {
    private String bootstrapServer = "localhost:9092";
    private String keyDeserializer = "org.apache.kafka.common.serialization.StringDeserializer";
    private String valueDeserializer = "org.apache.kafka.common.serialization.StringDeserializer";
    private String topicCardInfo = "card-info";
    private String groupId = "card-info-group";

    private String keySerializer = "org.apache.kafka.common.serialization.StringSerializer";
    private String valueSerializer = "org.apache.kafka.common.serialization.StringSerializer";
    private String topicEncryptedCardInfo = "encrypted-card-info";
    private String encryptionKey = "Bar12345Bar12345"; // 128 bit key

    private String redisHost = "localhost";
    private String redisPort = "6379";

    public String getBootstrapServer() {
        return bootstrapServer;
    }

    public AppConfiguration setBootstrapServer(String bootstrapServer) {
        this.bootstrapServer = bootstrapServer;
        return this;
    }

    public String getKeyDeserializer() {
        return keyDeserializer;
    }

    public AppConfiguration setKeyDeserializer(String keyDeserializer) {
        this.keyDeserializer = keyDeserializer;
        return this;
    }

    public String getValueDeserializer() {
        return valueDeserializer;
    }

    public AppConfiguration setValueDeserializer(String valueDeserializer) {
        this.valueDeserializer = valueDeserializer;
        return this;
    }

    public String getTopicCardInfo() {
        return topicCardInfo;
    }

    public AppConfiguration setTopicCardInfo(String topicCardInfo) {
        this.topicCardInfo = topicCardInfo;
        return this;
    }

    public String getGroupId() {
        return groupId;
    }

    public AppConfiguration setGroupId(String groupId) {
        this.groupId = groupId;
        return this;
    }

    public String getKeySerializer() {
        return keySerializer;
    }

    public AppConfiguration setKeySerializer(String keySerializer) {
        this.keySerializer = keySerializer;
        return this;
    }

    public String getTopicEncryptedCardInfo() {
        return topicEncryptedCardInfo;
    }

    public AppConfiguration setTopicEncryptedCardInfo(String topicEncryptedCardInfo) {
        this.topicEncryptedCardInfo = topicEncryptedCardInfo;
        return this;
    }

    public String getValueSerializer() {
        return valueSerializer;
    }

    public AppConfiguration setValueSerializer(String valueSerializer) {
        this.valueSerializer = valueSerializer;
        return this;
    }

    public String getEncryptionKey() {
        return encryptionKey;
    }

    public AppConfiguration setEncryptionKey(String encryptionKey) {
        this.encryptionKey = encryptionKey;
        return this;
    }

    public String getRedisHost() {
        return redisHost;
    }

    public AppConfiguration setRedisHost(String redisHost) {
        this.redisHost = redisHost;
        return this;
    }

    public String getRedisPort() {
        return redisPort;
    }

    public AppConfiguration setRedisPort(String redisPort) {
        this.redisPort = redisPort;
        return this;
    }
}
