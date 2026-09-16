package com.sportygroup.settlement.delivery.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

@ConfigurationProperties(prefix = "app.rocketmq")
public record RocketMqProperties(
        String nameServer,
        String topic,
        String producerGroup,
        String consumerGroup,
        Duration sendTimeout,
        int sendRetries,
        int maxReconsumeTimes,
        boolean consumerEnabled
) {
}
