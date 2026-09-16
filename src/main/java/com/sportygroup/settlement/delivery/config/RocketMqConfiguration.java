package com.sportygroup.settlement.delivery.config;

import com.sportygroup.settlement.delivery.messaging.RocketMqSettlementMessageListener;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(RocketMqProperties.class)
@Profile("rocketmq")
public class RocketMqConfiguration {

    @Bean(destroyMethod = "shutdown")
    DefaultMQProducer settlementRocketMqProducer(RocketMqProperties properties) throws Exception {
        var producer = new DefaultMQProducer(properties.producerGroup());
        producer.setNamesrvAddr(properties.nameServer());
        producer.setRetryTimesWhenSendFailed(properties.sendRetries());
        producer.start();
        return producer;
    }

    @Bean(destroyMethod = "shutdown")
    @ConditionalOnProperty(
            name = "app.rocketmq.consumer-enabled",
            havingValue = "true",
            matchIfMissing = true)
    DefaultMQPushConsumer settlementRocketMqConsumer(
            RocketMqProperties properties,
            RocketMqSettlementMessageListener listener
    ) throws Exception {
        var consumer = new DefaultMQPushConsumer(properties.consumerGroup());
        consumer.setNamesrvAddr(properties.nameServer());
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.setConsumeMessageBatchMaxSize(1);
        consumer.setMaxReconsumeTimes(properties.maxReconsumeTimes());
        consumer.subscribe(properties.topic(), "*");
        consumer.registerMessageListener( listener);
        consumer.start();
        return consumer;
    }
}
