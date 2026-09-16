package com.sportygroup.settlement.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    @Bean
    NewTopic outcomesTopic(@Value("${app.kafka.outcomes-topic}") String topic) {
        return TopicBuilder.name(topic).partitions(16).replicas(1).build();
    }

    @Bean
    NewTopic pageTasksTopic(@Value("${app.kafka.page-tasks-topic}") String topic) {
        return TopicBuilder.name(topic).partitions(32).replicas(1).build();
    }

    @Bean
    NewTopic settlementCommandsTopic(@Value("${app.kafka.settlement-commands-topic}") String topic) {
        return TopicBuilder.name(topic).partitions(32).replicas(1).build();
    }

    @Bean
    NewTopic outcomesDltTopic(@Value("${app.kafka.outcomes-dlt-topic}") String topic) {
        return TopicBuilder.name(topic).partitions(16).replicas(1).build();
    }

    @Bean
    NewTopic pageTasksDltTopic(@Value("${app.kafka.page-tasks-dlt-topic}") String topic) {
        return TopicBuilder.name(topic).partitions(32).replicas(1).build();
    }

    @Bean
    NewTopic settlementCommandsDltTopic(
            @Value("${app.kafka.settlement-commands-dlt-topic}") String topic
    ) {
        return TopicBuilder.name(topic).partitions(32).replicas(1).build();
    }
}
