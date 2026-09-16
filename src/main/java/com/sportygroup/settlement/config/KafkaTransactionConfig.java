package com.sportygroup.settlement.config;

import jakarta.persistence.EntityManagerFactory;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultAfterRollbackProcessor;
import org.springframework.kafka.transaction.KafkaTransactionManager;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.util.backoff.FixedBackOff;

import java.time.Duration;

@Configuration
public class KafkaTransactionConfig {

    @Bean
    @Primary
    JpaTransactionManager transactionManager(EntityManagerFactory entityManagerFactory) {
        return new JpaTransactionManager(entityManagerFactory);
    }

    @Bean
    KafkaTemplate<String, String> kafkaTemplate(ProducerFactory<String, String> producerFactory) {
        var kafkaTemplate = new KafkaTemplate<>(producerFactory);
        kafkaTemplate.setAllowNonTransactional(true);
        return kafkaTemplate;
    }

    @Bean
    KafkaTransactionManager<String, String> kafkaTransactionManager(
            ProducerFactory<String, String> producerFactory
    ) {
        return new KafkaTransactionManager<>(producerFactory);
    }

    @Bean
    ConcurrentKafkaListenerContainerFactory<String, String> transactionalKafkaListenerContainerFactory(
            ConsumerFactory<String, String> consumerFactory,
            KafkaTransactionManager<String, String> kafkaTransactionManager,
            DefaultAfterRollbackProcessor<String, String> afterRollbackProcessor
    ) {
        var factory = new ConcurrentKafkaListenerContainerFactory<String, String>();
        factory.setConsumerFactory(consumerFactory);
        factory.getContainerProperties().setKafkaAwareTransactionManager(kafkaTransactionManager);
        factory.setAfterRollbackProcessor(afterRollbackProcessor);
        return factory;
    }

    @Bean
    DefaultAfterRollbackProcessor<String, String> afterRollbackProcessor(
            KafkaTemplate<String, String> kafkaTemplate,
            @Value("${app.kafka.dlt-suffix}") String dltSuffix,
            @Value("${app.kafka.consumer-retry.interval}") Duration retryInterval,
            @Value("${app.kafka.consumer-retry.max-retries}") long maxRetries
    ) {
        var recoverer = new DeadLetterPublishingRecoverer(
                kafkaTemplate,
                (record, exception) -> new TopicPartition(
                        record.topic() + dltSuffix,
                        record.partition()));
        return new DefaultAfterRollbackProcessor<>(
                recoverer,
                new FixedBackOff(retryInterval.toMillis(), maxRetries),
                kafkaTemplate,
                true);
    }
}
