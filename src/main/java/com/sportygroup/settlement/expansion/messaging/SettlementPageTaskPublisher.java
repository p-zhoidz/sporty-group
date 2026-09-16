package com.sportygroup.settlement.expansion.messaging;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sportygroup.settlement.expansion.model.SettlementPageTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class SettlementPageTaskPublisher {

    private static final Logger log = LoggerFactory.getLogger(SettlementPageTaskPublisher.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final String topic;

    public SettlementPageTaskPublisher(
            KafkaTemplate<String, String> kafkaTemplate,
            ObjectMapper objectMapper,
            @Value("${app.kafka.page-tasks-topic}") String topic
    ) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.topic = topic;
    }

    public void publish(SettlementPageTask task) {
        kafkaTemplate.send(topic, task.eventId(), serialize(task));
        log.info(
                "[KAFKA_SETTLEMENT_PAGE_QUEUED][EVENT_ID: {}][AFTER_BET_ID: {}][TOPIC: {}]",
                task.eventId(), task.afterBetId(), topic);
    }

    private String serialize(SettlementPageTask task) {
        try {
            return objectMapper.writeValueAsString(task);
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("Cannot serialize settlement page task", exception);
        }
    }
}
