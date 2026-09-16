package com.sportygroup.settlement.expansion.messaging;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sportygroup.settlement.delivery.model.BetSettlementCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class SettlementCommandPublisher {

    private static final Logger log = LoggerFactory.getLogger(SettlementCommandPublisher.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final String topic;

    public SettlementCommandPublisher(
            KafkaTemplate<String, String> kafkaTemplate,
            ObjectMapper objectMapper,
            @Value("${app.kafka.settlement-commands-topic}") String topic
    ) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.topic = topic;
    }

    public void publish(BetSettlementCommand command) {
        kafkaTemplate.send(topic, command.betId(), serialize(command));
        log.debug(
                "[KAFKA_SETTLEMENT_COMMAND_QUEUED][EVENT_ID: {}][BET_ID: {}][RESULT: {}][TOPIC: {}]",
                command.eventId(), command.betId(), command.result(), topic);
    }

    private String serialize(BetSettlementCommand command) {
        try {
            return objectMapper.writeValueAsString(command);
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException(
                    "Cannot serialize settlement command for bet " + command.betId(), exception);
        }
    }
}
