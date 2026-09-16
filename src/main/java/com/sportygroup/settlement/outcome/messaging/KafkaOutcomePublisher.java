package com.sportygroup.settlement.outcome.messaging;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sportygroup.settlement.outcome.model.EventOutcome;
import com.sportygroup.settlement.outcome.service.OutcomePublishException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Component
public class KafkaOutcomePublisher {

    private static final Logger log = LoggerFactory.getLogger(KafkaOutcomePublisher.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final String topic;
    private final Duration timeout;

    public KafkaOutcomePublisher(
            KafkaTemplate<String, String> kafkaTemplate,
            ObjectMapper objectMapper,
            @Value("${app.kafka.outcomes-topic}") String topic,
            @Value("${app.kafka.publish-timeout}") Duration timeout
    ) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.topic = topic;
        this.timeout = timeout;
    }

    public void publish(EventOutcome outcome) {
        try {
            var result = kafkaTemplate.send(topic, outcome.eventId(), serialize(outcome))
                    .get(timeout.toMillis(), TimeUnit.MILLISECONDS);
            var metadata = result.getRecordMetadata();
            log.info(
                    "[KAFKA_OUTCOME_PUBLISHED][EVENT_ID: {}][TOPIC: {}][PARTITION: {}][OFFSET: {}]",
                    outcome.eventId(), metadata.topic(), metadata.partition(), metadata.offset());
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            log.error(
                    "[KAFKA_OUTCOME_PUBLISH_INTERRUPTED][EVENT_ID: {}]",
                    outcome.eventId(), exception);
            throw new OutcomePublishException("Interrupted while publishing event outcome", exception);
        } catch (ExecutionException | TimeoutException exception) {
            log.error(
                    "[KAFKA_OUTCOME_PUBLISH_FAILED][EVENT_ID: {}][REASON: {}]",
                    outcome.eventId(), exception.getMessage(), exception);
            throw new OutcomePublishException("Kafka did not acknowledge event outcome", exception);
        }
    }

    private String serialize(EventOutcome outcome) {
        try {
            return objectMapper.writeValueAsString(outcome);
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("Cannot serialize validated event outcome", exception);
        }
    }
}
