package com.sportygroup.settlement.outcome.messaging;

import com.sportygroup.settlement.outcome.model.EventOutboxEntity;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

@Component
public class KafkaOutcomePublisher {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final String topic;

    public KafkaOutcomePublisher(
            KafkaTemplate<String, String> kafkaTemplate,
            @Value("${app.kafka.outcomes-topic}") String topic
    ) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
    }

    public void publish(EventOutboxEntity row) throws Exception {
        kafkaTemplate.send(topic, row.getEventId(), row.getPayload()).get(10, TimeUnit.SECONDS);
    }
}
