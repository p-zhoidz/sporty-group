package com.sportygroup.settlement.outcome.service;

import com.sportygroup.settlement.outcome.messaging.KafkaOutcomePublisher;
import com.sportygroup.settlement.outcome.model.EventOutcome;
import org.springframework.stereotype.Service;

@Service
public class AcceptOutcomeService {

    private final KafkaOutcomePublisher publisher;

    public AcceptOutcomeService(KafkaOutcomePublisher publisher) {
        this.publisher = publisher;
    }

    public void accept(EventOutcome outcome) {
        publisher.publish(outcome);
    }
}
