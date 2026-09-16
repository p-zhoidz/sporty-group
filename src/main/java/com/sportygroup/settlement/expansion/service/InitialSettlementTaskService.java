package com.sportygroup.settlement.expansion.service;

import com.sportygroup.settlement.expansion.messaging.SettlementPageTaskPublisher;
import com.sportygroup.settlement.expansion.model.SettlementPageTask;
import com.sportygroup.settlement.outcome.model.EventOutcome;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class InitialSettlementTaskService {

    private final SettlementPageTaskPublisher publisher;

    public InitialSettlementTaskService(SettlementPageTaskPublisher publisher) {
        this.publisher = publisher;
    }

    @Transactional("kafkaTransactionManager")
    public void create(EventOutcome outcome) {
        publisher.publish(new SettlementPageTask(
                outcome.eventId(),
                outcome.eventName(),
                outcome.eventWinnerId(),
                null));
    }
}
