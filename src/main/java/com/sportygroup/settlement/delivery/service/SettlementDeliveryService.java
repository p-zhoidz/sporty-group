package com.sportygroup.settlement.delivery.service;

import com.sportygroup.settlement.delivery.model.BetSettlementCommand;
import com.sportygroup.settlement.delivery.transport.SettlementPublisher;
import org.springframework.stereotype.Service;

@Service
public class SettlementDeliveryService {

    private final SettlementPublisher publisher;

    public SettlementDeliveryService(SettlementPublisher publisher) {
        this.publisher = publisher;
    }

    public void deliver(BetSettlementCommand command) {
        publisher.publish(command);
    }
}
