package com.sportygroup.settlement.delivery.service;

import com.sportygroup.settlement.delivery.model.BetSettlementCommand;
import com.sportygroup.settlement.delivery.transport.SettlementPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class SettlementDeliveryService {

    private static final Logger log = LoggerFactory.getLogger(SettlementDeliveryService.class);

    private final SettlementPublisher publisher;

    public SettlementDeliveryService(SettlementPublisher publisher) {
        this.publisher = publisher;
    }

    public void deliver(BetSettlementCommand command) {
        log.debug(
                "[SETTLEMENT_DELIVERY_STARTED][EVENT_ID: {}][BET_ID: {}][RESULT: {}]",
                command.eventId(), command.betId(), command.result());
        publisher.publish(command);
        log.debug(
                "[SETTLEMENT_DELIVERY_COMPLETED][EVENT_ID: {}][BET_ID: {}][RESULT: {}]",
                command.eventId(), command.betId(), command.result());
    }
}
