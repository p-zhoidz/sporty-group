package com.sportygroup.settlement.delivery.transport;

import com.sportygroup.settlement.bet.service.BetSettlementService;
import com.sportygroup.settlement.delivery.model.BetSettlementCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Component
@Profile("in-memory & !rocketmq")
public class InMemorySettlementPublisher implements SettlementPublisher {

    private static final Logger log = LoggerFactory.getLogger(InMemorySettlementPublisher.class);

    private final BetSettlementService settlementService;

    public InMemorySettlementPublisher(BetSettlementService settlementService) {
        this.settlementService = settlementService;
    }

    @Override
    public void publish(BetSettlementCommand command) {
        log.info("[IN_MEMORY_SETTLEMENT_PUBLISHING][PAYLOAD: {}]", command);
        var result = settlementService.settle(command.betId(), command.result());
        log.info(
                "[IN_MEMORY_SETTLEMENT_HANDLED][BET_ID: {}][RESULT: {}]",
                command.betId(), result);
    }
}
