package com.sportygroup.settlement.delivery.transport;

import com.sportygroup.settlement.delivery.model.BetSettlementCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "app.delivery.transport", havingValue = "logging")
public class LoggingSettlementPublisher implements SettlementPublisher {

    private static final Logger log = LoggerFactory.getLogger(LoggingSettlementPublisher.class);

    @Override
    public void publish(BetSettlementCommand command) {
        log.info("Mock RocketMQ bet-settlements payload={}", command);
    }
}
