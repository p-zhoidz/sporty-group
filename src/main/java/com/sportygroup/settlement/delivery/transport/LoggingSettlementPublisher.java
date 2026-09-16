package com.sportygroup.settlement.delivery.transport;

import com.sportygroup.settlement.delivery.model.BetSettlementCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Component
@Profile("!rocketmq & !in-memory")
public class LoggingSettlementPublisher implements SettlementPublisher {

    private static final Logger log = LoggerFactory.getLogger(LoggingSettlementPublisher.class);

    @Override
    public void publish(BetSettlementCommand command) {
        log.info("Logging settlement payload={}", command);
    }
}
