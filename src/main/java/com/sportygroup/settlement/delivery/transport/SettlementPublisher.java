package com.sportygroup.settlement.delivery.transport;

import com.sportygroup.settlement.delivery.model.BetSettlementCommand;

public interface SettlementPublisher {
    void publish(BetSettlementCommand command);
}
