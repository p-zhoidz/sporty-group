package com.sportygroup.settlement.delivery.model;

import com.sportygroup.settlement.bet.model.BetStatus;
import java.math.BigDecimal;

public record BetSettlementCommand(
        String betId,
        String userId,
        String eventId,
        String eventName,
        String eventMarketId,
        String eventWinnerId,
        BigDecimal betAmount,
        BetStatus result
) {
}
