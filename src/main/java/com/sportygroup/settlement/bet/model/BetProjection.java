package com.sportygroup.settlement.bet.model;

import java.math.BigDecimal;

public interface BetProjection {
    String getBetId();

    String getUserId();

    String getEventId();

    String getEventMarketId();

    String getEventWinnerId();

    BigDecimal getBetAmount();
}
