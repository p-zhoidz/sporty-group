package com.sportygroup.settlement.expansion.model;

import com.sportygroup.settlement.bet.model.BetStatus;
import java.util.Objects;

public final class SettlementDecider {

    private SettlementDecider() {
    }

    public static BetStatus decide(String predictedWinnerId, String actualWinnerId) {
        return Objects.equals(predictedWinnerId, actualWinnerId) ? BetStatus.WON : BetStatus.LOST;
    }
}
