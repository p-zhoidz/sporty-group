package com.sportygroup.settlement.expansion.model;

import com.sportygroup.settlement.bet.model.BetStatus;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SettlementDeciderTest {

    @Test
    void matchingWinnerWins() {
        assertThat(SettlementDecider.decide("team-a", "team-a")).isEqualTo(BetStatus.WON);
    }

    @Test
    void differentWinnerLoses() {
        assertThat(SettlementDecider.decide("team-b", "team-a")).isEqualTo(BetStatus.LOST);
    }
}
