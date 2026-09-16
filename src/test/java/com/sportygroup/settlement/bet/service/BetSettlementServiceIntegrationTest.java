package com.sportygroup.settlement.bet.service;

import com.sportygroup.settlement.bet.model.BetEntity;
import com.sportygroup.settlement.bet.model.BetStatus;
import com.sportygroup.settlement.bet.repository.BetRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

import static org.assertj.core.api.Assertions.assertThat;

@DataJpaTest
class BetSettlementServiceIntegrationTest {

    private static final Instant NOW = Instant.parse("2026-01-01T00:00:00Z");

    @Autowired
    BetRepository betRepository;

    @Test
    void settlementIsIdempotent() {
        betRepository.saveAndFlush(new BetEntity(
                "bet-0001", "user-1", "event-1", "winner", "team-a",
                new BigDecimal("10.00")));
        var service = new BetSettlementService(
                betRepository, Clock.fixed(NOW, ZoneOffset.UTC));

        assertThat(service.settle("bet-0001", BetStatus.WON))
                .isEqualTo(BetSettlementService.Result.APPLIED);
        assertThat(service.settle("bet-0001", BetStatus.WON))
                .isEqualTo(BetSettlementService.Result.DUPLICATE_OR_MISSING);
        assertThat(betRepository.findById("bet-0001").orElseThrow().getStatus())
                .isEqualTo(BetStatus.WON);
    }
}
