package com.sportygroup.settlement.bet.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;

import java.math.BigDecimal;
import java.time.Instant;

@Entity
@Table(name = "bets", indexes = {
        @Index(name = "idx_bets_event_bet", columnList = "event_id, bet_id")
})
public class BetEntity {

    @Id
    @Column(name = "bet_id", nullable = false, updatable = false)
    private String betId;

    @Column(name = "user_id", nullable = false)
    private String userId;

    @Column(name = "event_id", nullable = false, updatable = false)
    private String eventId;

    @Column(name = "event_market_id", nullable = false)
    private String eventMarketId;

    @Column(name = "event_winner_id", nullable = false)
    private String eventWinnerId;

    @Column(name = "bet_amount", nullable = false, precision = 19, scale = 2)
    private BigDecimal betAmount;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false)
    private BetStatus status;

    @Column(name = "settled_at")
    private Instant settledAt;

    protected BetEntity() {
    }

    public BetEntity(String betId, String userId, String eventId, String eventMarketId,
                     String eventWinnerId, BigDecimal betAmount) {
        this.betId = betId;
        this.userId = userId;
        this.eventId = eventId;
        this.eventMarketId = eventMarketId;
        this.eventWinnerId = eventWinnerId;
        this.betAmount = betAmount;
        this.status = BetStatus.PENDING;
    }

    public String getBetId() {
        return betId;
    }

    public String getUserId() {
        return userId;
    }

    public String getEventId() {
        return eventId;
    }

    public String getEventMarketId() {
        return eventMarketId;
    }

    public String getEventWinnerId() {
        return eventWinnerId;
    }

    public BigDecimal getBetAmount() {
        return betAmount;
    }

    public BetStatus getStatus() {
        return status;
    }

    public Instant getSettledAt() {
        return settledAt;
    }
}
