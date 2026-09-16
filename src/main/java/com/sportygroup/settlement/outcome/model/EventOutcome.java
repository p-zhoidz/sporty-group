package com.sportygroup.settlement.outcome.model;

public record EventOutcome(
        String eventId,
        String eventName,
        String eventWinnerId
) {
}
