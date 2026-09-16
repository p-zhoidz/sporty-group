package com.sportygroup.settlement.expansion.model;

public record SettlementPageTask(
        String eventId,
        String eventName,
        String eventWinnerId,
        String afterBetId
) {
    public SettlementPageTask next(String lastBetId) {
        return new SettlementPageTask(eventId, eventName, eventWinnerId, lastBetId);
    }
}
