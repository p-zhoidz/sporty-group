package com.sportygroup.settlement.outcome.api;

import com.sportygroup.settlement.outcome.model.EventOutcome;
import jakarta.validation.constraints.NotBlank;

public record EventOutcomeRequest(
        @NotBlank String eventId,
        @NotBlank String eventName,
        @NotBlank String eventWinnerId
) {
    EventOutcome toDomain() {
        return new EventOutcome(eventId, eventName, eventWinnerId);
    }
}
