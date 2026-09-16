package com.sportygroup.settlement.expansion.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sportygroup.settlement.expansion.service.InitialSettlementTaskService;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class OutcomeKafkaListenerTest {

    private final InitialSettlementTaskService service = mock(InitialSettlementTaskService.class);
    private final OutcomeKafkaListener listener = new OutcomeKafkaListener(new ObjectMapper(), service);

    @Test
    void createsInitialPageTaskForValidOutcome() {
        listener.onMessage("""
                {"eventId":"event-1","eventName":"A vs B","eventWinnerId":"team-a"}
                """);

        verify(service).create(any());
    }

    @Test
    void rejectsMalformedPayload() {
        assertThatThrownBy(() -> listener.onMessage("not-json"))
                .isInstanceOf(MalformedKafkaMessageException.class);

        verify(service, never()).create(any());
    }
}
