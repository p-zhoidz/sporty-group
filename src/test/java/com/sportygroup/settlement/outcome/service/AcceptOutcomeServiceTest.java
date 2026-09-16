package com.sportygroup.settlement.outcome.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sportygroup.settlement.outcome.model.EventOutcome;
import com.sportygroup.settlement.outcome.repository.EventOutboxRepository;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataIntegrityViolationException;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class AcceptOutcomeServiceTest {

    private final EventOutboxWriter writer = mock(EventOutboxWriter.class);
    private final EventOutboxRepository repository = mock(EventOutboxRepository.class);
    private final OutcomeShardResolver shardResolver = mock(OutcomeShardResolver.class);
    private final AcceptOutcomeService service = new AcceptOutcomeService(
            writer,
            repository,
            shardResolver,
            new ObjectMapper(),
            Clock.fixed(Instant.parse("2026-01-01T00:00:00Z"), ZoneOffset.UTC));

    AcceptOutcomeServiceTest() {
        when(shardResolver.resolve(any())).thenReturn(3);
    }

    @Test
    void acceptsFirstOutcome() {
        var result = service.accept(new EventOutcome("event-1", "A vs B", "A"));

        assertThat(result).isEqualTo(AcceptOutcomeService.Result.ACCEPTED);
    }

    @Test
    void mapsPrimaryKeyConflictToDuplicate() {
        doThrow(new DataIntegrityViolationException("duplicate"))
                .when(writer).insert(any(), anyInt(), any(), any());
        when(repository.existsById("event-1")).thenReturn(true);

        var result = service.accept(new EventOutcome("event-1", "A vs B", "A"));

        assertThat(result).isEqualTo(AcceptOutcomeService.Result.DUPLICATE);
    }

    @Test
    void doesNotHideUnrelatedDatabaseFailure() {
        doThrow(new DataIntegrityViolationException("database failure"))
                .when(writer).insert(any(), anyInt(), any(), any());
        when(repository.existsById("event-1")).thenReturn(false);

        assertThatThrownBy(() -> service.accept(new EventOutcome("event-1", "A vs B", "A")))
                .isInstanceOf(DataIntegrityViolationException.class);
    }
}
