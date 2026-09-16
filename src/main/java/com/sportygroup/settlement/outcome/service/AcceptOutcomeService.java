package com.sportygroup.settlement.outcome.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sportygroup.settlement.outcome.model.EventOutcome;
import com.sportygroup.settlement.outcome.repository.EventOutboxRepository;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;

import java.time.Clock;

@Service
public class AcceptOutcomeService {

    public enum Result {
        ACCEPTED,
        DUPLICATE
    }

    private final EventOutboxWriter writer;
    private final EventOutboxRepository repository;
    private final OutcomeShardResolver shardResolver;
    private final ObjectMapper objectMapper;
    private final Clock clock;

    public AcceptOutcomeService(
            EventOutboxWriter writer,
            EventOutboxRepository repository,
            OutcomeShardResolver shardResolver,
            ObjectMapper objectMapper,
            Clock clock
    ) {
        this.writer = writer;
        this.repository = repository;
        this.shardResolver = shardResolver;
        this.objectMapper = objectMapper;
        this.clock = clock;
    }

    public Result accept(EventOutcome outcome) {
        String payload = serialize(outcome);
        try {
            writer.insert(
                    outcome.eventId(),
                    shardResolver.resolve(outcome.eventId()),
                    payload,
                    clock.instant());
            return Result.ACCEPTED;
        } catch (DataIntegrityViolationException duplicate) {
            if (repository.existsById(outcome.eventId())) {
                return Result.DUPLICATE;
            }
            throw duplicate;
        }
    }

    private String serialize(EventOutcome outcome) {
        try {
            return objectMapper.writeValueAsString(outcome);
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("Cannot serialize a validated event outcome", exception);
        }
    }
}
