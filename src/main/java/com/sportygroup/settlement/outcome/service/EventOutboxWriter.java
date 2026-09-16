package com.sportygroup.settlement.outcome.service;

import com.sportygroup.settlement.outcome.model.EventOutboxEntity;
import com.sportygroup.settlement.outcome.repository.EventOutboxRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

@Service
class EventOutboxWriter {

    private final EventOutboxRepository repository;

    EventOutboxWriter(EventOutboxRepository repository) {
        this.repository = repository;
    }

    @Transactional
    void insert(String eventId, int shardId, String payload, Instant now) {
        repository.saveAndFlush(new EventOutboxEntity(eventId, shardId, payload, now));
    }
}
