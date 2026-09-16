package com.sportygroup.settlement.outcome.service;

import com.sportygroup.settlement.outcome.model.EventOutboxEntity;
import com.sportygroup.settlement.outcome.repository.EventOutboxRepository;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;

@Service
public class EventOutboxService {

    private final EventOutboxRepository repository;

    public EventOutboxService(EventOutboxRepository repository) {
        this.repository = repository;
    }

    @Transactional(readOnly = true)
    public List<EventOutboxEntity> findReady(List<Integer> ownedShards, Instant now, int batchSize) {
        if (ownedShards.isEmpty()) {
            return List.of();
        }
        return repository.findByShardIdInAndSentAtIsNullAndNextRetryAtLessThanEqualOrderByCreatedAtAsc(
                ownedShards, now, PageRequest.of(0, batchSize));
    }

    @Transactional
    public void markSent(String eventId, Instant sentAt) {
        repository.findById(eventId).ifPresent(row -> row.markSent(sentAt));
    }

    @Transactional
    public void scheduleRetry(String eventId, Instant nextRetryAt) {
        repository.findById(eventId).ifPresent(row -> row.scheduleRetry(nextRetryAt));
    }
}
