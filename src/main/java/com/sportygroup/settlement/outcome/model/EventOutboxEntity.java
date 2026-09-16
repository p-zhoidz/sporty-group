package com.sportygroup.settlement.outcome.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Lob;
import jakarta.persistence.PostLoad;
import jakarta.persistence.PostPersist;
import jakarta.persistence.Table;
import jakarta.persistence.Transient;
import org.springframework.data.domain.Persistable;

import java.time.Instant;

@Entity
@Table(name = "event_outbox", indexes = {
        @Index(name = "idx_event_outbox_ready", columnList = "shard_id, sent_at, next_retry_at, created_at")
})
public class EventOutboxEntity implements Persistable<String> {

    @Id
    @Column(name = "event_id", nullable = false, updatable = false)
    private String eventId;

    @Column(name = "shard_id", nullable = false, updatable = false)
    private int shardId;

    @Lob
    @Column(name = "payload", nullable = false)
    private String payload;

    @Column(name = "next_retry_at", nullable = false)
    private Instant nextRetryAt;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "sent_at")
    private Instant sentAt;

    @Transient
    private boolean newEntity = true;

    protected EventOutboxEntity() {
    }

    public EventOutboxEntity(String eventId, int shardId, String payload, Instant now) {
        this.eventId = eventId;
        this.shardId = shardId;
        this.payload = payload;
        this.nextRetryAt = now;
        this.createdAt = now;
    }

    public String getEventId() {
        return eventId;
    }

    @Override
    public String getId() {
        return eventId;
    }

    @Override
    public boolean isNew() {
        return newEntity;
    }

    @PostLoad
    @PostPersist
    void markNotNew() {
        newEntity = false;
    }

    public String getPayload() {
        return payload;
    }

    public int getShardId() {
        return shardId;
    }

    public Instant getNextRetryAt() {
        return nextRetryAt;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getSentAt() {
        return sentAt;
    }

    public void markSent(Instant at) {
        this.sentAt = at;
    }

    public void scheduleRetry(Instant at) {
        this.nextRetryAt = at;
    }
}
