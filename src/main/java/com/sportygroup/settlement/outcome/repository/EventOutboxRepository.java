package com.sportygroup.settlement.outcome.repository;

import com.sportygroup.settlement.outcome.model.EventOutboxEntity;

import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;

import java.time.Instant;
import java.util.List;

public interface EventOutboxRepository extends JpaRepository<EventOutboxEntity, String> {
    List<EventOutboxEntity> findByShardIdInAndSentAtIsNullAndNextRetryAtLessThanEqualOrderByCreatedAtAsc(
            List<Integer> shardIds, Instant now, Pageable pageable);
}
