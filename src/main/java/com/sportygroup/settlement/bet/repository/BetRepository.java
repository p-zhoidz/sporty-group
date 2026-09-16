package com.sportygroup.settlement.bet.repository;

import com.sportygroup.settlement.bet.model.BetEntity;
import com.sportygroup.settlement.bet.model.BetProjection;
import com.sportygroup.settlement.bet.model.BetStatus;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.List;

public interface BetRepository extends JpaRepository<BetEntity, String> {

    List<BetProjection> findByEventIdOrderByBetIdAsc(String eventId, Pageable pageable);

    List<BetProjection> findByEventIdAndBetIdGreaterThanOrderByBetIdAsc(
            String eventId, String betId, Pageable pageable);

    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("""
            update BetEntity b
               set b.status = :result, b.settledAt = :settledAt
             where b.betId = :betId and b.status = com.sportygroup.settlement.bet.model.BetStatus.PENDING
            """)
    int settleIfPending(@Param("betId") String betId,
                        @Param("result") BetStatus result,
                        @Param("settledAt") Instant settledAt);
}
