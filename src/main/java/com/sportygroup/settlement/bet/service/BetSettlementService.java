package com.sportygroup.settlement.bet.service;

import com.sportygroup.settlement.bet.model.BetStatus;
import com.sportygroup.settlement.bet.repository.BetRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;

@Service
public class BetSettlementService {

    public enum Result {
        APPLIED,
        DUPLICATE_OR_MISSING
    }

    private final BetRepository betRepository;
    private final Clock clock;

    public BetSettlementService(BetRepository betRepository, Clock clock) {
        this.betRepository = betRepository;
        this.clock = clock;
    }

    @Transactional
    public Result settle(String betId, BetStatus result) {
        int affected = betRepository.settleIfPending(betId, result, clock.instant());
        return affected == 1 ? Result.APPLIED : Result.DUPLICATE_OR_MISSING;
    }
}
