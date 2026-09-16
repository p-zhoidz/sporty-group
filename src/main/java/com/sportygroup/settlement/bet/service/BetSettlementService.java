package com.sportygroup.settlement.bet.service;

import com.sportygroup.settlement.bet.model.BetStatus;
import com.sportygroup.settlement.bet.repository.BetRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;

@Service
public class BetSettlementService {

    private static final Logger log = LoggerFactory.getLogger(BetSettlementService.class);

    public enum Result {
        APPLIED,
        DUPLICATE,
        MISSING,
        CONFLICT
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
        Result settlementResult;
        if (affected == 1) {
            settlementResult = Result.APPLIED;
        } else {
            settlementResult = betRepository.findById(betId)
                    .map(bet -> bet.getStatus() == result ? Result.DUPLICATE : Result.CONFLICT)
                    .orElse(Result.MISSING);
        }

        log.debug(
                "[BET_SETTLEMENT_PROCESSED][BET_ID: {}][REQUESTED_STATUS: {}][RESULT: {}]",
                betId, result, settlementResult);
        return settlementResult;
    }
}
