package com.sportygroup.settlement.expansion.service;

import com.sportygroup.settlement.bet.model.BetProjection;
import com.sportygroup.settlement.bet.service.BetQueryService;
import com.sportygroup.settlement.delivery.model.BetSettlementCommand;
import com.sportygroup.settlement.expansion.messaging.SettlementCommandPublisher;
import com.sportygroup.settlement.expansion.messaging.SettlementPageTaskPublisher;
import com.sportygroup.settlement.expansion.model.SettlementDecider;
import com.sportygroup.settlement.expansion.model.SettlementPageTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
public class SettlementPageService {

    private static final Logger log = LoggerFactory.getLogger(SettlementPageService.class);

    private final BetQueryService betQueryService;
    private final SettlementCommandPublisher commandPublisher;
    private final SettlementPageTaskPublisher taskPublisher;
    private final int pageSize;

    public SettlementPageService(
            BetQueryService betQueryService,
            SettlementCommandPublisher commandPublisher,
            SettlementPageTaskPublisher taskPublisher,
            @Value("${app.expansion.page-size}") int pageSize
    ) {
        this.betQueryService = betQueryService;
        this.commandPublisher = commandPublisher;
        this.taskPublisher = taskPublisher;
        this.pageSize = pageSize;
    }

    @Transactional("kafkaTransactionManager")
    public void process(SettlementPageTask task) {
        List<BetProjection> bets = betQueryService.findPage(
                task.eventId(), task.afterBetId(), pageSize);

        log.info(
                "[SETTLEMENT_PAGE_LOADED][EVENT_ID: {}][AFTER_BET_ID: {}][BET_COUNT: {}]",
                task.eventId(), task.afterBetId(), bets.size());

        bets.stream()
                .map(bet -> toCommand(task, bet))
                .forEach(commandPublisher::publish);

        if (bets.size() == pageSize) {
            taskPublisher.publish(task.next(bets.getLast().getBetId()));
        }

        log.info(
                "[SETTLEMENT_PAGE_PROCESSED][EVENT_ID: {}][AFTER_BET_ID: {}]"
                        + "[BET_COUNT: {}][HAS_NEXT_PAGE: {}]",
                task.eventId(), task.afterBetId(), bets.size(), bets.size() == pageSize);
    }

    private BetSettlementCommand toCommand(SettlementPageTask task, BetProjection bet) {
        return new BetSettlementCommand(
                bet.getBetId(),
                bet.getUserId(),
                bet.getEventId(),
                task.eventName(),
                bet.getEventMarketId(),
                bet.getEventWinnerId(),
                bet.getBetAmount(),
                SettlementDecider.decide(bet.getEventWinnerId(), task.eventWinnerId()));
    }
}
