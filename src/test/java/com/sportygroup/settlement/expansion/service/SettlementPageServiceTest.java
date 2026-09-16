package com.sportygroup.settlement.expansion.service;

import com.sportygroup.settlement.bet.model.BetProjection;
import com.sportygroup.settlement.bet.service.BetQueryService;
import com.sportygroup.settlement.expansion.messaging.SettlementCommandPublisher;
import com.sportygroup.settlement.expansion.messaging.SettlementPageTaskPublisher;
import com.sportygroup.settlement.expansion.model.SettlementPageTask;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class SettlementPageServiceTest {

    private final BetQueryService betQueryService = mock(BetQueryService.class);
    private final SettlementCommandPublisher commandPublisher = mock(SettlementCommandPublisher.class);
    private final SettlementPageTaskPublisher taskPublisher = mock(SettlementPageTaskPublisher.class);
    private final SettlementPageService service = new SettlementPageService(
            betQueryService, commandPublisher, taskPublisher, 2);
    private final SettlementPageTask task = new SettlementPageTask(
            "event-1", "A vs B", "team-a", null);

    @Test
    void emptyPageCompletesEventWithoutPublishingMoreMessages() {
        when(betQueryService.findPage("event-1", null, 2)).thenReturn(List.of());

        service.process(task);

        verify(commandPublisher, never()).publish(any());
        verify(taskPublisher, never()).publish(any());
    }

    @Test
    void fullPagePublishesCommandsAndContinuation() {
        BetProjection first = bet("bet-1");
        BetProjection second = bet("bet-2");
        when(betQueryService.findPage("event-1", null, 2))
                .thenReturn(List.of(first, second));

        service.process(task);

        verify(commandPublisher, org.mockito.Mockito.times(2)).publish(any());
        verify(taskPublisher).publish(task.next("bet-2"));
    }

    @Test
    void partialPagePublishesCommandsWithoutContinuation() {
        BetProjection first = bet("bet-1");
        when(betQueryService.findPage("event-1", null, 2))
                .thenReturn(List.of(first));

        service.process(task);

        verify(commandPublisher).publish(any());
        verify(taskPublisher, never()).publish(any());
    }

    private BetProjection bet(String betId) {
        BetProjection bet = mock(BetProjection.class);
        when(bet.getBetId()).thenReturn(betId);
        when(bet.getUserId()).thenReturn("user-1");
        when(bet.getEventId()).thenReturn("event-1");
        when(bet.getEventMarketId()).thenReturn("winner");
        when(bet.getEventWinnerId()).thenReturn("team-a");
        when(bet.getBetAmount()).thenReturn(new BigDecimal("10.00"));
        return bet;
    }
}
