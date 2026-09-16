package com.sportygroup.settlement.delivery.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sportygroup.settlement.bet.model.BetStatus;
import com.sportygroup.settlement.bet.service.BetSettlementService;
import com.sportygroup.settlement.delivery.model.BetSettlementCommand;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.common.message.MessageExt;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.math.BigDecimal;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class RocketMqSettlementMessageListenerTest {

    private final ObjectMapper objectMapper = new ObjectMapper().findAndRegisterModules();
    private final BetSettlementService settlementService = mock(BetSettlementService.class);
    private final RocketMqSettlementMessageListener listener =
            new RocketMqSettlementMessageListener(objectMapper, settlementService);

    @Test
    void acknowledgesAppliedSettlement() throws Exception {
        when(settlementService.settle("bet-001", BetStatus.WON))
                .thenReturn(BetSettlementService.Result.APPLIED);

        var result = listener.consumeMessage(List.of(message(command())), null);

        assertThat(result).isEqualTo(ConsumeConcurrentlyStatus.CONSUME_SUCCESS);
        verify(settlementService).settle("bet-001", BetStatus.WON);
    }

    @Test
    void acknowledgesDuplicateSettlement() throws Exception {
        when(settlementService.settle("bet-001", BetStatus.WON))
                .thenReturn(BetSettlementService.Result.DUPLICATE);

        var result = listener.consumeMessage(List.of(message(command())), null);

        assertThat(result).isEqualTo(ConsumeConcurrentlyStatus.CONSUME_SUCCESS);
        verify(settlementService).settle("bet-001", BetStatus.WON);
    }

    @ParameterizedTest
    @EnumSource(
            value = BetSettlementService.Result.class,
            names = {"MISSING", "CONFLICT"})
    void acknowledgesUnrecoverableBusinessResultWithoutRetry(
            BetSettlementService.Result settlementResult
    ) throws Exception {
        when(settlementService.settle("bet-001", BetStatus.WON))
                .thenReturn(settlementResult);

        var result = listener.consumeMessage(List.of(message(command())), null);

        assertThat(result).isEqualTo(ConsumeConcurrentlyStatus.CONSUME_SUCCESS);
        verify(settlementService).settle("bet-001", BetStatus.WON);
    }

    @Test
    void acknowledgesMalformedMessageWithoutRetry() {
        var message = new MessageExt();
        message.setBody("not-json".getBytes());

        var result = listener.consumeMessage(List.of(message), null);

        assertThat(result).isEqualTo(ConsumeConcurrentlyStatus.CONSUME_SUCCESS);
        verifyNoInteractions(settlementService);
    }

    @Test
    void requestsRetryForTemporaryProcessingFailure() throws Exception {
        when(settlementService.settle("bet-001", BetStatus.WON))
                .thenThrow(new RuntimeException("Database is temporarily unavailable"));

        var result = listener.consumeMessage(List.of(message(command())), null);

        assertThat(result).isEqualTo(ConsumeConcurrentlyStatus.RECONSUME_LATER);
        verify(settlementService).settle("bet-001", BetStatus.WON);
    }

    private MessageExt message(BetSettlementCommand command) throws Exception {
        var message = new MessageExt();
        message.setBody(objectMapper.writeValueAsBytes(command));
        message.setMsgId("message-001");
        return message;
    }

    private BetSettlementCommand command() {
        return new BetSettlementCommand(
                "bet-001",
                "user-001",
                "event-123",
                "A vs B",
                "winner",
                "team-a",
                new BigDecimal("10.00"),
                BetStatus.WON);
    }
}
