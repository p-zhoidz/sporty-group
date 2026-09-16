package com.sportygroup.settlement.delivery.transport;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sportygroup.settlement.bet.model.BetStatus;
import com.sportygroup.settlement.delivery.config.RocketMqProperties;
import com.sportygroup.settlement.delivery.model.BetSettlementCommand;
import com.sportygroup.settlement.delivery.service.SettlementPublishException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.Message;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.math.BigDecimal;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class RocketMqSettlementPublisherTest {

    private final DefaultMQProducer producer = mock(DefaultMQProducer.class);
    private final ObjectMapper objectMapper = new ObjectMapper().findAndRegisterModules();
    private final RocketMqProperties properties = new RocketMqProperties(
            "localhost:9876",
            "bet-settlements",
            "producer-group",
            "consumer-group",
            Duration.ofSeconds(10),
            3,
            3,
            true);
    private final RocketMqSettlementPublisher publisher =
            new RocketMqSettlementPublisher(producer, objectMapper, properties);

    @Test
    void publishesSettlementUsingBetIdAsBusinessKey() throws Exception {
        SendResult result = mock(SendResult.class);
        when(result.getSendStatus()).thenReturn(SendStatus.SEND_OK);
        when(producer.send(any(Message.class), eq(10_000L))).thenReturn(result);

        publisher.publish(command());

        ArgumentCaptor<Message> message = ArgumentCaptor.forClass(Message.class);
        verify(producer).send(message.capture(), eq(10_000L));
        assertThat(message.getValue().getTopic()).isEqualTo("bet-settlements");
        assertThat(message.getValue().getKeys()).isEqualTo("bet-001");
        assertThat(objectMapper.readValue(message.getValue().getBody(), BetSettlementCommand.class))
                .isEqualTo(command());
    }

    @Test
    void rejectsNonSuccessfulBrokerAcknowledgement() throws Exception {
        SendResult result = mock(SendResult.class);
        when(result.getSendStatus()).thenReturn(SendStatus.FLUSH_DISK_TIMEOUT);
        when(producer.send(any(Message.class), eq(10_000L))).thenReturn(result);

        assertThatThrownBy(() -> publisher.publish(command()))
                .isInstanceOf(SettlementPublishException.class)
                .hasMessageContaining("FLUSH_DISK_TIMEOUT");
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
