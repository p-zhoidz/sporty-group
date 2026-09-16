package com.sportygroup.settlement.delivery.transport;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sportygroup.settlement.delivery.config.RocketMqProperties;
import com.sportygroup.settlement.delivery.model.BetSettlementCommand;
import com.sportygroup.settlement.delivery.service.SettlementPublishException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Component
@Profile("rocketmq")
public class RocketMqSettlementPublisher implements SettlementPublisher {

    private static final Logger log = LoggerFactory.getLogger(RocketMqSettlementPublisher.class);

    private final DefaultMQProducer producer;
    private final ObjectMapper objectMapper;
    private final RocketMqProperties properties;

    public RocketMqSettlementPublisher(
            DefaultMQProducer producer,
            ObjectMapper objectMapper,
            RocketMqProperties properties
    ) {
        this.producer = producer;
        this.objectMapper = objectMapper;
        this.properties = properties;
    }

    @Override
    public void publish(BetSettlementCommand command) {
        var message = new Message(properties.topic(), serialize(command));
        message.setKeys(command.betId());

        try {
            log.debug(
                    "[ROCKETMQ_SETTLEMENT_SEND_STARTED][EVENT_ID: {}][BET_ID: {}][TOPIC: {}]",
                    command.eventId(), command.betId(), properties.topic());
            var result = producer.send(message, properties.sendTimeout().toMillis());
            if (result.getSendStatus() != SendStatus.SEND_OK) {
                log.warn(
                        "[ROCKETMQ_SETTLEMENT_SEND_REJECTED][EVENT_ID: {}][BET_ID: {}]"
                                + "[TOPIC: {}][STATUS: {}]",
                        command.eventId(), command.betId(), properties.topic(), result.getSendStatus());
                throw new SettlementPublishException(
                        "RocketMQ rejected settlement for bet " + command.betId()
                                + " with status " + result.getSendStatus());
            }
            log.debug(
                    "[ROCKETMQ_SETTLEMENT_SENT][EVENT_ID: {}][BET_ID: {}]"
                            + "[TOPIC: {}][MESSAGE_ID: {}]",
                    command.eventId(), command.betId(), properties.topic(), result.getMsgId());
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            log.error(
                    "[ROCKETMQ_SETTLEMENT_SEND_INTERRUPTED][EVENT_ID: {}][BET_ID: {}][TOPIC: {}]",
                    command.eventId(), command.betId(), properties.topic(), exception);
            throw new SettlementPublishException(
                    "Interrupted while publishing settlement for bet " + command.betId(), exception);
        } catch (SettlementPublishException exception) {
            throw exception;
        } catch (Exception exception) {
            log.error(
                    "[ROCKETMQ_SETTLEMENT_SEND_FAILED][EVENT_ID: {}][BET_ID: {}]"
                            + "[TOPIC: {}][REASON: {}]",
                    command.eventId(), command.betId(), properties.topic(),
                    exception.getMessage(), exception);
            throw new SettlementPublishException(
                    "Cannot publish settlement for bet " + command.betId(), exception);
        }
    }

    private byte[] serialize(BetSettlementCommand command) {
        try {
            return objectMapper.writeValueAsBytes(command);
        } catch (JsonProcessingException exception) {
            throw new SettlementPublishException(
                    "Cannot serialize settlement for bet " + command.betId(), exception);
        }
    }
}
