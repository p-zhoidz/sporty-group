package com.sportygroup.settlement.delivery.transport;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sportygroup.settlement.delivery.config.RocketMqProperties;
import com.sportygroup.settlement.delivery.model.BetSettlementCommand;
import com.sportygroup.settlement.delivery.service.SettlementPublishException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.Message;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Component
@Profile("rocketmq")
public class RocketMqSettlementPublisher implements SettlementPublisher {

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
            var result = producer.send(message, properties.sendTimeout().toMillis());
            if (result.getSendStatus() != SendStatus.SEND_OK) {
                throw new SettlementPublishException(
                        "RocketMQ rejected settlement for bet " + command.betId()
                                + " with status " + result.getSendStatus());
            }
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new SettlementPublishException(
                    "Interrupted while publishing settlement for bet " + command.betId(), exception);
        } catch (SettlementPublishException exception) {
            throw exception;
        } catch (Exception exception) {
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
