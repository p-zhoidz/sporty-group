package com.sportygroup.settlement.delivery.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sportygroup.settlement.bet.service.BetSettlementService;
import com.sportygroup.settlement.delivery.model.BetSettlementCommand;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.util.List;

@Component
@Profile("rocketmq")
public class RocketMqSettlementMessageListener implements MessageListenerConcurrently {

    private static final Logger log = LoggerFactory.getLogger(RocketMqSettlementMessageListener.class);

    private final ObjectMapper objectMapper;
    private final BetSettlementService settlementService;

    public RocketMqSettlementMessageListener(
            ObjectMapper objectMapper,
            BetSettlementService settlementService
    ) {
        this.objectMapper = objectMapper;
        this.settlementService = settlementService;
    }

    @Override
    public ConsumeConcurrentlyStatus consumeMessage(
            List<MessageExt> messages,
            ConsumeConcurrentlyContext context
    ) {
        for (MessageExt message : messages) {
            BetSettlementCommand command;
            try {
                command = deserialize(message);
                validate(command);
            } catch (IOException | IllegalArgumentException exception) {
                log.warn(
                        "Skipping invalid RocketMQ settlement messageId={} reason={}",
                        message.getMsgId(), exception.getMessage());
                continue;
            }

            try {
                var result = settlementService.settle(command.betId(), command.result());
                if (result == BetSettlementService.Result.MISSING
                        || result == BetSettlementService.Result.CONFLICT) {
                    log.warn(
                            "Skipping RocketMQ settlement betId={} result={} messageId={}",
                            command.betId(), result, message.getMsgId());
                    continue;
                }
                log.info(
                        "Handled RocketMQ settlement betId={} result={} messageId={}",
                        command.betId(), result, message.getMsgId());
            } catch (Exception exception) {
                log.error(
                        "Temporary RocketMQ settlement failure betId={} messageId={} reconsumeTimes={}",
                        command.betId(), message.getMsgId(), message.getReconsumeTimes(), exception);
                return ConsumeConcurrentlyStatus.RECONSUME_LATER;
            }
        }
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
    }

    private BetSettlementCommand deserialize(MessageExt message) throws IOException {
        return objectMapper.readValue(message.getBody(), BetSettlementCommand.class);
    }

    private void validate(BetSettlementCommand command) {
        if (!StringUtils.hasText(command.betId())
                || !StringUtils.hasText(command.eventId())
                || command.result() == null) {
            throw new IllegalArgumentException("Settlement command contains invalid required fields");
        }
    }
}
