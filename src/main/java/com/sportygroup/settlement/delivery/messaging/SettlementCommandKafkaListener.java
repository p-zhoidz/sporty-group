package com.sportygroup.settlement.delivery.messaging;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sportygroup.settlement.delivery.model.BetSettlementCommand;
import com.sportygroup.settlement.delivery.service.SettlementDeliveryService;
import com.sportygroup.settlement.expansion.messaging.MalformedKafkaMessageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

@Component
public class SettlementCommandKafkaListener {

    private static final Logger log = LoggerFactory.getLogger(SettlementCommandKafkaListener.class);

    private final ObjectMapper objectMapper;
    private final SettlementDeliveryService service;

    public SettlementCommandKafkaListener(ObjectMapper objectMapper, SettlementDeliveryService service) {
        this.objectMapper = objectMapper;
        this.service = service;
    }

    @KafkaListener(
            topics = "${app.kafka.settlement-commands-topic}",
            groupId = "settlement-bridge",
            concurrency = "${app.delivery.concurrency}",
            containerFactory = "transactionalKafkaListenerContainerFactory",
            autoStartup = "${app.kafka.listener-enabled:true}")
    public void onMessage(String payload) {
        try {
            BetSettlementCommand command = deserialize(payload);
            validate(command);
            log.debug(
                    "[KAFKA_SETTLEMENT_COMMAND_RECEIVED][EVENT_ID: {}][BET_ID: {}][RESULT: {}]",
                    command.eventId(), command.betId(), command.result());
            service.deliver(command);
        } catch (MalformedKafkaMessageException exception) {
            log.warn(
                    "[KAFKA_SETTLEMENT_COMMAND_INVALID][REASON: {}]",
                    exception.getMessage());
            throw exception;
        }
    }

    private BetSettlementCommand deserialize(String payload) {
        try {
            return objectMapper.readValue(payload, BetSettlementCommand.class);
        } catch (JsonProcessingException exception) {
            throw new MalformedKafkaMessageException("Cannot deserialize settlement command", exception);
        }
    }

    private void validate(BetSettlementCommand command) {
        if (!StringUtils.hasText(command.betId())
                || !StringUtils.hasText(command.eventId())
                || command.result() == null) {
            throw new MalformedKafkaMessageException("Settlement command contains invalid required fields");
        }
    }
}
