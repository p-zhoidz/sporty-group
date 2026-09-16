package com.sportygroup.settlement.delivery.messaging;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sportygroup.settlement.delivery.model.BetSettlementCommand;
import com.sportygroup.settlement.delivery.service.SettlementDeliveryService;
import com.sportygroup.settlement.expansion.messaging.MalformedKafkaMessageException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

@Component
public class SettlementCommandKafkaListener {

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
        BetSettlementCommand command = deserialize(payload);
        validate(command);
        service.deliver(command);
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
