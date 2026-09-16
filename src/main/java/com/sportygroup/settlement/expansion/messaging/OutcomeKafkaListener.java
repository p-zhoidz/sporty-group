package com.sportygroup.settlement.expansion.messaging;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sportygroup.settlement.outcome.model.EventOutcome;
import com.sportygroup.settlement.expansion.service.InitialSettlementTaskService;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

@Component
public class OutcomeKafkaListener {

    private final ObjectMapper objectMapper;
    private final InitialSettlementTaskService taskService;

    public OutcomeKafkaListener(ObjectMapper objectMapper, InitialSettlementTaskService taskService) {
        this.objectMapper = objectMapper;
        this.taskService = taskService;
    }

    @KafkaListener(
            topics = "${app.kafka.outcomes-topic}",
            groupId = "outcome-task-producers",
            containerFactory = "transactionalKafkaListenerContainerFactory",
            autoStartup = "${app.kafka.listener-enabled:true}")
    public void onMessage(String payload) {
        EventOutcome outcome = deserialize(payload);
        validate(outcome);
        taskService.create(outcome);
    }

    private EventOutcome deserialize(String payload) {
        try {
            return objectMapper.readValue(payload, EventOutcome.class);
        } catch (JsonProcessingException exception) {
            throw new MalformedKafkaMessageException("Cannot deserialize event outcome", exception);
        }
    }

    private void validate(EventOutcome outcome) {
        if (!StringUtils.hasText(outcome.eventId())
                || !StringUtils.hasText(outcome.eventName())
                || !StringUtils.hasText(outcome.eventWinnerId())) {
            throw new MalformedKafkaMessageException("Event outcome contains blank required fields");
        }
    }
}
