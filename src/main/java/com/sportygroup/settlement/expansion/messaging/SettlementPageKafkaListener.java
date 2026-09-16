package com.sportygroup.settlement.expansion.messaging;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sportygroup.settlement.expansion.model.SettlementPageTask;
import com.sportygroup.settlement.expansion.service.SettlementPageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

@Component
public class SettlementPageKafkaListener {

    private static final Logger log = LoggerFactory.getLogger(SettlementPageKafkaListener.class);

    private final ObjectMapper objectMapper;
    private final SettlementPageService service;

    public SettlementPageKafkaListener(ObjectMapper objectMapper, SettlementPageService service) {
        this.objectMapper = objectMapper;
        this.service = service;
    }

    @KafkaListener(
            topics = "${app.kafka.page-tasks-topic}",
            groupId = "settlement-page-workers",
            concurrency = "${app.expansion.concurrency}",
            containerFactory = "transactionalKafkaListenerContainerFactory",
            autoStartup = "${app.kafka.listener-enabled:true}")
    public void onMessage(String payload) {
        try {
            SettlementPageTask task = deserialize(payload);
            validate(task);
            log.info(
                    "[SETTLEMENT_PAGE_RECEIVED][EVENT_ID: {}][AFTER_BET_ID: {}]",
                    task.eventId(), task.afterBetId());
            service.process(task);
        } catch (MalformedKafkaMessageException exception) {
            log.warn(
                    "[SETTLEMENT_PAGE_INVALID][REASON: {}]",
                    exception.getMessage());
            throw exception;
        }
    }

    private SettlementPageTask deserialize(String payload) {
        try {
            return objectMapper.readValue(payload, SettlementPageTask.class);
        } catch (JsonProcessingException exception) {
            throw new MalformedKafkaMessageException("Cannot deserialize settlement page task", exception);
        }
    }

    private void validate(SettlementPageTask task) {
        if (!StringUtils.hasText(task.eventId())
                || !StringUtils.hasText(task.eventName())
                || !StringUtils.hasText(task.eventWinnerId())) {
            throw new MalformedKafkaMessageException("Settlement page task contains blank required fields");
        }
    }
}
