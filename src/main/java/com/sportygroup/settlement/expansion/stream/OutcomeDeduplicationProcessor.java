package com.sportygroup.settlement.expansion.stream;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sportygroup.settlement.expansion.model.SettlementPageTask;
import com.sportygroup.settlement.outcome.model.EventOutcome;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

import java.nio.charset.StandardCharsets;

final class OutcomeDeduplicationProcessor
        implements Processor<String, String, String, OutcomeProcessingResult> {

    private static final Logger log = LoggerFactory.getLogger(OutcomeDeduplicationProcessor.class);
    private static final String ERROR_HEADER = "dlt-exception-message";

    private final ObjectMapper objectMapper;
    private ProcessorContext<String, OutcomeProcessingResult> context;
    private KeyValueStore<String, Long> processedEvents;

    OutcomeDeduplicationProcessor(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public void init(ProcessorContext<String, OutcomeProcessingResult> context) {
        this.context = context;
        this.processedEvents = context.getStateStore(
                OutcomeDeduplicationTopology.PROCESSED_EVENTS_STORE);
    }

    @Override
    public void process(Record<String, String> record) {
        SettlementPageTask task;
        try {
            task = toPageTask(record.key(), record.value());
        } catch (JsonProcessingException | IllegalArgumentException exception) {
            log.warn(
                    "[OUTCOME_ROUTED_TO_DLT][EVENT_ID: {}][REASON: {}]",
                    record.key(), exception.getMessage());
            forwardToDlt(record, exception.getMessage());
            return;
        }

        if (processedEvents.get(task.eventId()) != null) {
            log.info("[OUTCOME_DUPLICATE_SKIPPED][EVENT_ID: {}]", task.eventId());
            return;
        }

        String payload;
        try {
            payload = objectMapper.writeValueAsString(task);
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("Cannot serialize settlement page task", exception);
        }

        processedEvents.put(task.eventId(), record.timestamp());
        context.forward(record.withValue(new OutcomeProcessingResult(
                OutcomeProcessingResult.Route.PAGE_TASK, payload)));
        log.info("[SETTLEMENT_EXPANSION_STARTED][EVENT_ID: {}]", task.eventId());
    }

    private SettlementPageTask toPageTask(String key, String payload)
            throws JsonProcessingException {
        if (!StringUtils.hasText(key) || !StringUtils.hasText(payload)) {
            throw new IllegalArgumentException("Event outcome key and payload must be non-blank");
        }
        EventOutcome outcome = objectMapper.readValue(payload, EventOutcome.class);
        if (!StringUtils.hasText(outcome.eventId())
                || !StringUtils.hasText(outcome.eventName())
                || !StringUtils.hasText(outcome.eventWinnerId())) {
            throw new IllegalArgumentException("Event outcome contains blank required fields");
        }
        if (!key.equals(outcome.eventId())) {
            throw new IllegalArgumentException("Kafka key does not match outcome eventId");
        }
        return new SettlementPageTask(
                outcome.eventId(), outcome.eventName(), outcome.eventWinnerId(), null);
    }

    private void forwardToDlt(Record<String, String> record, String error) {
        var headers = new RecordHeaders(record.headers());
        String safeError = error == null ? "Unknown processing error" : error;
        headers.add(ERROR_HEADER, safeError.getBytes(StandardCharsets.UTF_8));
        context.forward(new Record<>(
                record.key(),
                new OutcomeProcessingResult(OutcomeProcessingResult.Route.DLT, record.value()),
                record.timestamp(),
                headers));
    }
}
