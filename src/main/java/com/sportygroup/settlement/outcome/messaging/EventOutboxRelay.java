package com.sportygroup.settlement.outcome.messaging;

import com.sportygroup.settlement.outcome.model.EventOutboxEntity;
import com.sportygroup.settlement.outcome.config.EventRelayShardProperties;
import com.sportygroup.settlement.outcome.service.EventOutboxService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.Duration;

@Component
public class EventOutboxRelay {

    private static final Logger log = LoggerFactory.getLogger(EventOutboxRelay.class);

    private final EventOutboxService outboxService;
    private final KafkaOutcomePublisher publisher;
    private final Clock clock;
    private final int batchSize;
    private final Duration retryDelay;
    private final EventRelayShardProperties shardProperties;

    public EventOutboxRelay(
            EventOutboxService outboxService,
            KafkaOutcomePublisher publisher,
            Clock clock,
            EventRelayShardProperties shardProperties,
            @Value("${app.retry.delay}") Duration retryDelay
    ) {
        this.outboxService = outboxService;
        this.publisher = publisher;
        this.clock = clock;
        this.shardProperties = shardProperties;
        this.batchSize = shardProperties.batchSize();
        this.retryDelay = retryDelay;
    }

    @Scheduled(fixedDelayString = "${app.event-relay.poll-interval-ms}")
    public void relay() {
        var rows = outboxService.findReady(
                shardProperties.ownedShards(), clock.instant(), batchSize);
        for (EventOutboxEntity row : rows) {
            publish(row);
        }
    }

    private void publish(EventOutboxEntity row) {
        try {
            publisher.publish(row);
            outboxService.markSent(row.getEventId(), clock.instant());
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            outboxService.scheduleRetry(row.getEventId(), clock.instant().plus(retryDelay));
        } catch (Exception exception) {
            log.warn("Failed to publish event outcome eventId={}", row.getEventId(), exception);
            outboxService.scheduleRetry(row.getEventId(), clock.instant().plus(retryDelay));
        }
    }
}
