package com.sportygroup.settlement.outcome.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;
import java.util.stream.IntStream;

@ConfigurationProperties(prefix = "app.event-relay")
public record EventRelayShardProperties(
        int shardCount,
        int instanceCount,
        int instanceIndex,
        int batchSize
) {
    public EventRelayShardProperties {
        if (shardCount <= 0) {
            throw new IllegalArgumentException("app.event-relay.shard-count must be positive");
        }
        if (instanceCount <= 0) {
            throw new IllegalArgumentException("app.event-relay.instance-count must be positive");
        }
        if (instanceIndex < 0 || instanceIndex >= instanceCount) {
            throw new IllegalArgumentException(
                    "app.event-relay.instance-index must be between 0 and instance-count - 1");
        }
        if (batchSize <= 0) {
            throw new IllegalArgumentException("app.event-relay.batch-size must be positive");
        }
    }

    public List<Integer> ownedShards() {
        return IntStream.range(0, shardCount)
                .filter(shard -> shard % instanceCount == instanceIndex)
                .boxed()
                .toList();
    }
}
