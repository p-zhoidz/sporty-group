package com.sportygroup.settlement.outcome.service;

import com.sportygroup.settlement.outcome.config.EventRelayShardProperties;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class OutcomeShardResolverTest {

    @Test
    void assignsTheSameEventToTheSameConfiguredShard() {
        var properties = new EventRelayShardProperties(16, 4, 1, 100);
        var resolver = new OutcomeShardResolver(properties);

        int first = resolver.resolve("event-1");
        int second = resolver.resolve("event-1");

        assertThat(first).isBetween(0, 15);
        assertThat(second).isEqualTo(first);
        assertThat(properties.ownedShards()).containsExactly(1, 5, 9, 13);
    }
}
