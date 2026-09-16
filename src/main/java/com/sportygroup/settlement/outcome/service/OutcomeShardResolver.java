package com.sportygroup.settlement.outcome.service;

import com.sportygroup.settlement.outcome.config.EventRelayShardProperties;
import org.springframework.stereotype.Service;

@Service
public class OutcomeShardResolver {

    private final int shardCount;

    public OutcomeShardResolver(EventRelayShardProperties properties) {
        this.shardCount = properties.shardCount();
    }

    public int resolve(String eventId) {
        return Math.floorMod(eventId.hashCode(), shardCount);
    }
}
