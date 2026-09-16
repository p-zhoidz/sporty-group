package com.sportygroup.settlement.expansion.stream;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.Stores;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@Configuration
@EnableKafkaStreams
public class OutcomeDeduplicationTopology {

    static final String PROCESSED_EVENTS_STORE = "processed-event-outcomes";

    @Bean
    KStream<String, OutcomeProcessingResult> outcomeDeduplicationStream(
            StreamsBuilder builder,
            ObjectMapper objectMapper,
            @Value("${app.kafka.outcomes-topic}") String outcomesTopic,
            @Value("${app.kafka.page-tasks-topic}") String pageTasksTopic,
            @Value("${app.kafka.outcomes-dlt-topic}") String outcomesDltTopic
    ) {
        var store = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(PROCESSED_EVENTS_STORE),
                Serdes.String(),
                Serdes.Long());
        builder.addStateStore(store);

        KStream<String, OutcomeProcessingResult> processed = builder
                .stream(outcomesTopic, Consumed.with(Serdes.String(), Serdes.String()))
                .process(
                        () -> new OutcomeDeduplicationProcessor(objectMapper),
                        Named.as("deduplicate-event-outcomes"),
                        PROCESSED_EVENTS_STORE);

        processed
                .filter((key, result) -> result.route() == OutcomeProcessingResult.Route.PAGE_TASK)
                .mapValues(OutcomeProcessingResult::payload)
                .to(pageTasksTopic, Produced.with(Serdes.String(), Serdes.String()));

        processed
                .filter((key, result) -> result.route() == OutcomeProcessingResult.Route.DLT)
                .mapValues(OutcomeProcessingResult::payload)
                .to(outcomesDltTopic, Produced.with(Serdes.String(), Serdes.String()));

        return processed;
    }
}
