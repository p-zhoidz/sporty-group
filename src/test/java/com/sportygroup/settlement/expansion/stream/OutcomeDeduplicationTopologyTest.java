package com.sportygroup.settlement.expansion.stream;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class OutcomeDeduplicationTopologyTest {

    private static final String OUTCOMES = "event-outcomes";
    private static final String PAGE_TASKS = "settlement-page-tasks";
    private static final String DLT = "event-outcomes.DLT";

    private final ObjectMapper objectMapper = new ObjectMapper().findAndRegisterModules();

    @Test
    void emitsOnlyOneInitialTaskForDuplicateEventId() throws Exception {
        try (TopologyTestDriver driver = driver()) {
            var input = driver.createInputTopic(
                    OUTCOMES, new StringSerializer(), new StringSerializer());
            var output = driver.createOutputTopic(
                    PAGE_TASKS, new StringDeserializer(), new StringDeserializer());
            String payload = objectMapper.writeValueAsString(
                    new com.sportygroup.settlement.outcome.model.EventOutcome(
                            "event-1", "A vs B", "team-a"));

            input.pipeInput("event-1", payload);
            input.pipeInput("event-1", payload);

            assertThat(output.readKeyValuesToList()).hasSize(1);
        }
    }

    @Test
    void routesMalformedOutcomeToDlt() {
        try (TopologyTestDriver driver = driver()) {
            var input = driver.createInputTopic(
                    OUTCOMES, new StringSerializer(), new StringSerializer());
            var dlt = driver.createOutputTopic(
                    DLT, new StringDeserializer(), new StringDeserializer());

            input.pipeInput("event-1", "not-json");

            var recovered = dlt.readKeyValue();
            assertThat(recovered.key).isEqualTo("event-1");
            assertThat(recovered.value).isEqualTo("not-json");
        }
    }

    private TopologyTestDriver driver() {
        var builder = new StreamsBuilder();
        new OutcomeDeduplicationTopology().outcomeDeduplicationStream(
                builder, objectMapper, OUTCOMES, PAGE_TASKS, DLT);
        var properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "outcome-deduplication-test");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        return new TopologyTestDriver(builder.build(), properties);
    }
}
