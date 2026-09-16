package com.sportygroup.settlement;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sportygroup.settlement.bet.model.BetEntity;
import com.sportygroup.settlement.bet.model.BetStatus;
import com.sportygroup.settlement.bet.repository.BetRepository;
import com.sportygroup.settlement.outcome.api.EventOutcomeRequest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.web.servlet.MockMvc;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest(properties = {
        "app.kafka.listener-enabled=true",
        "app.expansion.page-size=2",
        "app.expansion.concurrency=2",
        "app.delivery.concurrency=2",
        "spring.kafka.producer.transaction-id-prefix=test-${random.uuid}-",
        "spring.kafka.streams.application-id=test-${random.uuid}"
})
@AutoConfigureMockMvc
@EmbeddedKafka(
        partitions = 3,
        brokerProperties = {
                "transaction.state.log.replication.factor=1",
                "transaction.state.log.min.isr=1"
        },
        topics = {
                "event-outcomes", "settlement-page-tasks", "bet-settlement-commands",
                "event-outcomes.DLT", "settlement-page-tasks.DLT", "bet-settlement-commands.DLT"
        },
        bootstrapServersProperty = "spring.kafka.bootstrap-servers")
class FullFlowIntegrationTest {

    private static final int PAGE_SIZE = 2;

    @Autowired
    MockMvc mockMvc;

    @Autowired
    ObjectMapper objectMapper;

    @Autowired
    BetRepository betRepository;

    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    EmbeddedKafkaBroker embeddedKafkaBroker;

    @ParameterizedTest(name = "{0}")
    @MethodSource("settlementScenarios")
    void settlesBetsThroughTheCompleteFlow(Scenario scenario) throws Exception {
        betRepository.saveAllAndFlush(scenario.bets());

        mockMvc.perform(post("/api/v1/event-outcomes")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsBytes(scenario.outcome())))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.eventId").value(scenario.eventId()))
                .andExpect(jsonPath("$.status").value("ACCEPTED"));

        await().atMost(Duration.ofSeconds(15)).pollInterval(Duration.ofMillis(50))
                .untilAsserted(() -> assertScenarioCompleted(scenario));
    }

    @Test
    void duplicatePostIsAcceptedAndDoesNotChangeTheResult() throws Exception {
        String eventId = uniqueId("duplicate-event");
        String betId = uniqueId("duplicate-bet");
        EventOutcomeRequest outcome = new EventOutcomeRequest(eventId, "A vs B", "team-a");
        betRepository.saveAndFlush(bet(betId, eventId, "team-a"));

        postOutcome(outcome, 202, "ACCEPTED");
        await().atMost(Duration.ofSeconds(15)).pollInterval(Duration.ofMillis(50))
                .untilAsserted(() -> assertThat(betRepository.findById(betId))
                        .get()
                        .extracting(BetEntity::getStatus)
                        .isEqualTo(BetStatus.WON));

        postOutcome(outcome, 202, "ACCEPTED");
        assertThat(betRepository.findById(betId))
                .get()
                .extracting(BetEntity::getStatus)
                .isEqualTo(BetStatus.WON);
    }

    @Test
    void malformedOutcomeIsMovedToDlt() throws Exception {
        String key = uniqueId("malformed-outcome");
        String payload = "not-json";
        String dltTopic = "event-outcomes.DLT";
        var consumerProps = KafkaTestUtils.consumerProps(
                uniqueId("dlt-verification"), "false", embeddedKafkaBroker);

        try (var consumer = new DefaultKafkaConsumerFactory<>(
                consumerProps, new StringDeserializer(), new StringDeserializer())
                .createConsumer()) {
            embeddedKafkaBroker.consumeFromAnEmbeddedTopic(consumer, dltTopic);

            kafkaTemplate.send("event-outcomes", key, payload).get(10, TimeUnit.SECONDS);

            var recovered = KafkaTestUtils.getSingleRecord(
                    consumer, dltTopic, Duration.ofSeconds(10));
            assertThat(recovered.key()).isEqualTo(key);
            assertThat(recovered.value()).isEqualTo(payload);
        }
    }

    private void postOutcome(EventOutcomeRequest outcome, int expectedStatus, String expectedResult)
            throws Exception {
        mockMvc.perform(post("/api/v1/event-outcomes")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsBytes(outcome)))
                .andExpect(status().is(expectedStatus))
                .andExpect(jsonPath("$.status").value(expectedResult));
    }

    private void assertScenarioCompleted(Scenario scenario) {
        assertThat(loadStatuses(scenario.expectedStatuses().keySet()))
                .containsExactlyInAnyOrderEntriesOf(scenario.expectedStatuses());
    }

    private Map<String, BetStatus> loadStatuses(Iterable<String> betIds) {
        Map<String, BetStatus> statuses = new LinkedHashMap<>();
        betRepository.findAllById(betIds)
                .forEach(bet -> statuses.put(bet.getBetId(), bet.getStatus()));
        return statuses;
    }

    static Stream<Arguments> settlementScenarios() {
        return Stream.of(
                Scenario.withWinners("single winner", 1, index -> true),
                Scenario.withWinners("single loser", 1, index -> false),
                Scenario.withWinners("exact page", PAGE_SIZE, index -> index % 2 == 0),
                Scenario.withWinners("page plus one", PAGE_SIZE + 1, index -> index % 2 == 0)
        ).map(Arguments::of);
    }

    private static BetEntity bet(String betId, String eventId, String predictedWinner) {
        return new BetEntity(
                betId,
                "user-" + betId,
                eventId,
                "winner",
                predictedWinner,
                new BigDecimal("10.00"));
    }

    private static String uniqueId(String prefix) {
        return prefix + "-" + UUID.randomUUID();
    }

    private record Scenario(
            String name,
            EventOutcomeRequest outcome,
            List<BetEntity> bets,
            Map<String, BetStatus> expectedStatuses
    ) {
        static Scenario withWinners(String name, int count, java.util.function.IntPredicate wins) {
            String eventId = uniqueId(name.replace(' ', '-'));
            Map<String, BetStatus> expected = new LinkedHashMap<>();
            List<BetEntity> bets = IntStream.range(0, count)
                    .mapToObj(index -> {
                        String betId = uniqueId("bet-" + index);
                        boolean won = wins.test(index);
                        expected.put(betId, won ? BetStatus.WON : BetStatus.LOST);
                        return bet(betId, eventId, won ? "team-a" : "team-b");
                    })
                    .toList();
            return new Scenario(
                    name,
                    new EventOutcomeRequest(eventId, "A vs B", "team-a"),
                    bets,
                    Map.copyOf(expected));
        }

        String eventId() {
            return outcome.eventId();
        }

        @Override
        public String toString() {
            return name;
        }
    }
}
