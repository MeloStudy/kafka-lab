package com.kafkalab.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class StatefulStreamsAppTest {

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, String> inputTopic;
    private TestOutputTopic<String, String> totalOutputTopic;
    private TestOutputTopic<String, String> windowedOutputTopic;

    @BeforeEach
    void setup() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        testDriver = new TopologyTestDriver(StatefulStreamsApp.buildTopology(), props);
        inputTopic = testDriver.createInputTopic("user-clicks", Serdes.String().serializer(), Serdes.String().serializer());
        totalOutputTopic = testDriver.createOutputTopic("user-clicks-total", Serdes.String().deserializer(), Serdes.String().deserializer());
        windowedOutputTopic = testDriver.createOutputTopic("user-clicks-windowed", Serdes.String().deserializer(), Serdes.String().deserializer());
    }

    @AfterEach
    void teardown() {
        testDriver.close();
    }

    @Test
    void testGlobalClicksAggregation() {
        inputTopic.pipeInput("alice", "click");
        inputTopic.pipeInput("alice", "click");
        inputTopic.pipeInput("bob", "click");

        assertThat(totalOutputTopic.readKeyValuesToList())
                .extracting(kv -> kv.value)
                .containsExactly("1", "2", "1");
    }

    @Test
    void testWindowedClicksAggregation() {
        Instant start = Instant.parse("2024-01-01T10:00:00Z");

        // Window 1: 10:00:00 - 10:01:00
        inputTopic.pipeInput("alice", "click", start);
        inputTopic.pipeInput("alice", "click", start.plusSeconds(30));

        // Window 2: 10:01:00 - 10:02:00
        inputTopic.pipeInput("alice", "click", start.plusSeconds(65));

        assertThat(windowedOutputTopic.readKeyValuesToList())
                .hasSize(3)
                .satisfies(list -> {
                    assertThat(list.get(0).value).isEqualTo("1"); // alice window 1
                    assertThat(list.get(1).value).isEqualTo("2"); // alice window 1
                    assertThat(list.get(2).value).isEqualTo("1"); // alice window 2
                });
    }
}
