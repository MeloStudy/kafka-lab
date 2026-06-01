package com.kafkalab.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class StatelessStreamsAppTest {

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, String> inputTopic;
    private TestOutputTopic<String, String> outputTopic;

    @BeforeEach
    void setup() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        testDriver = new TopologyTestDriver(StatelessStreamsApp.buildTopology(), props);
        inputTopic = testDriver.createInputTopic("raw-transactions", Serdes.String().serializer(), Serdes.String().serializer());
        outputTopic = testDriver.createOutputTopic("high-value-transactions", Serdes.String().deserializer(), Serdes.String().deserializer());
    }

    @AfterEach
    void teardown() {
        testDriver.close();
    }

    @Test
    void testFiltersTransactionsLessThan1000() {
        inputTopic.pipeInput("tx1", "{\"amount\": 500.0, \"creditCard\": \"1111-2222-3333-4444\"}");
        inputTopic.pipeInput("tx2", "{\"amount\": 1500.0, \"creditCard\": \"1234-5678-9012-3456\"}");

        assertThat(outputTopic.getQueueSize()).isEqualTo(1);
        String outputValue = outputTopic.readValue();
        assertThat(outputValue).contains("1500.0");
    }

    @Test
    void testMasksCreditCard() {
        inputTopic.pipeInput("tx1", "{\"amount\": 2500.0, \"creditCard\": \"1234-5678-9012-3456\"}");

        String outputValue = outputTopic.readValue();
        assertThat(outputValue)
                .doesNotContain("1234-5678-9012-3456")
                .contains("****-****-****-3456");
    }
}
