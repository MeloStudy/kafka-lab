package com.kafkalab.streams;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

@Slf4j
public class StatelessStreamsApp {

    public static Topology buildTopology() {
        StreamsBuilder builder = new StreamsBuilder();
        ObjectMapper mapper = new ObjectMapper();

        KStream<String, String> sourceStream = builder.stream("raw-transactions");

        sourceStream
                .filter((key, value) -> {
                    try {
                        JsonNode node = mapper.readTree(value);
                        return node.has("amount") && node.get("amount").asDouble() > 1000.0;
                    } catch (Exception e) {
                        return false;
                    }
                })
                .mapValues(value -> {
                    try {
                        ObjectNode node = (ObjectNode) mapper.readTree(value);
                        if (node.has("creditCard")) {
                            String cc = node.get("creditCard").asText();
                            if (cc.length() >= 4) {
                                node.put("creditCard", "****-****-****-" + cc.substring(cc.length() - 4));
                            }
                        }
                        return mapper.writeValueAsString(node);
                    } catch (Exception e) {
                        return value;
                    }
                })
                .to("high-value-transactions");

        return builder.build();
    }

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "stateless-streams-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        Topology topology = buildTopology();
        log.info("Topology Description:\n{}", topology.describe());

        KafkaStreams streams = new KafkaStreams(topology, props);

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        log.info("Starting Kafka Streams application...");
        streams.start();
    }
}
