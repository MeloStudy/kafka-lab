package com.kafkalab.streams;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;

import java.time.Duration;
import java.util.Properties;

@Slf4j
public class StatefulStreamsApp {

    public static Topology buildTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> clicksStream = builder.stream("user-clicks", Consumed.with(Serdes.String(), Serdes.String()));

        // 1. Global Count (KTable)
        clicksStream
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .count(Materialized.as("global-clicks-store"))
                .toStream()
                .mapValues(Object::toString)
                .to("user-clicks-total", Produced.with(Serdes.String(), Serdes.String()));

        // 2. Windowed Count (Tumbling Window of 1 minute)
        clicksStream
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)))
                .count(Materialized.as("windowed-clicks-store"))
                .toStream()
                .map((windowedKey, count) -> new org.apache.kafka.streams.KeyValue<>(
                        windowedKey.key() + "@" + windowedKey.window().start(),
                        count.toString()))
                .to("user-clicks-windowed", Produced.with(Serdes.String(), Serdes.String()));

        return builder.build();
    }

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "stateful-streams-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        // For Exactly-Once Semantics (EOS)
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);

        Topology topology = buildTopology();
        log.info("Topology Description:\n{}", topology.describe());

        KafkaStreams streams = new KafkaStreams(topology, props);
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        log.info("Starting Kafka Streams Stateful application...");
        streams.start();
    }
}
