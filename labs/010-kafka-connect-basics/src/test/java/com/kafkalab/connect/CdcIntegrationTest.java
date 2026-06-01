package com.kafkalab.connect;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CdcIntegrationTest {
    static Network network = Network.newNetwork();

    @Container
    static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("apache/kafka:4.3.0"))
            .withNetwork(network)
            .withNetworkAliases("kafka")
            .withListener("kafka:19092");

    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:15")
            .withNetwork(network)
            .withNetworkAliases("postgres")
            .withDatabaseName("mydb")
            .withUsername("postgres")
            .withPassword("postgres")
            .withCommand("postgres -c wal_level=logical");

    static GenericContainer<?> connect = new GenericContainer<>(DockerImageName.parse("debezium/connect:2.5"))
            .withNetwork(network)
            .withExposedPorts(8083)
            .withEnv("BOOTSTRAP_SERVERS", "kafka:19092")
            .withEnv("GROUP_ID", "1")
            .withEnv("CONFIG_STORAGE_TOPIC", "connect_configs")
            .withEnv("OFFSET_STORAGE_TOPIC", "connect_offsets")
            .withEnv("STATUS_STORAGE_TOPIC", "connect_statuses")
            .withEnv("KEY_CONVERTER", "org.apache.kafka.connect.json.JsonConverter")
            .withEnv("VALUE_CONVERTER", "org.apache.kafka.connect.json.JsonConverter")
            .withEnv("KEY_CONVERTER_SCHEMAS_ENABLE", "false")
            .withEnv("VALUE_CONVERTER_SCHEMAS_ENABLE", "false")
            .dependsOn(kafka, postgres)
            .waitingFor(Wait.forHttp("/connectors").forStatusCode(200));

    @BeforeAll
    static void setup() throws Exception {
        kafka.start();
        postgres.start();
        connect.start();

        // Initialize DB
        try (Connection conn = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE users (id SERIAL PRIMARY KEY, name VARCHAR(50));");
            stmt.execute("ALTER TABLE users REPLICA IDENTITY FULL;");
            stmt.execute("INSERT INTO users (name) VALUES ('Alice');");
        }

        // Register Connector
        String config = "{"
                + "\"name\": \"pg-connector\","
                + "\"config\": {"
                + "  \"connector.class\": \"io.debezium.connector.postgresql.PostgresConnector\","
                + "  \"database.hostname\": \"postgres\","
                + "  \"database.port\": \"5432\","
                + "  \"database.user\": \"postgres\","
                + "  \"database.password\": \"postgres\","
                + "  \"database.dbname\": \"mydb\","
                + "  \"topic.prefix\": \"pg\","
                + "  \"plugin.name\": \"pgoutput\""
                + "}"
                + "}";

        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://" + connect.getHost() + ":" + connect.getMappedPort(8083) + "/connectors"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(config))
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertTrue(response.statusCode() == 201 || response.statusCode() == 409, "Failed to create connector: " + response.body());

        // Wait a bit for snapshot to complete
        Thread.sleep(5000);
    }

    @AfterAll
    static void teardown() {
        connect.stop();
        postgres.stop();
        kafka.stop();
        network.close();
    }

    @Test
    void testCdcEventEmittedOnInsert() throws Exception {
        // Insert new record
        try (Connection conn = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
             Statement stmt = conn.createStatement()) {
            stmt.execute("INSERT INTO users (name) VALUES ('Bob');");
        }

        // Consume from Kafka
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList("pg.public.users"));

            await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                boolean foundBob = false;
                for (ConsumerRecord<String, String> recordEntity : records) {
                    if (recordEntity.value() != null && recordEntity.value().contains("Bob")) {
                        foundBob = true;
                        break;
                    }
                }
                assertTrue(foundBob, "Expected CDC event for 'Bob' not found yet");
            });
        }
    }
}
