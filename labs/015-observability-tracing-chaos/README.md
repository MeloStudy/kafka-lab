# LAB-015: Observability, Tracing & Chaos

In this lab, you will deploy a fully instrumented Spring Boot application, spin up a comprehensive observability stack, and intentionally break the network to observe system resilience.

## 1. Infrastructure Deployment

First, bring up the Docker Compose stack.

```bash
docker-compose up -d
```

### Infrastructure Dissection

-   `apache/kafka:4.3.0`: The official Apache Kafka image running in KRaft mode (no Zookeeper).
-   `KAFKA_NODE_ID=0`: The unique identifier for this KRaft node.
-   `KAFKA_PROCESS_ROLES=controller,broker`: This single JVM acts as both the metadata controller and the data broker.
-   `KAFKA_LISTENERS`: Defines the physical network interfaces to bind to. We define three networks: `INTERNAL` for broker-to-broker, `EXTERNAL` for client access, and `CONTROLLER` for KRaft quorum communication.
-   `prometheus`: Scrapes the `/actuator/prometheus` endpoint of our Spring Boot application every 5 seconds.
-   `zipkin`: A distributed tracing system that receives OpenTelemetry spans from our application.
-   `toxiproxy`: A TCP proxy that sits between our application and Kafka, allowing us to inject network chaos.

## 2. Running the Application

Run the Spring Boot application using Maven:

```bash
./mvnw spring-boot:run
```

Once running, generate some traffic by sending a POST request to the REST endpoint:

```bash
curl -X POST \
  http://localhost:8080/api/orders
```

### Command Dissection
-   `-X POST`: Specifies the HTTP method as POST.
-   `http://localhost:8080/api/orders`: The endpoint exposed by `OrderController` which triggers the `OrderProducer` to send a Kafka message.

Repeat the command a few times to generate multiple traces.

## 3. Observing Traces and Metrics

### Viewing Traces in Zipkin
Open your browser and navigate to `http://localhost:9411`.
Click "Run Query". You should see traces representing the end-to-end journey of your POST request:
1.  The HTTP Request (Tomcat Span).
2.  The Kafka Producer Span (sending to the `orders` topic).
3.  The Kafka Consumer Span (receiving from the `orders` topic).

Notice how the asynchronous Kafka boundary is bridged seamlessly into a single trace.

### Viewing Metrics in Prometheus
Navigate to `http://localhost:9090`.
In the query bar, execute:
```text
kafka_producer_record_send_total
```
You will see the JMX metrics exposed natively by Spring Boot Actuator and Micrometer.

## 4. Injecting Chaos

Now, let's use the Toxiproxy CLI to simulate a network degradation. We will inject 5 seconds of latency into the downstream connection from Kafka to the Consumer.

```bash
docker exec -it toxiproxy \
  /toxiproxy-cli toxic add \
  -t latency \
  -a latency=5000 \
  kafka
```

### Command Dissection
-   `docker exec -it toxiproxy`: Runs the command inside the running Toxiproxy container.
-   `/toxiproxy-cli toxic add`: Instructs Toxiproxy to add a new "toxic" (failure injection).
-   `-t latency`: Specifies the type of toxic as latency.
-   `-a latency=5000`: Adds an attribute setting the latency to 5000 milliseconds (5 seconds).
-   `kafka`: The name of the proxy endpoint we are targeting.

Send another order via curl. Watch the application logs. You will notice a significant delay before the `OrderConsumer` logs the receipt of the message, proving that the network traffic is successfully being intercepted and throttled.

## <details><summary>Self-Assessment / Knowledge Check</summary>
1. **How does OpenTelemetry propagate trace context across Kafka?**
   *Answer*: The Producer injects the W3C Trace Context (Trace ID and Span ID) into the Kafka Record Headers (specifically `traceparent`). The Consumer extracts these headers before processing the message to link the spans.

2. **Why do we use Toxiproxy instead of just stopping the Kafka container to test resilience?**
   *Answer*: Stopping the container completely severs the TCP connection immediately, which clients detect instantly. Toxiproxy allows us to simulate "gray failures" like extreme latency, packet loss, or bandwidth throttling, which often cause more complex behavior (like `poll()` timeouts and rebalances) than a clean disconnection.
</details>
