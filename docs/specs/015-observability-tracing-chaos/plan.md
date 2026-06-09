# Implementation Plan: LAB-015: Observability, Tracing & Chaos

## Phase 1: Specifications & Design
- [x] Draft `spec.md`
- [x] Review against Constitution

## Phase 2: Documentation (Theory)
- [ ] Write `CONCEPT.md` focusing on observability principles, JMX metrics, OpenTelemetry context propagation, and chaos engineering mechanics.

## Phase 3: Code & Practice
- [ ] Scaffold Maven module `015-observability-tracing-chaos`.
- [ ] Configure `pom.xml` with Spring Boot Actuator, Micrometer, OpenTelemetry, and Toxiproxy Testcontainers.
- [ ] Create `docker-compose.yml` including KRaft Kafka, Prometheus, Zipkin, and Toxiproxy.
- [ ] Implement hands-on code examples: Producer, Consumer, and REST endpoint to trigger events.
- [ ] Write `README.md` guiding the student through metrics, tracing, and chaos experiments.

## Phase 4: Audit
- [ ] Verify Code compiles and tests pass.
- [ ] Verify Self-Assessment questions are included in `README.md`.
