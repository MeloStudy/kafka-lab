# Implementation Plan: LAB-014: Reactive Kafka (WebFlux Integration)

## Phase 1: Specifications & Design
- [x] Draft `spec.md`
- [x] Review against Constitution

## Phase 2: Documentation (Theory)
- [ ] Write `CONCEPT.md` focusing on internal mechanics of `reactor-kafka` and backpressure.

## Phase 3: Code & Practice
- [ ] Scaffold Maven module `014-reactive-kafka`.
- [ ] Implement hands-on code examples:
  - Add Spring WebFlux and `reactor-kafka` dependencies.
  - Implement WebFlux REST Controller.
  - Implement Reactive Producer (`KafkaSender`).
  - Implement Reactive Consumer (`KafkaReceiver`).
- [ ] Write `README.md` guiding the student through the concepts, execution, and backpressure observation.

## Phase 4: Audit
- [ ] Verify Code compiles and tests pass using Testcontainers.
- [ ] Verify Self-Assessment questions are included.
