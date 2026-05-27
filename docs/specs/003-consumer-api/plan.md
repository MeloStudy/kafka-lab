# Implementation Plan: LAB-003: The Consumer API

## Phase 1: Specifications & Design
- [x] Draft `spec.md`
- [x] Review against Constitution

## Phase 2: Documentation (Theory)
- [ ] Write `CONCEPT.md` focusing on the internal mechanics of the `poll()` loop, Heartbeats, and the Rebalance Protocol.

## Phase 3: Code & Practice
- [ ] Scaffold Maven module (`labs/003-consumer-api`).
- [ ] Implement hands-on code examples:
  - `DummyProducer.java` (to generate load).
  - `VanillaConsumer.java` (to process messages).
- [ ] Implement simulation for slow consumer (exceeding `max.poll.interval.ms`).
- [ ] Write `README.md` guiding the student through scaling and rebalances.

## Phase 4: Audit
- [ ] Verify Code compiles and runs correctly against local cluster.
- [ ] Verify Self-Assessment questions are included.
