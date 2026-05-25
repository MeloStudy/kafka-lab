# Implementation Plan: LAB-001: Architecture Core & Infrastructure

## Phase 1: Specifications & Design
- [x] Draft `spec.md` focusing on Infrastructure and CLI.
- [x] Review against Constitution (Apply TDD exemption for infra lab).

## Phase 2: Documentation (Theory)
- [ ] Write `CONCEPT.md` focusing on the physical topology of Kafka.
  - Detail Brokers, Partitions, Replicas, and ISR.
  - Explain the KRaft architecture.
  - (REFINEMENT) Add deep explanation of Partitions (physical files on disk, resources used).
  - (REFINEMENT) Add deep explanation of Consumer Groups (how they balance work, partition mapping, multiple groups).

## Phase 3: Code & Practice
- [ ] Scaffold `labs/001-architecture-core/` directory.
- [ ] Copy `docker-compose-basic.yml` to the lab folder as `docker-compose.yml`.
- [ ] Write `README.md` guiding the student through the CLI commands.
  - Detail `kafka-topics.sh` (Create, List, Describe).
  - Detail `kafka-console-producer.sh` and `kafka-console-consumer.sh`.

## Phase 4: Audit
- [ ] Verify Docker Compose configuration runs properly.
- [ ] Verify Self-Assessment questions are included and aligned.
