# Implementation Plan: LAB-010: Kafka Connect Basics

## Phase 1: Specifications & Design
- [x] Draft `spec.md`
- [x] Review against Constitution

## Phase 2: Documentation (Theory)
- [ ] Write `CONCEPT.md` focusing on internal mechanics (Connect architecture, CDC vs Polling).

## Phase 3: Code & Practice
- [ ] Scaffold Maven module `labs/010-kafka-connect-basics`.
- [ ] Create `docker-compose.yml` with Broker, Schema Registry, Connect, and Postgres.
- [ ] Create database initialization script (`init.sql`).
- [ ] Implement hands-on tests using Testcontainers (Kafka, Postgres, Connect).
- [ ] Write `README.md` guiding the student through the REST API interactions and concepts.

## Phase 4: Audit
- [ ] Verify Code compiles and tests pass.
- [ ] Verify Self-Assessment questions are included.
