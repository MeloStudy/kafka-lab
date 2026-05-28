# Implementation Plan: P01-Enterprise-Workflow

## Phase 1: Specifications & Design
- [x] Draft `spec.md`
- [x] Review against Constitution
- [x] Ensure `P01` namespace is used and directory is `projects/P01-enterprise-workflow`.

## Phase 2: Documentation (Theory)
- [ ] Write `CONCEPT.md` focusing on reactive streams, bridging REST to Kafka, and eventual consistency.

## Phase 3: Code & Practice
- [ ] Scaffold independent Maven project (no parent POM from `kafka-lab`) in `projects/P01-enterprise-workflow`.
- [ ] Implement `docker-compose.yml` for KRaft.
- [ ] Write Integration Tests using Testcontainers (TDD).
- [ ] Implement `OrderController` (REST API).
- [ ] Implement `OrderProducer` and `OrderConsumer`.
- [ ] Write `README.md` guiding the student through the workflow.

## Phase 4: Audit
- [ ] Verify Code compiles and tests pass.
- [ ] Verify Self-Assessment questions are included.
