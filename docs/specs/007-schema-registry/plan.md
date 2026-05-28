# Implementation Plan: LAB-007: Data Contracts & Schema Registry

## Phase 1: Specifications & Design
- [x] Draft `spec.md`
- [x] Review against Constitution

## Phase 2: Documentation (Theory)
- [ ] Write `CONCEPT.md` focusing on Schema Registry internals, Data Governance, Avro serialization, and Schema Evolution compatibility rules.

## Phase 3: Code & Practice
- [ ] Scaffold Maven module `labs/007-schema-registry`.
- [ ] Configure `pom.xml` with `avro-maven-plugin`, `kafka-avro-serializer`, and Testcontainers dependencies.
- [ ] Define initial Avro schema (`order-v1.avsc`).
- [ ] Implement Java Producer and Consumer using Avro serialization.
- [ ] Define evolved Avro schemas (`order-v2.avsc` with default, `order-v3.avsc` without default) for evolution scenarios.
- [ ] Write `docker-compose.yml` based on `docs/templates/infra/docker-compose-schema.yml`.
- [ ] Write `README.md` guiding the student through CLI interactions, REST API exploration, Java implementation, and the schema evolution process.

## Phase 4: Audit
- [ ] Verify Code compiles and tests pass (including Schema Registry Testcontainers integration).
- [ ] Verify Self-Assessment questions are included in `README.md`.
