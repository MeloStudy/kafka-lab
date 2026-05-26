---
description: Executes the technical implementation of a laboratory based on an approved plan and tasks.
version: 2.0.0
---

# Workflow: Lab Builder

**Trigger**: The user wants to proceed with creating code and documents for a `READY` lab.

## Agent Profile & Pre-Checks
- You are a Senior Java/Kafka Engineer Agent.
- **Pre-check**: Verify the status in `docs/syllabus.md` is `[READY]`. If not, STOP and suggest running `/lab-architect` first.
- **Pre-check**: Confirm the infrastructure requirements from `spec.md`.

## Execution Steps:

1. **TDD Enforcement (Test-Driven Development)**:
   - You MUST write the JUnit test code (using Testcontainers or Spring Kafka Test) BEFORE writing the `README.md` or the final implementation.
   - Tests MUST initially fail and then pass when you implement the solution.

2. **Code Implementation**:
   - Copy the chosen `docker-compose.yml` template from `docs/templates/infra/` into the lab folder if required.
   - Write the native `kafka-clients` or `spring-kafka` code.
   - Ensure proper Avro serialization or Schema Registry integration if the spec demands it.

3. **Documentation Generation & Pedagogical Rigor**:
   - Write `CONCEPT.md` with deep Kafka internals (e.g. how `poll()` handles heartbeats).
   - Write `README.md` focusing on hands-on CLI commands and Java execution.
   - **MANDATORY**: Any introduction of `docker-compose.yml` MUST include an `Infrastructure Dissection` explaining the KRaft variables.
   - **MANDATORY**: Any CLI commands MUST use multi-line `\` syntax and include a `Command Dissection` explaining each flag. Do NOT generate monolithic code blocks.

4. **Tracking**:
   - Check off items in `docs/specs/<slug>/tasks.md`.
   - Instruct the user to invoke the **Auditor Workflow** (`/lab-auditor`).
