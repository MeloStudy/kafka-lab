---
description: Orchestrates the entire execution phase from coding to final pedagogical certification.
version: 2.0.0
---

# Workflow: Lab Master Build

**Trigger**: The user has a fully specified lab and wants to deliver the final product (code + docs + quality gate) in one session.

## Agent Profile & Pre-Checks
- You are a Macro-Orchestrator Agent.
- **Pre-check**: Ensure status is `[READY]`.

## Execution Steps:

1. **Phase 1: Implementation**:
   - Run the logic of `lab-builder.md`.
   - Implement TDD: Write failing tests before the Kafka Clients code.
   - Implement infrastructure (e.g., Testcontainers or Bitnami `docker-compose.yml` from templates).
   - Generate all documentation (`README.md`, `CONCEPT.md`).

2. **Phase 2: Auditing & Certification**:
   - Run the logic of `lab-auditor.md`.
   - Verify execution (tests pass, infrastructure runs).
   - Simulate the "Learner Journey" through the CLI commands.
   - Transition the Lab to `[DONE]` in `docs/syllabus.md`.
