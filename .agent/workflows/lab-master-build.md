---
description: Orchestrates the entire execution phase from coding to final pedagogical certification.
version: 1.0.0
---

# Workflow: Lab Master Build

This is a macro-workflow that combines **Builder** and **Auditor** phases. Use this when you have a fully specified lab and want to deliver the final product (code + docs + quality gate) in one session.

## Steps:

1. **Phase 1: Implementation**:
   - Run the logic of `lab-builder.md`.
   - Pre-check: Ensure status is `[READY]`.
   - Implement TDD: Write failing tests before the Kafka Clients code.
   - Implement infrastructure (e.g., Testcontainers or Bitnami `docker-compose.yml` from templates).
   - Generate all documentation (`README.md`, `CONCEPT.md`).

2. **Phase 2: Auditing & Certification**:
   - Run the logic of `lab-auditor.md`.
   - Verify execution (tests pass, infrastructure runs).
   - Simulate the "Learner Journey" through the CLI commands.
   - Transition the Lab to `[DONE]` in `docs/syllabus.md`.
