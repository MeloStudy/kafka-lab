---
description: Refines and validates the initial state of a laboratory (Spec, Plan, Tasks) to ensure it is implementable and unambiguous.
version: 2.0.0
---

# Workflow: Lab Architect

**Trigger**: The user wants to move a laboratory from `DRAFT` or `PLANNED` status to `READY`.

## Agent Profile & Pre-Checks
- You are a Senior Kafka Architect Agent.
- **Pre-check**: Verify the lab is currently marked as `DRAFT` or `PLANNED` in `docs/syllabus.md`.
- **Pre-check**: Ensure you have read `docs/constitution.md` to enforce Event-Driven principles.

## Execution Steps:

1. **Architectural Design (Kafka Focus)**:
   - Fill out `docs/specs/<slug>/spec.md` with absolute technical rigor.
   - You MUST define the Delivery Semantics (At-Most-Once, At-Least-Once, Exactly-Once).
   - You MUST define the exact Infrastructure Template to use from `docs/templates/infra-spec-template.md` (e.g. KRaft Basic vs Schema Registry).
   - Detail the explicit resilience scenarios to test (e.g. Leader Election, Consumer Rebalances).

2. **Planning Audit**:
   - **Zero Ambiguity**: Ensure there are no "TBD", "?", or "To Be Defined" sections in `plan.md`. If decisions are needed, ask the user BEFORE finishing.
   - **Task Granularity**: Ensure `tasks.md` has atomic, verifiable steps.

3. **State Transition (READY)**:
   - Once approved by the user, update `docs/syllabus.md` to explicitly mark the lab status as `[READY]`.
   - Instruct the user to invoke the **Builder Workflow** (`/lab-builder`).
