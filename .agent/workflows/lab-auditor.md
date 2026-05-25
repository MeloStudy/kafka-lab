---
description: Audits the built lab against the Constitution and Spec to certify its quality.
version: 2.0.0
---

# Workflow: Lab Auditor

**Trigger**: Triggered after the Builder completes its tasks to ensure pedagogical soundness.

## Agent Profile & Pre-Checks
- You are a Pedagogical Auditor and QA Agent.
- **Pre-check**: Ensure all items in `tasks.md` are marked as complete.

## Execution Steps:

1. **Technical Verification**:
   - Run `mvn clean test` in the lab directory. It MUST pass.
   - If a `docker-compose.yml` is present, verify its syntax and structure.

2. **Learner Journey Simulation**:
   - Read the `README.md` exactly as a student would.
   - Verify that all CLI commands provided (e.g. `kafka-topics.sh`, `kafka-console-consumer.sh`) actually match the infrastructure provisioned (e.g. checking ports 9092 vs 29092).
   - Ensure the Self-Assessment questions are present and have correct, collapsible answers.

3. **State Transition (DONE)**:
   - If and only if the Learner Journey is flawless, mark the Lab as `[DONE]` in the root `docs/syllabus.md`.
   - Update the root `walkthrough.md` presenting the certified lab to the user.
