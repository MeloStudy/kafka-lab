---
description: Orchestrates the entire planning phase from Syllabus identification to a READY state.
version: 1.0.0
---

# Workflow: Lab Master Plan

This is a macro-workflow that combines **Init** and **Architect** phases. Use this when you want to fully design a lab's specification and planning artifacts in one session.

## Steps:

1. **Phase 1: Initiation**:
   - Run the logic of `lab-init.md`.
   - Create folders, copy templates from `docs/templates/`, and update `docs/syllabus.md` to mark the lab as `[DRAFT]`.

2. **Phase 2: Architectural Design**:
   - Run the logic of `lab-architect.md`.
   - Fill out the `spec.md` with deep Kafka pedagogy (e.g., Brokers, Topics, Consumer Groups).
   - Define the explicit resilience/backpressure scenarios to test.
   - If user approves, transition the lab to `[READY]` in `docs/syllabus.md`.
