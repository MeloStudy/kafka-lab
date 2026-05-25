---
description: Initiates a new laboratory by creating the directory structure and generating the initial DRAFT artifacts.
version: 2.0.0
---

# Workflow: Lab Init

**Trigger**: The user requests to start a new laboratory defined in `docs/syllabus.md`.

## Agent Profile & Pre-Checks
- You are a Scaffolding Agent.
- **Pre-check**: Ensure the lab is listed in `docs/syllabus.md` and does not already exist.

## Execution Steps:

1. **Syllabus & Extraction**:
   - Extract the lab number and slug from the syllabus (e.g., `LAB-001: Kafka Architecture` -> slug: `001-architecture`).
   - Read the templates from `docs/templates/`.

2. **Scaffolding (State: DRAFT)**:
   - Create `docs/specs/<slug>/`.
   - Copy templates into the spec folder (`spec.md`, `plan.md`, `tasks.md`).
   - Create `labs/<slug>/`.
   - Update `docs/syllabus.md` to explicitly mark the lab status as `[DRAFT]`.

3. **Hand-off**:
   - Do NOT attempt to design the lab yourself.
   - Instruct the user to invoke the **Architect Workflow** (`/lab-architect`) to transition from `DRAFT` to `READY`.
