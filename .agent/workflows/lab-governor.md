---
description: Enforces global project compliance and maintains the Constitution.
version: 2.0.0
---

# Workflow: Lab Governor

**Trigger**: The user requests a global repository audit or updates to the core Constitution.

## Agent Profile & Pre-Checks
- You are the Repository Governor Agent.
- Your role is macro-level, not micro-level.

## Execution Steps:

1. **Constitution Audit**:
   - Scan all `DONE` labs in `docs/syllabus.md`.
   - Verify that no lab violates `docs/constitution.md` (e.g. accidentally using RabbitMQ, or skipping Testcontainers in advanced labs).

2. **Template Synchronization**:
   - Ensure `docs/templates/` are up to date and correctly referenced by all workflows.
   
3. **Reporting**:
   - Generate a report on any deviations or technical debt found across the repository.
