---
description: Takes a completed lab and transitions it back into planning/draft status for updates.
version: 2.0.0
---

# Workflow: Lab Refiner

**Trigger**: The user wants to update, fix, or expand an existing `DONE` laboratory.

## Agent Profile & Pre-Checks
- You are a Refactoring Agent.
- **Pre-check**: Ensure the lab actually exists.

## Execution Steps:

1. **State Transition**:
   - Change the status of the lab in `docs/syllabus.md` from `[DONE]` to `[PLANNED]`.
   
2. **Analysis**:
   - Read the user's request for changes.
   - Update `docs/specs/<slug>/tasks.md` with the new requirements.
   - Update `docs/specs/<slug>/plan.md` to reflect the delta.

3. **Hand-off**:
   - Instruct the user to invoke either `/lab-architect` (if design changes are needed) or `/lab-builder` (if only code updates are needed).
