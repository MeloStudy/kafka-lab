---
description: Performs a comprehensive retrospective audit and end-to-end refinement of a delivered laboratory to ensure compliance with the latest standards.
version: 2.0.0
---

# Workflow: Lab Refiner

**Trigger**: Triggered when the user wants to audit, upgrade, or fix an already delivered laboratory (usually in `[DONE]`, `[READY]`, or `[PLANNED]` status) to ensure it meets the latest Constitution standards or conversational insights.

## Agent Profile & Pre-Checks
- You are a Senior Pedagogical Refiner Agent.
- **Pre-check**: Change the lab status in `docs/syllabus.md` to `[PLANNED]` temporarily before doing anything else.

## Execution Steps:

1. **Retrospective Audit**:
   - Read the laboratory's current artifacts (`spec.md`, `plan.md`, `tasks.md`, `CONCEPT.md`, `README.md`, and any code files).
   - Compare them strictly against the latest conversational insights or `docs/constitution.md`.
   - Identify gaps: e.g., superficial conceptual explanations, missing TDD coverage, lack of cleanup steps, or monolithic command blocks lacking dissection.

2. **Refinement Planning (Architect Phase)**:
   - Create an `implementation_plan.md` to propose the necessary upgrades to the user.
   - Wait for the user's approval.
   - Once approved, modify `docs/specs/<slug>/spec.md` to include new Learning Objectives if necessary.
   - Generate a new, atomic checklist in `task.md`.

3. **Execution (Builder Phase)**:
   - Follow the `task.md` to execute the retrofitting.
   - Rewrite `CONCEPT.md` to ensure deep engineering rigor.
   - Modify or add tests (TDD) to cover new requirements.
   - Update `README.md` to fix missing Command Dissections or native execution instructions.

4. **Final Certification (Auditor Phase)**:
   - Perform a final conceptual gap audit on the refined deliverables.
   - Ensure the lab status is set back to `[DONE]` in `docs/syllabus.md`.
   - Provide the user with a summary walkthrough of the improvements made in the `walkthrough.md` artifact.
