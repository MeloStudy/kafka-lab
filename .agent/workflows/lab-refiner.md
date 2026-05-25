---
description: Performs a comprehensive retrospective audit and end-to-end refinement of a delivered laboratory to ensure compliance with the latest standards.
version: 1.0.0
---

# Workflow: Lab Refiner

This workflow is triggered when the user wants to audit, upgrade, or fix an already delivered laboratory (usually in `[DONE]`, `[READY]`, or `[PLANNED]` status) to ensure it meets the latest Constitution standards or conversational insights.

## Agent Steps:

1. **Retrospective Audit**:
   - Change the lab status in `docs/syllabus.md` to `[PLANNED]` temporarily.
   - Read the laboratory's current artifacts (`spec.md`, `plan.md`, `tasks.md`, `CONCEPT.md`, `README.md`, and any code files).
   - Compare them strictly against the latest conversational insights or `docs/constitution.md`.
   - Identify gaps: e.g., superficial conceptual explanations, missing TDD coverage, lack of cleanup steps.

2. **Refinement Planning (Architect Phase)**:
   - Propose the necessary upgrades to the user (if ambiguous) or proceed automatically if the user provided explicit directions.
   - Modify `docs/specs/<slug>/spec.md` to include new Learning Objectives if necessary.
   - Update `docs/specs/<slug>/plan.md` to outline the refinement strategy.
   - Generate a new, atomic checklist in `docs/specs/<slug>/tasks.md` covering all the identified gaps.

3. **Execution (Builder Phase)**:
   - Follow the newly generated `tasks.md` to execute the retrofitting.
   - Rewrite `CONCEPT.md` to ensure deep engineering rigor.
   - Modify or add tests (TDD) to cover new requirements.
   - Update `README.md` to fix missing Command Dissections or native execution instructions.
   - Verify code compiles and passes tests (if applicable).

4. **Final Certification (Auditor Phase)**:
   - Perform a final conceptual gap audit on the refined deliverables.
   - Check off all items in `tasks.md`.
   - Ensure the lab status is set back to `[DONE]` in `docs/syllabus.md`.
   - Provide the user with a summary walkthrough of the improvements made to the laboratory in the `walkthrough.md` artifact.
