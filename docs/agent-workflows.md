# AI Agent Workflow Guide

This laboratory is designed to be built in collaboration with an AI Agent. We have established a set of standardized **slash commands** (workflows) that orchestrate the entire lifecycle of a lab—from initial conception to pedagogical certification.

## 🚀 The Fast Track (Master Workflows)

If you want the AI to handle the process end-to-end, you only need two commands:

1. **`/lab-master-plan`**: Use this command to start a new lab. It will identify the next pending lab in the Syllabus, create the directory structure, draft the `spec.md`, refine it, and leave it in a `[READY]` state for coding.
2. **`/lab-master-build`**: Once a lab is `[READY]`, use this command. It will execute the TDD implementation, write the code, verify the infrastructure, and audit the results until the lab is `[DONE]`.

## 🛠️ The Manual Track (Granular Workflows)

If you prefer to maintain tight control over each phase, you can use the granular workflows sequentially:

### Phase 1: Planning
- **`/lab-init`**: Creates the folder structure for a new lab (e.g., `labs/001-architecture-core`) and generates the initial `[DRAFT]` of the `spec.md`, `plan.md`, and `tasks.md`.
- **`/lab-architect`**: Reviews the generated drafts to ensure they are pedagogically sound, unambiguous, and compliant with the Constitution. Once approved, the state changes to `[READY]`.

### Phase 2: Execution
- **`/lab-builder`**: Reads the `[READY]` specifications and executes the technical implementation (TDD, Java classes, Docker Compose, README.md).
- **`/lab-auditor`**: Runs after the builder. It audits the code, tests, and documentation to certify that everything meets the high standards of the Constitution. Once passed, the lab is marked as `[DONE]`.

### Maintenance & Refinement
- **`/lab-governor`**: Analyzes the entire repository to ensure all labs and configurations comply with the global rules defined in `docs/constitution.md`.
- **`/lab-refiner`**: Use this if you want to update or refactor an already `[DONE]` lab. It transitions the lab back to `[DRAFT]` so it can be safely modified through the planning and building phases again.

---

### 💡 How to use them
Simply open the AI Chat window in your IDE and type the slash command (e.g., `/lab-master-plan`). The AI will automatically read the context and guide you through the process.
