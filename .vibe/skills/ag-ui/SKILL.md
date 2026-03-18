---
name: ag-ui
description: How to wire agents and frontends together with the Agent User Interaction protocol in this repo.
---

# Skill: Building with AG-UI

## When to reach for AG-UI
Use AG-UI whenever you need the Python agent and React frontend to share chat history, tool definitions, and synchronized state (for example robot path plans). It keeps the transport, schema enforcement, and streaming events consistent across runtimes.

## Core building blocks
- **Messages** (`developer`, `system`, `assistant`, `user`, `tool`, `activity`) describe the chat timeline. See [python-ag-ui.md](./python-ag-ui.md) and [typescript-ag-ui.md](./typescript-ag-ui.md) for mirrored type definitions.
- **Tools** expose callable functions with JSON Schema parameters so the LLM can invoke backend capabilities.
- **State** is an arbitrary Pydantic/TypeScript structure that both sides understand. It is synchronized via `StateSnapshotEvent` metadata.
- **Events** stream over SSE so the frontend reacts instantly to state changes or progress updates.

## Implementation cookbook
1. **Model the shared state.** Define/update `RobotPathPlan` and nested models in `server/cuopt_tool.py`. Mirror the shape in `frontend_web/src/types/robot-path.ts` so the UI can render it. See [how-we-use-ag-ui.md](./how-we-use-ag-ui.md#our-state-model-robotpathplan).
2. **Write tools that mutate state.** In tools such as `plan_robot_path`, build the new state, assign `ctx.deps = state_obj`, and return a `ToolReturn` that includes a `StateSnapshotEvent` in `metadata`. This both satisfies the agent's tool result and streams state to the client.
3. **Run the agent via AG-UI.** The backend receives `RunAgentInput` (thread/run IDs, messages, tools, state) and hands it to the orchestrator. Always forward the latest state so subsequent tool calls have context.
4. **Consume events on the frontend.** The React app's AG-UI client stores incoming state in the zustand store (`useChat(chatId).state`). Components like `IncidentView` and `MapboxIncidentMap` read `RobotPathPlan.path` to render updated waypoints.

## Python-side checklist
- Import models from `ag_ui.core` (`RunAgentInput`, `Message`, `Tool`, `StateSnapshotEvent`, etc.).
- Keep tool parameter schemas precise—docstrings and type hints become the JSON Schema sent to the UI.
- Use `ToolReturn(return_value=..., metadata=[...])` to emit both tool output and UI events.
- When adding new state fields, update the corresponding TypeScript interfaces and ensure snapshots include them.

## Frontend checklist
- Register the same tools/types in the AG-UI client so devtools can display schemas.
- Cast `chatState.state` to the shared interface (`RobotPathPlan`) before passing it to presentation components.
- Subscribe to SSE updates to keep UI responsive; AG-UI already batches message + state events for you.

## Debugging tips
- Log emitted `StateSnapshotEvent` payloads server-side and compare with what the browser receives.
- If the UI is missing data, confirm the tool returned metadata and that the zustand store is updated.
- Use the type docs linked above to verify both runtimes agree on message/tool/state structures.
