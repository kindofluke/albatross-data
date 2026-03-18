# How We Use AG-UI in This Project

AG-UI (Agent User Interaction Protocol) provides a standardized way to communicate between our Python backend agent and the React frontend. The key feature we leverage is **shared state synchronization** between the agent and UI.

## Overview

The AG-UI protocol defines:
- **Messages**: Structured communication between user, assistant, and tools
- **Tools**: Functions the agent can call with typed parameters
- **State**: Flexible data structure shared between backend and frontend
- **Events**: Real-time updates including `StateSnapshotEvent` for state sync

See the full type definitions:
- Python SDK: [python-ag-ui.md](./python-ag-ui.md)
- TypeScript SDK: [typescript-ag-ui.md](./typescript-ag-ui.md)

## Our State Model: RobotPathPlan

We define a shared state type `RobotPathPlan` in [server/cuopt_tool.py](../../server/cuopt_tool.py) that represents the robot path planning result:

```python
class RobotPathPlan(BaseModel):
    status: str
    optimization_engine: str | None = None
    path: PathInfo | None = None
    safety_analysis: SafetyAnalysis | None = None
    optimization_metrics: OptimizationMetrics | None = None
    recommendation: Recommendation | None = None
```

The `PathInfo` contains waypoints with coordinates that can be rendered on the map:

```python
class Waypoint(BaseModel):
    id: int | None = None
    name: str | None = None
    coordinates: tuple[float, float] | None = None  # (longitude, latitude)
    description: str | None = None

class PathInfo(BaseModel):
    waypoints: list[Waypoint] | None = None
    total_distance_meters: float | None = None
    estimated_time_seconds: int | None = None
    # ...
```

## Backend: Updating State with Tools

When the agent calls the `plan_robot_path` tool, it:

1. Creates a `RobotPathPlan` with waypoints and metrics
2. Updates the run context state: `ctx.deps = path_plan`
3. Returns a `StateSnapshotEvent` to sync with the frontend

```python
async def plan_robot_path(
    ctx: RunContext[StateDeps[RobotPathPlan]],
    start_location: str,
    destination: str,
    obstacles: str,
) -> ToolReturn:
    path_plan = RobotPathPlan(
        status="success",
        path=PathInfo(waypoints=[...]),
        # ...
    )

    # Update state
    ctx.deps = path_plan

    # Return with state snapshot to sync frontend
    return ToolReturn(
        return_value=path_plan.model_dump(),
        metadata=[
            StateSnapshotEvent(
                type=EventType.STATE_SNAPSHOT,
                snapshot=ctx.deps.model_dump(),
            ),
        ],
    )
```

## Frontend: Consuming State

The frontend receives state updates via the AG-UI protocol and stores them in a zustand store. Components can then access the state:

```typescript
// In ChatPage.tsx / IncidentView component
const chatState = useChat(chatId);
const agentState = chatState?.state as unknown as RobotPathPlan | undefined;
const robotPath = agentState?.path;

// Pass to map component
<MapboxIncidentMap
  robotPath={robotPath}
  // ...
/>
```

The `MapboxIncidentMap` component renders the waypoints as a path on the Mapbox map.

## Data Flow Diagram

```
┌─────────────────────────────────────────────────────────────────────┐
│                           USER                                       │
│                    "Plan a path to the leak"                         │
└─────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      FRONTEND (React)                                │
│  ChatTextInput → sendMessage() → AG-UI Client                        │
└─────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼ POST /api/agent (AG-UI protocol)
┌─────────────────────────────────────────────────────────────────────┐
│                      BACKEND (Python)                                │
│  1. Agent receives message                                           │
│  2. Agent decides to call plan_robot_path tool                       │
│  3. Tool creates RobotPathPlan with waypoints                        │
│  4. Tool updates ctx.deps = path_plan                                │
│  5. Tool returns StateSnapshotEvent                                  │
└─────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼ SSE stream with events
┌─────────────────────────────────────────────────────────────────────┐
│                      FRONTEND (React)                                │
│  1. AG-UI client receives StateSnapshotEvent                         │
│  2. State stored in zustand: useChat(chatId).state                   │
│  3. IncidentView reads state.path                                    │
│  4. MapboxIncidentMap renders waypoints on map                       │
└─────────────────────────────────────────────────────────────────────┘
```

## Key AG-UI Types Used

### StateSnapshotEvent (Python)
```python
from ag_ui.core import EventType, StateSnapshotEvent

StateSnapshotEvent(
    type=EventType.STATE_SNAPSHOT,
    snapshot=state_dict,  # Serialized state
)
```

### State (TypeScript)
```typescript
type State = any  // Flexible, we cast to RobotPathPlan
```

### ToolReturn (Python)
```python
from pydantic_ai import ToolReturn

ToolReturn(
    return_value=result,  # Tool result for the agent
    metadata=[event, ...],  # Events to send to frontend
)
```

## Frontend Types

We mirror the backend types in TypeScript for type safety:

```typescript
// frontend_web/src/types/robot-path.ts
interface Waypoint {
  id?: number;
  name?: string;
  coordinates?: [number, number];  // [lng, lat]
  description?: string;
}

interface PathInfo {
  waypoints?: Waypoint[];
  total_distance_meters?: number;
  // ...
}

interface RobotPathPlan {
  status: string;
  path?: PathInfo;
  // ...
}
```

## Benefits of This Architecture

1. **Type Safety**: Pydantic models on backend, TypeScript interfaces on frontend
2. **Real-time Updates**: State changes stream to frontend immediately
3. **Decoupled UI**: Frontend just renders state, doesn't know about tool implementation
4. **Flexible State**: Can add new fields to RobotPathPlan without protocol changes
5. **Debuggable**: State snapshots can be logged/inspected at both ends
