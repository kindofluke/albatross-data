---
name: hex-heatmap
description: How the async hex heatmap pipeline works in this repo — from PostGIS query through background thread, file buffering, polling API, and MapLibre GL rendering.
---

# Skill: Async Hex Heatmap Pipeline

## Overview

The hex heatmap feature computes a PostGIS `ST_HexagonGrid` aggregate over the RF captures table and renders it on a MapLibre GL map backed by a local PMTiles file. Because the query can take 30–120 seconds, the agent returns immediately with a `pending` state snapshot and the frontend polls until the file is ready.

---

## Architecture: end-to-end flow

```
User prompt
  → agent calls hex_heatmap() tool
      → emits StateSnapshotEvent { heatmap_layers: [{file_id, status:"pending"}] }
      → spawns background thread → POST /predictionsUnstructured (HEX_HEATMAP_AGGREGATE)
          → Java: ST_HexagonGrid + ST_ConvexHull query
          → writes {file_id}.geojson to HEATMAP_DIR
  → frontend receives StateSnapshotEvent → shows map section + "heatmap computing…" badge
  → useHeatmapPolling hook polls GET /api/heatmap/{file_id} every 3s
      → status: "pending" → keep polling
      → status: "ready"   → fetch GET /api/heatmap/{file_id}/data → store in hexData Map
      → status: "error"   → log error, stop polling
  → HighlightMap receives hexLayers={Array.from(hexData.values())}
      → renders fill + line layers per bin, coloured by normalised RSSI
      → renders dashed convex hull perimeter
      → auto-fits map bounds to data
```

---

## Key files

| File | Role |
|------|------|
| `agent/src/tools.py` | `hex_heatmap()` tool — queues background thread, emits pending state |
| `agent/src/agent.py` | `HeatmapLayer` model in `AgentState`; tool registered with `agent.tool(hex_heatmap)` |
| `agent/src/config.py` | `heatmap_dir: str` config field (env var `HEATMAP_DIR`, default `/tmp/deepwave_heatmaps`) |
| `metadata-tools/.../CaptureQuery.java` | `aggregateHexHeatmap()` — native SQL with PostGIS hex grid + convex hull |
| `metadata-tools/.../HeatmapBinResult.java` | DTO: `List<HexBin> bins` + `String perimeterJson` |
| `metadata-tools/.../CaptureAggregate.java` | `HEX_HEATMAP_AGGREGATE` enum value |
| `frontend/server/main.py` | `GET /api/heatmap/{file_id}` (status poll) + `GET /api/heatmap/{file_id}/data` (GeoJSON serve) + `GET /api/tiles/{filename}` (PMTiles range-request server) |
| `frontend/server/config.py` | `heatmap_dir: str` — must match agent config |
| `frontend/src/types/HeatmapLayer.ts` | `HeatmapLayer`, `HexBin`, `HeatmapBinResult` TypeScript types |
| `frontend/src/hooks/use-heatmap-polling.ts` | `useHeatmapPolling(layers)` — polls status, fetches data, returns `Map<file_id, HeatmapBinResult>` |
| `frontend/src/components/HighlightMap.tsx` | MapLibre GL map — renders base tiles, hex fill/line layers, perimeter, pin markers |
| `frontend/src/pages/Home.tsx` | Wires `heatmapLayers` state + `hexData` polling into `<HighlightMap>` |

---

## Java: PostGIS hex aggregate (`CaptureQuery.aggregateHexHeatmap`)

The query runs in two parts inside the `resultSupplier` lambda:

**Part 1 — hex grid bins:**
```sql
WITH points AS (
  SELECT ST_SetSRID(ST_MakePoint(longitude, latitude), 4326) AS geom, rssi
  FROM public.captures
  [WHERE <nativeConditions>]
),
bounds AS (
  SELECT ST_SetSRID(ST_Extent(geom)::geometry, 4326) AS bbox FROM points
),
hex_size AS (
  SELECT SQRT(GREATEST(ST_Area(bbox) / 500.0, 0.00000001) / 2.598076) AS side_length, bbox
  FROM bounds
),
grid AS (
  SELECT (ST_HexagonGrid(
    (SELECT side_length FROM hex_size),
    (SELECT bbox FROM hex_size)
  )).geom AS geom
)
SELECT ST_AsGeoJSON(g.geom), AVG(p.rssi), COUNT(p.rssi)
FROM grid g
JOIN points p ON ST_Intersects(p.geom, g.geom)
GROUP BY g.geom
```
Targets ~500 hexagons by deriving side length from bounding box area.

**Part 2 — convex hull perimeter:**
```sql
SELECT ST_AsGeoJSON(ST_ConvexHull(ST_Collect(ST_SetSRID(ST_MakePoint(longitude, latitude), 4326))))
FROM public.captures
[WHERE <nativeConditions>]
```
Returns the tightest polygon wrapping all matching points — rendered as a dashed outline.

**Result DTO:**
```java
public record HeatmapBinResult(List<HexBin> bins, String perimeterJson) implements AggregateResult {
  public record HexBin(String geometryJson, double avgRssi, long pointCount) {}
}
```
`AggregateResult` is a sealed interface — add `HeatmapBinResult` to its `permits` clause.

---

## Python agent: `hex_heatmap` tool

```python
async def hex_heatmap(ctx: RunContext, tool_steps: List[dict], label: Optional[str]) -> ToolReturn:
    file_id = str(uuid.uuid4())
    out_path = _get_heatmap_dir() / f"{file_id}.geojson"

    # 1. Add pending layer to state immediately
    layer = HeatmapLayer(file_id=file_id, status="pending", label=label)
    ctx.deps.state.heatmap_layers.append(layer)

    # 2. Emit StateSnapshotEvent so frontend shows the map section right away
    snapshot = StateSnapshotEvent(type=EventType.STATE_SNAPSHOT, snapshot=ctx.deps.state.model_dump())

    # 3. Fire background thread — agent returns without waiting
    def _run_query():
        payload = {"toolSteps": tool_steps, "aggregate": {"aggregate": "HEX_HEATMAP_AGGREGATE", "params": None}}
        r = requests.post(_metadata_server_url("/predictionsUnstructured"), json=payload, timeout=600, headers=headers)
        r.raise_for_status()
        out_path.write_text(json.dumps(r.json()), encoding="utf-8")
        # On error: out_path.with_suffix(".error").write_text(str(exc))

    threading.Thread(target=_run_query, daemon=True).start()
    return ToolReturn(return_value=f"Hex heatmap queued (file_id={file_id})...", metadata=[snapshot])
```

**`AgentState` additions:**
```python
class HeatmapLayer(BaseModel):
    file_id: str
    status: str = "pending"  # "pending" | "ready" | "error"
    label: str | None = None

class AgentState(BaseModel):
    highlights: list[HighlightPeriod] = []
    map_highlights: list[MapHighlightPoint] = []
    heatmap_layers: list[HeatmapLayer] = []
```

**Config** (`agent/src/config.py` and `frontend/server/config.py`):
```python
heatmap_dir: str = "/tmp/deepwave_heatmaps"
```
Both sides must point at the same directory. Set `HEATMAP_DIR` env var to override.

---

## Frontend server: heatmap + PMTiles routes (`frontend/server/main.py`)

**Status poll:**
```python
@app.get("/api/heatmap/{file_id}")
async def heatmap_status(file_id: str) -> JSONResponse:
    geojson_path = _HEATMAP_DIR / f"{file_id}.geojson"
    error_path   = _HEATMAP_DIR / f"{file_id}.error"
    if geojson_path.is_file(): return JSONResponse({"status": "ready", "url": f"/api/heatmap/{file_id}/data"})
    if error_path.is_file():   return JSONResponse({"status": "error", "detail": error_path.read_text()})
    return JSONResponse({"status": "pending"})
```

**Data serve:**
```python
@app.get("/api/heatmap/{file_id}/data")
async def heatmap_data(file_id: str) -> JSONResponse:
    return JSONResponse(json.loads((_HEATMAP_DIR / f"{file_id}.geojson").read_text()))
```

**PMTiles range-request server** (critical — PMTiles clients fetch only the byte ranges they need):
```python
@app.get("/api/tiles/{filename}")
async def serve_pmtiles(filename: str, request: Request) -> Response:
    # Parses Range: bytes=X-Y header → returns 206 Partial Content
    # Falls back to full 200 if no Range header
    # Validates filename against r"[\w\-]+\.pmtiles"
```
The 170 MB `philadelphia_metro.pmtiles` file lives at `frontend/server/static/` and is never transferred in full — only the tiles needed for the current viewport are fetched.

---

## Frontend: `useHeatmapPolling` hook

```typescript
export function useHeatmapPolling(layers: HeatmapLayer[]): Map<string, HeatmapBinResult> {
  // For each layer with status "pending" not already tracked:
  //   poll GET /api/heatmap/{file_id} every 3s
  //   on "ready": fetch /api/heatmap/{file_id}/data → store in dataMap
  //   on "error": log and stop
  // Returns Map<file_id, HeatmapBinResult>
}
```

---

## Frontend: `HighlightMap` (MapLibre GL)

Replaces the old Plotly `scattergeo` component. Key design decisions:

- **Base map**: Protomaps Basemaps (`protomaps-themes-base`) with `namedTheme("dark")` — gives correct layer names for the Protomaps PMTiles schema (`earth`, `water`, `roads`, `buildings`, `places`). Background and water colours patched to Deepwave navy palette.
- **PMTiles protocol**: `pmtiles://` registered once globally via `new Protocol()` from the `pmtiles` package.
- **Hex bins**: One MapLibre `fill` + `line` layer pair per `HeatmapBinResult`. Each bin's GeoJSON Polygon is stored as a feature with a `norm` property (0–1 normalised RSSI). Colour ramp: `#0037dc` (weak) → `#9c27b0` (mid) → `#ff8a00` (strong).
- **Perimeter**: Dashed orange `line` layer from `perimeterJson` (convex hull).
- **Pin markers**: Custom DOM elements coloured by label group, with click popups.
- **Lifecycle**: Map initialised once in `useEffect([], [])`. Dynamic layers tracked by ID in refs and cleaned up on each data update via `clearDynamic()`.
- **Bounds**: `map.fitBounds()` called whenever data changes.

**Vite chunk split** (`vite.config.ts`):
```typescript
if (id.includes("maplibre-gl") || id.includes("pmtiles")) return "maplibre";
if (id.includes("plotly") || id.includes("react-plotly"))  return "plotly";
```

---

## Home.tsx: map section visibility

The map section is shown when any of these are true:
```tsx
{(mapHighlights.length > 0 || hexData.size > 0 || heatmapLayers.some(l => l.status === "pending")) && (
  <section>
    <h3>
      Map highlights
      {heatmapLayers.some(l => l.status === "pending") && (
        <span><span className="animate-pulse" /> heatmap computing…</span>
      )}
    </h3>
    <HighlightMap points={mapHighlights} hexLayers={Array.from(hexData.values())} />
  </section>
)}
```
This ensures the map appears as soon as the agent queues a heatmap, before any data arrives.

---

## Adding a new aggregate (checklist)

1. Add enum value to `CaptureAggregate.java`
2. Create result DTO implementing `AggregateResult` (add to `permits` clause)
3. Implement `aggregateXxx()` in `CaptureQuery.java`, wire into `applyAggregate()` switch
4. If slow: follow the `hex_heatmap` pattern — background thread + file + polling
5. If fast: call `data_execute()` directly from the agent and return inline JSON
6. Add TypeScript types in `frontend/src/types/`
7. Render in `HighlightMap.tsx` as a new MapLibre layer

---

## Gotchas

- **`HEATMAP_DIR` must be the same path** for both the agent process and the frontend server process. In local dev both default to `/tmp/deepwave_heatmaps`. In production set the env var on both containers.
- **PMTiles schema**: This repo uses Protomaps Basemaps schema (source layers: `earth`, `water`, `roads`, `buildings`, `places`, `pois`). Do NOT use OpenMapTiles layer names (`land`, `transportation`, `place`) — they won't match.
- **Sealed interface**: Every new `AggregateResult` subtype must be added to the `permits` clause in `AggregateResult.java` or the Java compiler will reject it.
- **Map visibility**: The `HighlightMap` component is lazy-loaded. The map section must be conditionally rendered based on `mapHighlights.length > 0 || hexData.size > 0 || heatmapLayers.some(l => l.status === "pending")` — not just `mapHighlights.length > 0`, or heatmap-only responses won't show the map.
- **`clearDynamic()` before re-adding layers**: Always remove old MapLibre layers/sources before adding new ones, or you'll get "source already exists" errors on re-render.
