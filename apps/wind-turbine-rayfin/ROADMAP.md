# Wind Turbine Rayfin App — Roadmap

> Geo wind‑twin command center built on Fabric Rayfin (React 19 + Three.js + Vitest).
> Last updated: 2026‑07‑01

---

## Vision

An ontology‑grounded digital‑twin command center for multi‑site wind fleets: live 3D
geospatial monitoring, per‑turbine digital twins, forecasting, writeback to the Fabric
ontology, and natural‑language operations ("Ask Fabric IQ").

---

## ✅ Shipped (current build)

| Area | Capability |
|------|------------|
| **Map view** | 3D geospatial multi‑site farm, auto‑orbit camera, canvas world map, raycaster turbine picking, status coloring |
| **Digital Twin view** | Single scaled‑up turbine scene, ontology entity panel, live signal meters, related dispatch notes, turbine picker (dropdown + prev/next) |
| **Zoom** | Mouse‑wheel + ＋/－/⟳ controls on map and twin scenes; zoom works while paused |
| **Analytics view** | Fleet KPIs, output by site, fleet health, top performers, average wind by site |
| **Operations view** | Telemetry detail, sparkline with forecast overlay (predicted value + confidence band), power forecast (linear regression), writeback form (acknowledge/setpoint/note) |
| **Alerts view** | Active alarms/warnings list, acknowledge workflow persisted with who/when, show‑acknowledged toggle, unacknowledged badge on the nav rail, predictive anomaly watch |
| **Graph view** | Interactive ontology relationship graph (Fleet → Site → Turbine) with zoom, pan, hover edge‑tracing, status filter, and reset |
| **Forecasting** | Linear‑regression forecast with confidence bands, multi‑horizon (3/6/12), sparkline overlay |
| **Alarm automation** | Auto‑writes a `DispatchNote` the first time a turbine crosses into alarm (deduped per onset) |
| **Deep‑linking** | `view` + `selectedId` encoded in the URL hash for shareable views |
| **Ask Fabric IQ** | Grounded NL Q&A over telemetry + sites + notes, suggested questions |
| **Ontology backend** | `WindSite` + `DispatchNote` entities, ensure‑sites bootstrap, dispatch note persistence |
| **Shell** | Menu‑driven nav rail (with badges), KPI header strip, pause/resume, refresh interval, turbine detail modal |
| **Bundle** | Three.js split into its own cached chunk via `manualChunks` |
| **Quality** | 56 passing tests, clean `build:fabric`, one‑command `rayfin up` deploy |

---

## 🔜 Near term (next 1–2 iterations)

- [x] **Fabric Data Agent seam** — “Ask Fabric IQ” routes to a real deployed Data Agent when `VITE_DATA_AGENT_URL` is set (source `fabriciq`), with graceful fallback to the ontology‑grounded engine (`ontology`) and a pure offline engine (`local`). UI now labels the active engine honestly and flags telemetry as **simulated**.
- [x] **Real timeseries history** — replace seeded telemetry with persisted readings (Eventhouse/KQL) so sparklines and forecasts reflect actual data.
- [x] **Twin signal thresholds from ontology** — drive Meter warn/alarm bounds from ontology property metadata instead of hardcoded constants.
- [x] **Lazy‑load the Three.js scenes** — `React.lazy` + `Suspense` so the three chunk loads on demand (the chunk is already split; this defers it off the critical path).
- [x] **Alarm log & acknowledgement workflow** — active alarms list with persisted ack state (who/when) + auto `DispatchNote` on alarm onset.
- [x] **URL/state deep‑linking** — `view` + `selectedId` encoded in the URL for shareable views.

## 🟡 Mid term

- [x] **Relationship graph view** — interactive ontology graph (turbine → site → fleet) with zoom/pan/hover/filter.
- [x] **Forecast confidence + multi‑horizon** — confidence bands and selectable 3/6/12‑tick horizons.
- [x] **Selectable history windows** — 1h / 6h / 24h sparkline and forecast context for live and fallback history.
- [x] **Role‑based writeback** — operator vs viewer permissions on dispatch/setpoint actions.
- [ ] **Per‑site drill‑down dashboard** — site‑scoped KPIs and turbine grid.
- [x] **Mobile / responsive layout** — collapsible nav rail and stacked panels.

## 🟢 Long term

- [~] **Anomaly detection** — predictive anomaly watch + auto‑logging shipped; next: model‑based failure prediction from trends.
- [ ] **Teams proactive alerts** — push alarm notifications via the Operations Agent.
- [ ] **Multi‑ontology / domain switch** — reuse the shell for the other accelerator domains (Solar, Manufacturing, etc.).
- [ ] **Scenario / what‑if simulator** — model curtailment and maintenance windows against forecast output.

---

## Cross‑cutting / tech debt

- [ ] Expand test coverage beyond render smoke test (scene helpers, forecast math, filter logic).
- [ ] Extract Three.js scene logic from `App.tsx` into dedicated modules as it grows.
- [ ] Add error boundaries around the 3D canvases.
- [ ] Accessibility pass (keyboard nav, ARIA on nav rail and controls).

---

## Milestones

| Milestone | Goal | Exit criteria |
|-----------|------|---------------|
| **M1 — Real data** | Live persisted telemetry | Sparklines/forecasts use stored readings; no seeded data |
| **M2 — Ops workflow** | Closed‑loop alarms | Alarm list + ack + writeback fully wired to ontology |
| **M3 — Insight** | Relationship + anomaly | Graph view + early‑warning anomaly signals |
| **M4 — Scale** | Multi‑domain shell | Shell reused for ≥2 accelerator domains |
