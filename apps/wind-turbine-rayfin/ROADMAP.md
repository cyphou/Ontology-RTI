# Wind Turbine Rayfin App — Roadmap

> Geo wind‑twin command center built on Fabric Rayfin (React 19 + Three.js + Vitest).
> Last updated: 2026‑07‑14

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
| **Guided demo script** | Header-level staged demo runner (Prepare story → Show evidence → Dispatch lead → AutoHeal) with a `Run all` button, per-step buttons, a `Step X of 4` badge, active-step highlighting, and a ~2s cadence between events |
| **Technician dispatch workflow** | WorkIQ-style responder ranking (skill/site/shift/on-call/load), skill-matched mock evidence, role/asset-aware persona + asset imagery, in-app technician card + separate browser popup, viewer-mode demo override so dispatch/escalation never blocks |
| **Dispatch quality tool** | Readiness scoring with READY/MISSING checklist (lead technician, evidence, priority, story content) surfaced before dispatch |
| **Mission challenge** | Mission Panel `actions` tab with readiness score/verdict, objective checklist (triage/order/escalation/drill), and one-click runbook actions |
| **Streamlined shell** | Decluttered header (single Demo Script block + live controls) and consolidated Operations/mission actions to reduce button overload |
| **Alerts view** | Active alarms/warnings list, acknowledge workflow persisted with who/when, show‑acknowledged toggle, unacknowledged badge on the nav rail, predictive anomaly watch |
| **Graph view** | Interactive ontology relationship graph (Fleet → Site → Turbine) with zoom, pan, hover edge‑tracing, status filter, and reset |
| **Forecasting** | Linear‑regression forecast with confidence bands, multi‑horizon (3/6/12), sparkline overlay |
| **Alarm automation** | Auto‑writes a `DispatchNote` the first time a turbine crosses into alarm (deduped per onset) |
| **Predictive work orders** | Raise a structured, tracked `MaintenanceOrder` from a turbine — suspected component + P1/P2/P3 priority derived from the anomaly escalation forecast, planned curtailment/downtime and projected energy impact pulled from the what‑if simulator; operator‑gated, fallback‑safe |
| **Deep‑linking** | `view` + `selectedId` encoded in the URL hash for shareable views |
| **Ask Fabric IQ** | Grounded NL Q&A over telemetry + sites + notes, suggested questions |
| **Ontology backend** | `WindSite` + `DispatchNote` + `SensorThreshold` + `MaintenanceOrder` + `TurbineDevice` entities, ensure‑sites bootstrap, dispatch note / work-order persistence, twin graph hierarchy persistence |
| **Twin graph authoring** | In-app Twin Graph Admin for backend edits (label/property/unit/note/camera vectors/order), with save/reset/add/delete actions and live scene refresh |
| **Shell** | Menu‑driven nav rail (with badges), KPI header strip, pause/resume, refresh interval, turbine detail modal |
| **Bundle** | Three.js split into its own cached chunk via `manualChunks` |
| **Quality** | 115+ passing tests, clean `build:fabric`, one‑command `rayfin up` deploy |

---

## 🔜 Near term (next 1–2 iterations)

### Demo & storytelling
- [x] **Jury/demo run report** — `Export Report` in the Mission Panel builds a per-run Mission Report (JSON) via the pure `buildMissionReport` helper, capturing turbine/site/component/priority, responder, dispatch-quality and challenge scores, scripted step events with timestamps, total run duration, and outcome. Unit-tested (127 tests total).
- [x] **SLA countdown visual** — the Operations SLA panel now shows a countdown ring with elapsed-% and color-coded urgency (on track / warning / critical / breached) driven by the pure `slaUrgency` helper, plus the regional-escalation prompt at breach. Unit-tested (132 tests total).
- [x] **Scripted narration overlay** — a bottom-center caption layer narrates each demo-script step (title + jury-facing caption + `Step X of N` progress) via the pure `demoNarration` helper; shows during auto-play and manual steps with a dismiss control. Unit-tested (134 tests total).

### Platform
- [x] **Fabric Data Agent seam** — “Ask Fabric IQ” routes to a real deployed Data Agent when `VITE_DATA_AGENT_URL` is set (source `fabriciq`), with graceful fallback to the ontology‑grounded engine (`ontology`) and a pure offline engine (`local`). UI now labels the active engine honestly and flags telemetry as **simulated**.
- [x] **Real timeseries history** — replace seeded telemetry with persisted readings (Eventhouse/KQL) so sparklines and forecasts reflect actual data.
- [x] **Twin signal thresholds from ontology** — drive Meter warn/alarm bounds from ontology property metadata instead of hardcoded constants.
- [x] **Lazy‑load the Three.js scenes** — `React.lazy` + `Suspense` so the three chunk loads on demand (the chunk is already split; this defers it off the critical path).
- [x] **Alarm log & acknowledgement workflow** — active alarms list with persisted ack state (who/when) + auto `DispatchNote` on alarm onset.
- [x] **URL/state deep‑linking** — `view` + `selectedId` encoded in the URL for shareable views.

## � Next phase (M6 — Operationalize the demo experience)

> M5 (Demo-ready) is fully shipped. M6 turns the guided storytelling into repeatable,
> shareable, cross-domain operational value.

### Portability
- [x] **Extract the demo engine** — the pure demo/challenge/SLA/narration helpers (`buildMissionChallenge`, `buildMissionReport`, `slaUrgency`, `demoNarration`) plus their types now live in a shared, React-free `services/demo-experience.service.ts` with its own spec; `App.tsx` imports and re-exports them so Solar and Refinery can reuse the engine without copy/paste.
- [x] **Domain-agnostic demo manifest** — a typed `DomainDemoManifest` (entity/asset nouns, components, responder roles, evidence labels, and per-step narration) now drives the guided walkthrough via `narrateStep(manifest, step)`; `WIND_DEMO_MANIFEST` is the Wind default and `demoNarration` delegates to it, so Solar/Refinery only need to supply their own manifest. Unit-tested (137 tests total).

### Operational depth
- [x] **Run history & replay** — Mission Reports are persisted to `localStorage` (capped at 10) on export and at auto-run completion via the pure `pushMissionRun`/`summarizeMissionRun` helpers; the Mission Panel shows a recent-runs list, and selecting an entry re-focuses that turbine with a read-only replay summary. Unit-tested (144 tests total).
- [x] **Escalation timeline** — a compact vertical dispatch → manager → regional timeline in the Operations SLA panel with done/current/pending states, SLA-breach notes, and urgency-colored markers, driven by the pure `buildEscalationTimeline` helper. Unit-tested (141 tests total).
- [x] **Responder availability board** — a compact roster strip above the Operations responder list shows in-scope count, on-call and free counts, and the day/swing/night distribution, re-rolling live as shift/on-call filters change, via the pure `summarizeResponderAvailability` helper. Unit-tested (149 tests total).

### Trust & polish
- [x] **A11y pass on new surfaces** — the Demo Script popover is a labelled `role="dialog"` (Escape + click-scrim to close, `aria-haspopup`/`aria-expanded` on the trigger), the narration overlay is an `aria-live="polite"` status region, and the SLA ring exposes a screen-reader group label with priority, urgency, and elapsed-%.
- [x] **Snapshot/report parity tests** — parity tests lock the exported Mission Report field set and its deterministic field mapping (ignoring the timestamp), plus the run-summary shape, so future changes to the report structure are caught. Unit-tested (147 tests total).

## �🟡 Mid term

- [x] **Relationship graph view** — interactive ontology graph (turbine → site → fleet) with zoom/pan/hover/filter.
- [x] **Forecast confidence + multi‑horizon** — confidence bands and selectable 3/6/12‑tick horizons.
- [x] **Selectable history windows** — 1h / 6h / 24h sparkline and forecast context for live and fallback history.
- [x] **Role‑based writeback** — operator vs viewer permissions on dispatch/setpoint actions.
- [x] **Per‑site drill‑down dashboard** — site‑scoped KPIs and turbine grid.
- [x] **Mobile / responsive layout** — collapsible nav rail and stacked panels.

## 🟢 Long term

- [x] **Anomaly detection** — predictive anomaly watch + auto‑logging, plus slope‑based escalation forecast (rising/falling trend + ETA‑to‑alarm) from a rolling anomaly‑score window.
- [x] **Teams proactive alerts** — fallback-safe incoming-webhook seam posts a MessageCard on alarm onset when `VITE_TEAMS_WEBHOOK_URL` is set (no-op otherwise).
- [~] **Multi‑ontology / domain switch** — reuse the shell for the other accelerator domains (Solar, Manufacturing, etc.). Scaffolder `apps/tools/scaffold-domain-app.mjs` stamps a new domain app from a template via a tested domain manifest; per-domain scene/telemetry authored on top.
- [x] **Scenario / what‑if simulator** — model curtailment and maintenance windows against forecast output.

## 🔶 Next phase (M4 — Multi-domain shell)

> M6 is fully shipped. M4 now reuses the shared demo engine across accelerator domains.

- [x] **Domain manifest registry** — `DEMO_MANIFESTS` registers `wind-turbine`, `solar-farm`, and `oil-gas-refinery` manifests with a `getDemoManifest(domainId)` lookup (Wind fallback); each domain supplies its own nouns/components/responder roles/evidence/step narration, and the shared engine drives the guided script unchanged. Unit-tested (152 tests total).
- [ ] **Port the demo engine to Solar** — copy `demo-experience.service.ts` into `solar-france-rayfin` (default `SOLAR_DEMO_MANIFEST`) and wire the Demo Script popover + mission challenge into the Solar `App.tsx`.
- [ ] **Port the demo engine to Refinery** — same for `refinery-worldwide-rayfin` with `REFINERY_DEMO_MANIFEST`.

---

## Cross‑cutting / tech debt

- [~] Expand test coverage beyond render smoke test (scene helpers, forecast math, filter logic). App logic suite now includes mission challenge scoring and dispatch flow helper coverage (124 tests total).
- [x] Extract Three.js scene logic from `App.tsx` into dedicated modules as it grows.
- [x] Add error boundaries around the 3D canvases.
- [x] Accessibility pass (keyboard nav, ARIA on nav rail and controls).

---

## Milestones

| Milestone | Goal | Exit criteria |
|-----------|------|---------------|
| **M1 — Real data** | Live persisted telemetry | Sparklines/forecasts use stored readings; no seeded data |
| **M2 — Ops workflow** | Closed‑loop alarms | Alarm list + ack + writeback fully wired to ontology |
| **M3 — Insight** | Relationship + anomaly | Graph view + early‑warning anomaly signals |
| **M4 — Scale** | Multi‑domain shell | Shell reused for ≥2 accelerator domains |
| **M5 — Demo-ready** | Guided challenge storytelling | Staged demo script + technician dispatch + mission challenge, with an exportable run report and SLA countdown |
| **M6 — Operationalize** | Reusable, shareable demo experience | Demo engine extracted to a shared module + data-driven domain manifest + run history/replay |
