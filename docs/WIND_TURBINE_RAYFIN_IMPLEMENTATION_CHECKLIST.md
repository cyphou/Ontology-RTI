# Wind Turbine Rayfin Implementation Checklist

Use this checklist as an execution companion to:

1. docs/WIND_TURBINE_RAYFIN_PLAN.md
2. docs/WIND_TURBINE_RAYFIN_ROADMAP.md
3. docs/WIND_TURBINE_RAYFIN_TASKBOARD.csv

## Global Definition of Done

1. Feature behavior is verifiable locally and in Fabric.
2. Data contract remains backward compatible unless explicitly versioned.
3. No unresolved P0 defects in the active milestone.
4. Documentation and demo steps are updated with each milestone.

## Milestone M1: Live 3D scene with data binding

### Contract and model

- [ ] Turbine entity finalized.
- [ ] Component entity finalized.
- [ ] TurbineTelemetry entity finalized.
- [ ] Status enum values fixed: healthy, warning, alarm.
- [ ] sceneObjectId mapping documented and validated.

### App and scene

- [ ] Rayfin app scaffold runs locally.
- [ ] Three.js scene renders 8 to 12 turbines.
- [ ] Orbit/navigation controls are usable.
- [ ] Turbine selection is functional.
- [ ] Details panel displays turbine metrics.

### Data flow

- [ ] Seed data is loaded.
- [ ] Telemetry generator writes updates.
- [ ] UI refresh loop updates turbine status.
- [ ] Status color changes reflect telemetry state.

### Deployment and validation

- [ ] Fabric deployment succeeds.
- [ ] SSO path is verified.
- [ ] No unmapped turbine IDs detected.
- [ ] Data-to-visual latency is under 3 seconds target.

## Milestone M2: Part 1 published

### Content readiness

- [ ] Architecture diagram exported.
- [ ] Commands and snippets tested.
- [ ] Screenshots captured for local and Fabric runs.
- [ ] Common pitfalls section verified against actual issues.

### Publishing readiness

- [ ] Draft reviewed by engineering owner.
- [ ] Draft reviewed by platform owner.
- [ ] Final post published with reproducible steps.

## Milestone M3: Realistic visual twin

### Asset pipeline

- [ ] Blender turbine imported.
- [ ] Terrain pipeline implemented.
- [ ] Asset naming follows mapping rules.

### Performance

- [ ] Draw call budget documented and within target.
- [ ] Texture memory budget documented and within target.
- [ ] LOD strategy implemented and tested.

### Regression checks

- [ ] Existing data bindings remain valid.
- [ ] Selection and details panel remain stable.
- [ ] Telemetry-to-visual updates remain under SLA target.

## Milestone M4: AI and voice integrated

### Agent integration

- [ ] Foundry agent connected to Fabric KQL context.
- [ ] Text copilot path works with selected turbine context.
- [ ] Guardrails and fallback responses implemented.

### Voice interaction

- [ ] Voice input path works.
- [ ] Voice output path works.
- [ ] Text fallback path works when voice fails.

### Observability

- [ ] Interaction traces are captured.
- [ ] Error telemetry is captured.
- [ ] Grounding quality checks are recorded.

## Milestone M5: Demo-ready hardening

### Reliability and security

- [ ] 30-minute demo run completes with zero blockers.
- [ ] Auth and permission boundaries validated.
- [ ] Recovery steps for known failures documented.

### Handoff package

- [ ] Final architecture summary updated.
- [ ] Demo runbook finalized.
- [ ] Troubleshooting guide finalized.
- [ ] Taskboard statuses and closure notes updated.

## Weekly operating checklist

- [ ] Review taskboard status and blockers.
- [ ] Review KPI trend: latency, reliability, defects.
- [ ] Review dependencies and re-sequence if needed.
- [ ] Update release notes for completed items.
