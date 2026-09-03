# Refinery Twin Demo Brief

> Demo positioning and run-of-show for the Oil & Gas Refinery Rayfin app.
> Last updated: 2026-08-28

**Video showcase:** [Refinery Worldwide Rayfin demo](../apps/refinery-worldwide-rayfin/Refinery-video-demo.mp4)

## Demo thesis

**From plant signal to governed operational decision in one Fabric experience.**

The demo should show how Fabric can contextualize refinery telemetry, ontology relationships,
analytics, AI assistance, field coordination, and a governed simulation workflow in one
operator-facing command center.

This is an accelerator and decision-support demo. It is not a claim that the project replaces a
DCS, SIS, historian, CMMS/EAM, laboratory system, or a vendor's full industrial suite.

## Audience and outcome

The primary audience is an industrial data, operations, or digital-transformation team evaluating
how to connect existing plant systems to a modern data and AI operating layer.

By the end, the audience should be able to answer three questions:

1. Can one refinery incident be traced from fleet context to process-unit evidence?
2. Can the same context support a field action and an auditable decision package?
3. Can the pattern aggregate across sites without losing asset identity or data provenance?

## Reference landscape

The following comparison is a positioning aid, not a feature certification or procurement scorecard.
Product capabilities vary by edition, deployment model, partner integrations, and implementation.

| Capability | This accelerator | AVEVA PI / CONNECT | AspenTech industrial suite | Siemens Industrial Operations X | Hexagon / HxGN | Cognite Data Fusion |
|---|---|---|---|---|---|---|
| Historian and high-frequency OT collection | Uses Fabric semantic model and Eventhouse telemetry; ingestion connectors are the next integration layer | Strong historian, time-series, and industrial data foundation | Strong process-industry data, modeling, and optimization portfolio | Strong industrial connectivity and operations portfolio | Strong asset and engineering information context | Strong contextualization and industrial data integration |
| Contextual asset model | Ontology entities and relationships across refinery, unit, equipment, sensor, alarm, tank, and pipeline | Context and asset frameworks are a reference pattern to integrate with | Strong process and asset context, especially around engineering and operations | Strong asset and industrial knowledge context | Strong engineering, reality, and asset lifecycle context | Strong industrial knowledge graph and contextualization |
| Cross-site aggregation | Native Fabric semantic model, graph, KQL, and dashboard pattern across demo refineries | Typically assembled through PI System and enterprise layers | Portfolio and implementation dependent | Portfolio and implementation dependent | Portfolio and implementation dependent | Designed for cross-source and cross-site contextual access |
| Real-time operational view | Rayfin globe, process-unit twin, alerts, schematic, analytics cockpit | PI visualization products and partner ecosystem | Operations and performance applications | Industrial operations applications | Operations and asset applications | Operational applications and data products |
| Field management | Demo-safe work-order draft, responder ranking, dispatch note, escalation timeline | Requires connected work management / partner process for execution | Can connect to maintenance and operations workflows | Can connect to execution and maintenance workflows | Strong EAM/asset-management adjacency depending on product scope | Workflow and application layer typically requires implementation |
| Simulation and decision support | Governed client/backend simulation run, provenance, economics, review decision; domain model is intentionally bounded | Often paired with asset performance, analytics, and engineering tools | A major portfolio strength in process optimization and planning | Strong engineering, automation, and industrial optimization adjacency | Engineering and asset lifecycle tools provide complementary analysis | Analytics and operational decision applications require domain models |
| AI interaction | Fabric Data Agent seam plus ontology-grounded fallback; Ask Fabric IQ | AI depends on connected AVEVA capabilities and solution architecture | AI/optimization capabilities vary by product and implementation | Industrial AI capabilities vary by product and deployment | AI capabilities vary by product and solution | Strong contextualized data foundation for industrial AI applications |
| Governance and enterprise analytics | Fabric semantic model, lineage-friendly deployment artifacts, RBAC path, and decision-package records | Enterprise governance is available through the platform and implementation architecture | Enterprise governance is available through the platform and implementation architecture | Enterprise governance is available through the platform and implementation architecture | Enterprise governance is available through the platform and implementation architecture | Governance depends on platform configuration and source integration |

### The honest differentiation

The strongest claim is not “better historian” or “better CMMS.” The strongest claim is:

- **Fabric-native convergence:** telemetry, lakehouse/warehouse data, semantic models, KQL,
  ontology, graph, dashboards, AI, and Rayfin in one deployment pattern.
- **Context before conversation:** Ask Fabric IQ answers against refinery entities and signals,
  rather than presenting an uncontextualized chatbot.
- **Decision traceability:** a simulation run records baseline source, timestamp, inputs, outputs,
  economics, and review status instead of silently changing a live control value.
- **Cross-site operating picture:** the same asset vocabulary supports a worldwide fleet view and
  site or process-unit drill-down.
- **Integration posture:** existing AVEVA, AspenTech, Siemens, Hexagon, Cognite, DCS, historian,
  and CMMS investments remain systems of record; Fabric adds an aggregation and intelligence layer.

## Ten-minute run of show

### 0:00-1:00 | Frame the problem

Open the Refinery Worldwide Rayfin app and state the thesis. Clarify that the telemetry is either
live through the configured semantic-model alias or explicitly marked simulated.

Show:

- Worldwide refinery fleet
- Fleet health and active alarm count
- Data trust / freshness indicator

Say: "This is the enterprise operating picture above the plant systems, not a replacement for the
control system."

### 1:00-2:30 | Locate the incident

Use the globe to select the affected refinery and process unit. Keep the geography visible long
enough to establish multi-site aggregation, then drill into the unit.

Show:

- Site and unit identity
- Alarm severity and latest reading
- Related equipment and sensors

### 2:30-4:00 | Prove context in the twin

Switch between **3D twin** and **Process schematic**. Use the schematic for the fastest explanation
of process flow and the 3D view for asset orientation.

Show:

- Live signal values and thresholds
- Feed, unit, exchanger, product, or tank path
- Alarm evidence and affected asset

Bridge to AVEVA and other industrial platforms: "The value here is the contextual operating view
and the Fabric data layer; a production deployment would preserve the historian as the authoritative
high-frequency source."

### 4:00-5:00 | Trace dependencies

Open Graph and follow refinery -> process unit -> equipment -> sensor/alarm relationships.

Show:

- Why the alarm belongs to this unit
- What neighboring assets or flows may be affected
- The difference between an isolated signal and an operational context

### 5:00-6:30 | Coordinate field response

Open the work-order and dispatch flow. Select the evidence item, responder, priority, and escalation
path. Treat the action as a tracked work-order draft or dispatch note unless a real CMMS connector
has been configured.

Show:

- Evidence attached to the incident
- Responder fit and availability
- SLA state and escalation timeline
- Durable dispatch/work-order record

Do not imply that the demo action controls equipment or completes a real maintenance job.

### 6:30-8:30 | Simulate a decision

Open **Simulation**. Import or use the prepared scenario, confirm baseline provenance, review
throughput and economics, and submit the decision package for review.

Show:

- Purpose, objective, horizon, and baseline timestamp
- Throughput, margin, maintenance, and energy assumptions
- Validation status and scenario outputs
- Backend-persisted run ID and approval/rejection record

Say: "The simulation recommends and records; it does not write a setpoint to the plant."

### 8:30-9:30 | Ask Fabric IQ

Ask a question that requires both context and action, for example:

> Which refinery unit has the highest operational risk, what evidence supports it, and what should
> the duty manager review before approving the maintenance scenario?

Point out the answer source label: live Fabric Data Agent, ontology-grounded engine, or local
fallback. Never present the fallback as live plant intelligence.

### 9:30-10:00 | Close on scale and next step

Return to Analytics / Global Refinery Cockpit and close with the comparison position:

> "AVEVA PI and other industrial platforms remain valuable sources of operational truth. This
> accelerator demonstrates how Fabric can aggregate that truth with enterprise data, ontology,
> AI, simulation, and governed workflows across the fleet."

## Demo readiness checklist

### Before the meeting

- [ ] Confirm the Fabric workspace and Rayfin hosting URL.
- [ ] Confirm the semantic-model alias and inspect the telemetry freshness badge.
- [ ] Confirm `SimulationRun` and `SimulationApproval` exist in the Rayfin backend.
- [ ] Load one prepared incident with a named unit, evidence item, responder, and open order.
- [ ] Load one simulation scenario with documented baseline and cost assumptions.
- [ ] Decide whether the demo is live-data, synthetic-data, or hybrid; say so explicitly.
- [ ] Open the app once in the embedded Fabric shell and verify map, twin, graph, Ask IQ, and Simulation.
- [ ] Prepare a local export of the decision package as a backup.

### During the meeting

- [ ] Keep the demo on one incident and one decision.
- [ ] Show provenance before showing the recommendation.
- [ ] Distinguish recommendation, dispatch draft, and plant control.
- [ ] Use the process schematic when the audience needs flow; use 3D when they need spatial context.
- [ ] If a live connector is unavailable, label the fallback immediately and continue the story.

### Claims to avoid

- Do not call the app a historian replacement.
- Do not call a demo work order a completed CMMS transaction without a configured connector.
- Do not describe local or synthetic telemetry as live plant data.
- Do not claim closed-loop control, SIS integration, or automatic setpoint execution.
- Do not use the matrix as a formal vendor benchmark without validating current product editions and scope.

## Post-demo product gaps

The next credible investments are:

1. Connector contracts for historian, DCS/SCADA, CMMS/EAM, laboratory, and work-management systems.
2. A backend-owned process model with constraints, yield, energy, throughput, uncertainty, and
   sensitivity analysis.
3. Idempotent work-order integration with external IDs, status synchronization, and approval policy.
4. Identity-aware roles for operator, planner, reliability engineer, approver, and viewer.
5. Data-quality evidence: freshness, completeness, late-arriving data, unit normalization, and
   source lineage per signal.
6. A scripted demo mode that locks the incident, cancels safely, captures the exact work-order ID,
   and produces one final mission report.

These gaps are part of the product roadmap, not reasons to overstate the current demo.
