# Changelog

## [Unreleased]

### Added
- `deploy/visual-mapping.json`: an authoritative business-need to visual mapping, grounded in Microsoft report and dashboard design guidance, that drives the report design lint instead of only documenting it
- `Test-VisualFit`: a data-aware lint that runs during the mockup and rejects visual choices the live data cannot support (a donut with too many slices, a waterfall where nothing subtracts, a funnel whose stages do not fall)
- `deploy/domain-branding.json` plus `deploy/New-DomainLogos.ps1`: one palette and one generated logo lockup per ontology domain, derived from that domain's own icon and shared by the report theme, the logo and the mockup
- `logo` visual type: images ship inside the report as registered resources, so no public URL is needed, and they stay out of the tab order
- `altText` on every visual, enforced by the lint, for the Power BI accessibility checklist
- Spec-driven Power BI report chain: `ontologies/<Domain>/report.spec.json` drives both an HTML mockup (`deploy/New-ReportMockup.ps1`) and the deployed PBIR report (`deploy/Deploy-ReportFromSpec.ps1`), with a blocking validation gate against the TMDL model and one live DAX query per visual
- Report specs for Oil & Gas Refinery (Executive overview, Safety & maintenance, Storage & assets) and Enterprise Finance + HR (Spend vs plan, Workforce & pay, Talent pipeline)
- `Test-ReportLayout` non-blocking layout lint: 4px grid, margins, KPI row consistency, page density, and generic "X by Y" titles
- `Report Layout` and `Report Mockup` Copilot agents, bringing the agent set to 11
- Report visual types: `kpi` (with goal and trend), `funnel`, `waterfall`, `slicer`, `textbox`, `column`, `line`
- Enterprise Finance + HR browser planning app (`apps/enterprise-finance-hr-rayfin`) with aggregate-only views on synthetic data
- Solar Farm domain as a first-class deployable ontology (12 entities, 12 relationships, 26 CSVs, 6 KQL tables) with full deploy-script parity, registered in `Deploy-Ontology.ps1` and the Pester test suite
- Wind Turbine Rayfin twin hierarchy persistence via a new `TurbineDevice` backend entity, with runtime load and fallback to bundled device graph defaults
- Wind Turbine Rayfin Twin Graph Admin for in-app backend editing of twin device metadata (save/reset/add/delete) with live scene updates
- Wind Turbine Ask panel Data Agent readiness diagnostics with one-click connection self-test (mode/auth/transport result and actionable error details)
- Wind Turbine Mission Challenge mode with readiness scoring, objective checklist, and one-click runbook actions (prime story, quality check, dispatch, escalation, full drill)
- `assets/icons/solar.svg` domain icon
- Initial documentation synchronization from template project

### Changed
- Redesigned both report specs away from the generated-dashboard look: the full-width dark header band is replaced by a logo and filter strip, the fourth metric card by a written finding, and misused donut charts by sorted bars. Axis titles are off, and themes now come from the domain rather than from hand-picked colours.
- Documentation updated across README, SETUP_GUIDE, SEMANTIC_MODEL_GUIDE, AGENTS, and diagrams to reflect 8 industry domains, 11 Copilot agents, 4 Rayfin apps, and the spec-driven report chain, including full coverage of the Enterprise Finance + HR package (ontology, report, dataflows, pipeline, Cowork scenario) and its aggregate-only, synthetic-data constraints
- Corrected stale per-domain entity, CSV and row counts in README and SETUP_GUIDE
- Wind Turbine Data Agent runtime seam now supports configurable auth/header modes for public API-era integrations (`bearer`, `api-key`, or `none`) while preserving MCP/legacy fallback behavior
- Rewrote `tests/Accelerator.Tests.ps1` for Pester 5 compatibility (`-ForEach` data binding); suite is 519/519 green
- Aligned agent topology with standard multi-agent architecture
- Wind Turbine operations workflow now supports demo-safe dispatch and escalation in Viewer mode via internal Operator override, preventing blocked repair-order actions during storytelling demos
- Wind Turbine dispatch quality tooling now exposes explicit READY/MISSING status messaging with score and checklist feedback before assignment/escalation actions
- Wind Turbine operations copy and assignee entry were updated to reflect the new demo behavior (writeback remains protected while dispatch/escalation can auto-switch for guided demos)

---

### Security
- Stopped tracking `.env`, `.env.local`, `rayfin/.env`, `rayfin/.deployments.json` and `rayfin/.temp/` across the Rayfin apps (48 files). On a public repository these carried Fabric workspace, capacity and tenant identifiers plus Rayfin publishable keys. Added matching `.gitignore` rules and a `.env.example` template per app. The files remain on disk, so local development and `rayfin up` are unaffected.
  - **Action still required:** the previously committed values remain in git history. Rotate the exposed Rayfin publishable keys, and rewrite history if the identifiers must be purged.

---

_This changelog follows [Keep a Changelog](https://keepachangelog.com/) format._
