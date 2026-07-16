# Changelog

## [Unreleased]

### Added
- Solar Farm domain as a first-class deployable ontology (12 entities, 12 relationships, 26 CSVs, 6 KQL tables) with full deploy-script parity, registered in `Deploy-Ontology.ps1` and the Pester test suite
- Wind Turbine Rayfin twin hierarchy persistence via a new `TurbineDevice` backend entity, with runtime load and fallback to bundled device graph defaults
- Wind Turbine Rayfin Twin Graph Admin for in-app backend editing of twin device metadata (save/reset/add/delete) with live scene updates
- Wind Turbine Ask panel Data Agent readiness diagnostics with one-click connection self-test (mode/auth/transport result and actionable error details)
- Wind Turbine Mission Challenge mode with readiness scoring, objective checklist, and one-click runbook actions (prime story, quality check, dispatch, escalation, full drill)
- `assets/icons/solar.svg` domain icon
- Initial documentation synchronization from template project

### Changed
- Documentation updated across README, SETUP_GUIDE, SEMANTIC_MODEL_GUIDE, AGENTS, and diagrams to reflect 7 industry domains
- Wind Turbine Data Agent runtime seam now supports configurable auth/header modes for public API-era integrations (`bearer`, `api-key`, or `none`) while preserving MCP/legacy fallback behavior
- Rewrote `tests/Accelerator.Tests.ps1` for Pester 5 compatibility (`-ForEach` data binding); suite is 519/519 green
- Aligned agent topology with standard multi-agent architecture
- Wind Turbine operations workflow now supports demo-safe dispatch and escalation in Viewer mode via internal Operator override, preventing blocked repair-order actions during storytelling demos
- Wind Turbine dispatch quality tooling now exposes explicit READY/MISSING status messaging with score and checklist feedback before assignment/escalation actions
- Wind Turbine operations copy and assignee entry were updated to reflect the new demo behavior (writeback remains protected while dispatch/escalation can auto-switch for guided demos)

---

_This changelog follows [Keep a Changelog](https://keepachangelog.com/) format._
