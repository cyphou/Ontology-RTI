# Changelog

## [Unreleased]

### Added
- Solar Farm domain as a first-class deployable ontology (12 entities, 12 relationships, 26 CSVs, 6 KQL tables) with full deploy-script parity, registered in `Deploy-Ontology.ps1` and the Pester test suite
- `assets/icons/solar.svg` domain icon
- Initial documentation synchronization from template project

### Changed
- Documentation updated across README, SETUP_GUIDE, SEMANTIC_MODEL_GUIDE, AGENTS, and diagrams to reflect 7 industry domains
- Rewrote `tests/Accelerator.Tests.ps1` for Pester 5 compatibility (`-ForEach` data binding); suite is 519/519 green
- Aligned agent topology with standard multi-agent architecture

---

_This changelog follows [Keep a Changelog](https://keepachangelog.com/) format._
