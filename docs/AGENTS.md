# Multi-Agent Architecture — Ontology to Accelerator Migration

This project uses a **specialized agent model**. Each agent has scoped domain knowledge,
file ownership, and clear boundaries.

> **Fabric Apps:** three domains also ship a browser digital-twin app under `apps/`
> (Fabric Rayfin — React 19 + Vite + Three.js): [wind-turbine-rayfin](../apps/wind-turbine-rayfin/README.md)
> (WindTurbine), [solar-france-rayfin](../apps/solar-france-rayfin/README.md) (SolarFarm, France),
> and [refinery-worldwide-rayfin](../apps/refinery-worldwide-rayfin/README.md) (OilGasRefinery). See the
> repo [Development Plan](../DEVELOPMENT_PLAN.md) for the app roadmap.

## Quick Reference

| Agent | Invoke When | Owns |
|-------|-------------|------|
| **@orchestrator** | Pipeline coordination, CLI, batch | `migrate.py`, orchestration modules |
| **@extractor** | Parsing Ontology source artifacts | `src/` extraction modules |
| **@converter** | Formula/expression conversion (coordination layer) | Delegates to @dax and @wiring |
| **@dax** | DAX/formula correctness, conversion, optimization | DAX conversion modules |
| **@wiring** | DAX↔M bridge, query generation, classification | M query modules |
| **@semantic** | Semantic model (TMDL), relationships, RLS | Semantic model generators |
| **@visual** | Report layout, visual containers, filters | Report/visual generators |
| **@generator** | Cross-cutting generation coordination | `deploy/` generators |
| **@assessor** | Migration readiness, scoring, strategy, validation | Assessment modules |
| **@merger** | Shared model, multi-source merge | Merge modules |
| **@deployer** | Deployment, auth, gateway, telemetry | Deployment modules |
| **@reviewer** | Artifact quality review, preceptorship loop | Quality review modules |
| **@tester** | Tests, coverage, fixtures, regression | `tests/` |
| **@fabric-apps** | Rayfin app features, cross-app parity, telemetry UX, scene performance | `apps/*-rayfin/src/**` |
| **@ui-wow** | Interface quality testing, visual polish, accessibility, and wow-effect UX upgrades | `apps/*-rayfin/src/**`, app UI docs |

## Architecture Diagram

```
                        ┌──────────────┐
                        │ Orchestrator │  ← CLI entry, pipeline coordination
                        └──────┬───────┘
                               │
              ┌────────────────┼────────────────┐
              │                │                │
        ┌─────▼─────┐   ┌─────▼─────┐   ┌─────▼─────┐
        │ Extractor  │   │ Converter │   │ Generator  │
        │(Ontology parse)│  │ (coord.)  │   │ (coord.)   │
        └──────┬─────┘   └─────┬─────┘   └─────┬──────┘
               │          ┌────┴────┐     ┌─────┴──────┐
               │          │         │     │            │
               │     ┌────▼───┐ ┌───▼───┐ ┌▼────────┐ ┌▼──────┐
               │     │  DAX   │ │Wiring │ │Semantic │ │Visual │
               │     │(formula)│ │(DAX↔M)│ │(model)  │ │(report)│
               │     └────────┘ └───────┘ └─────────┘ └───────┘
               │                              │
               │        ┌────────────────┬────┴────┐
               │        │                │         │
               │  ┌─────▼─────┐   ┌─────▼───┐  ┌──▼─────┐
               │  │  Assessor  │   │ Merger  │  │Deployer│
               │  │ (Analysis) │   │ (Merge) │  │(Deploy)│
               │  └────────────┘   └─────────┘  └────────┘
               │
              ┌┴───────────────────────────────────────────┐
              │                 Reviewer                    │
              │    (Preceptorship loop — reviews artifacts  │
              │     from Semantic + Visual + DAX + Wiring)  │
              └────────────────────────────────────────────┘

              ┌────────────────────────────────────────────┐
              │                  Tester                     │
              │    (Cross-cutting — reads all, writes       │
              │     only to tests/)    │
              └────────────────────────────────────────────┘
```

## The Preceptorship Loop

Every migration passes through a **quality gate** before artifacts are finalized:

```
DRAFT (Agent)  ──→  REVIEW (@reviewer)  ──→  APPROVE? (≥ 4★?)
     ↑                                           │
     │                  YES ─────────────────────→ DONE (artifacts ready)
     │                   NO ─────────────────────→ COACH (structured feedback)
     │                                                │
     └────────────────────────────────────────────────┘
                       (max 3 cycles, then escalate)
```

### Review Dimensions (5-star scoring)

| Dimension | What @reviewer Checks |
|-----------|----------------------|
| **Completeness** | All source objects have corresponding output |
| **Formula Correctness** | Valid syntax, correct conversion from Ontology formulas |
| **Query Validity** | Proper quoting, valid expressions |
| **Model Structure** | Valid relationships, proper cardinality |
| **Report Fidelity** | Visual types mapped correctly, filters at right level |

### Scoring Rules

- **≥ 4★ average** across all dimensions → **APPROVE**
- **< 4★ average** → **COACH** — @reviewer provides specific feedback per dimension
- **After 3 failed cycles** → **ESCALATE** to user
