
# Refinery Worldwide Rayfin App

> **Geo Refinery Twin Command Center · Worldwide** — an ontology-grounded, 3D digital-twin
> command center for worldwide crude-oil refinery fleets, built on Fabric Rayfin
> (React 19 + Vite + Three.js + Vitest).

This app visualizes live refinery telemetry on an interactive **3D globe** — real 3D refinery
structures placed at their true lat/lon and colored by live status. It exposes per–process-unit
**digital twins** with an **Operations & Orders** workspace (writeback, responder dispatch, work
orders, escalation), a **Scenario Lab** that compares what-if plans against forecast-vs-realised
throughput with **Excel import/export** and a **GenAI** recommendation, and it answers
natural-language questions ("Ask Fabric IQ"). It ships fallback-safe: with no Fabric connection
configured it runs on a synthetic telemetry generator, and it lights up real data the moment the
connection aliases are set.

See [ROADMAP.md](ROADMAP.md) for shipped capabilities and the forward plan, and
[AGENTS.md](AGENTS.md) for build/agent guidance.


## Fabric connectivity

The app talks to Microsoft Fabric through three seams, all managed by the Rayfin CLI and
all fallback-safe:

| Seam | Files | Activation | Fallback when unset |
|------|-------|------------|---------------------|
| **Fabric host + client** | `src/lib/fabric-client.ts`, `src/fabric.generated.ts`, `fabric.yaml` | Provisioned by `rayfin` (workspace / item / tenant IDs land in `.env.local`); `getFabricClient()` proxies to the Fabric host via `postMessage` | Client no-ops locally |
| **Live telemetry** | `src/services/live-telemetry.service.ts` | Set `VITE_LIVE_TELEMETRY_MODEL` to the deployed **OilGasRefinery** semantic model — DAX (Direct Lake over the Lakehouse) pulls real refinery / process-unit readings | Synthetic `buildFarm()` generator |
| **Data Agent** | `src/services/data-agent.service.ts` | Set `VITE_DATA_AGENT_URL` — "Ask Fabric IQ" routes to the deployed Fabric Data Agent (source `fabriciq`) | Ontology-grounded engine (`ontology`), then pure offline engine (`local`) |

The header badge and answer footers label the active source honestly
(`Fabric Data Agent (live)` / `Ontology-grounded engine` / `Local engine (offline)`), and
flag telemetry as **simulated** until `VITE_LIVE_TELEMETRY_MODEL` is set.

> `fabric.yaml` and `src/fabric.generated.ts` are intentionally empty stubs until a real
> semantic-model connection is wired — this is by design, not a missing step. The Rayfin
> host IDs in `.env.local` are what connect the running app to its Fabric workspace.


## Prerequisites

1. **Node.js (v22)**: Download and install from https://nodejs.org/dist/v22.22.2/node-v22.22.2-x64.msi
2. **GitHub Copilot CLI**: Refer to https://github.com/github/copilot-cli
3. **Azure CLI**: Install from https://learn.microsoft.com/en-us/cli/azure/install-azure-cli?view=azure-cli-latest. After installation, run `az login` to sign in to your Azure account.


## Getting started

1. **Install dependencies**: `npm install`
2. **Provision / refresh the Fabric host** (optional for local dev): `rayfin env` writes the
   workspace / item / tenant IDs into `.env.local`.
3. **Run the app**: `npm run dev`, then open http://localhost:5173.
4. **Run the tests**: `npm test` (Vitest).
5. **Build for Fabric**: `npm run build:fabric`.
6. **Deploy**: `rayfin up`.
7. **Open the Fabric shell**: open the artifact in the Fabric portal and append
   `&devUri=http://localhost:5173` to preview local changes inside the host.


## Enabling real Fabric data

Add the connection aliases to `.env.local` (they are read via `import.meta.env`):

```dotenv
# Real process-unit telemetry from the deployed OilGasRefinery semantic model
VITE_LIVE_TELEMETRY_MODEL=<semantic-model-name-or-dataset-id>

# Real natural-language answers from the deployed Fabric Data Agent
VITE_DATA_AGENT_URL=<data-agent-endpoint>
```

With neither set, the app is fully functional on synthetic data — no Fabric round-trips.


## Related

- [IQ Ontology Accelerator](../../README.md) — parent repo (7 ontology domains + deployment engine)
- [Wind Turbine Rayfin App](../wind-turbine-rayfin/README.md) — Geo Wind Twin (WindTurbine model)
- [Solar France Rayfin App](../solar-france-rayfin/README.md) — Geo Solar Twin (SolarFarm model, France)
- [ROADMAP.md](ROADMAP.md) — shipped capabilities and forward plan


## Need help?

If you have any questions or run into any problems, please [file an issue](../../issues) on this repository.