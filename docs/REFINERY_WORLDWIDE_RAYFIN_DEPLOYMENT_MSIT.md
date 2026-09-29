# Refinery Worldwide Rayfin Deployment Handoff

## Target workspace

1. Workspace URL: https://app.powerbi.com/groups/<workspace-id>/list?experience=power-bi
2. Workspace ID: <workspace-id>
3. Workspace name resolved by Rayfin: pde_windturbine (sanitized: pde-windturbine)

## Prepared project

1. Project folder: apps/refinery-worldwide-rayfin
2. Template used: dataapp (cloned and refactored from apps/solar-france-rayfin)
3. Rayfin project id: refinery-worldwide
4. Rayfin app name: Geo Refinery Twin Worldwide

## Deployment result

1. Fabric AppBackend item ID: <refinery-appbackend-id>
2. Fabric deep link: https://app.fabric.microsoft.com/groups/<workspace-id>/appbackends/<refinery-appbackend-id>?ctid=<tenant-id>
3. Static hosting URL: https://<refinery-app-host>
4. Deployment metadata file: apps/refinery-worldwide-rayfin/rayfin/.deployments.json

This item is distinct from the sibling geo-twin apps in the same workspace:

| App | Rayfin id | Fabric item ID | Hosting URL |
| --- | --- | --- | --- |
| Solar (France) | solar-france | <solar-appbackend-id> | <solar-app-host> |
| Wind turbine | wind-turbine-rayfin | <wind-appbackend-id> | <wind-app-host> |
| Refinery (worldwide) | refinery-worldwide | <refinery-appbackend-id> | <refinery-app-host> |

## Domain model

The app reuses the geospatial twin shell and rebinds it to an oil & gas refinery domain:

1. Six worldwide refinery sites (Jamnagar, Paraguaná, Ulsan, Ruwais, and more) projected onto the globe.
2. Process units replace solar arrays; `capacityMw` carries crude distillation capacity in thousand barrels.
3. Signal semantics: Throughput (kbd) and Feed rate (kbd) are informational context; Unit temp (°C, warn 380 / alarm 430) and Utilization (%, warn 90 / alarm 98) govern health.
4. Backend entity renamed `SolarPlant` → `RefineryUnit`; the ontology Q&A engine answers in refinery terms (throughput, feed rate, unit temp, utilization).

## Commands used

1. Install dependencies:
   npm install
2. Run tests:
   npm test -- --run
3. Fabric build:
   npm run build:fabric
4. Deploy:
   npx rayfin up --workspace-id "<workspace-id>" --yes

## Validation performed

1. Unit test suite passed: 50/50 (Vitest).
2. Fabric build succeeded (type generation + tsc + vite build).
3. Real deployment completed successfully as a new, distinct Fabric item.

## Notes

1. rayfin.yml in apps/refinery-worldwide-rayfin uses id `refinery-worldwide` and includes the deployed hosting URL in allowedRedirectUris.
2. Hard-refresh (Ctrl+F5) the hosting URL after deployment to bypass cached assets.
3. Internal carrier names (`powerKw`, `irradianceWm2`, `SolarPlantSite`) are retained as inert field names; all user-facing copy uses refinery semantics.
