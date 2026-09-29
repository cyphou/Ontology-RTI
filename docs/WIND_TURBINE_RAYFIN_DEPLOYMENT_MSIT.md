# Wind Turbine Rayfin Deployment Handoff

## Target workspace

1. Workspace URL: https://app.powerbi.com/groups/<workspace-id>/list?experience=power-bi
2. Workspace ID: <workspace-id>
3. Workspace name resolved by Rayfin: pde_windturbine

## Prepared project

1. Project folder: apps/wind-turbine-rayfin
2. Template used: dataapp
3. Local Rayfin CLI version: 1.33.1

## Deployment result

1. Fabric AppBackend item ID: <wind-appbackend-id>
2. Fabric deep link: https://app.fabric.microsoft.com/groups/<workspace-id>/appbackends/<wind-appbackend-id>?ctid=<tenant-id>
3. Static hosting URL: https://<wind-app-host>
4. Deployment metadata file: apps/wind-turbine-rayfin/rayfin/.deployments.json

## Commands used

1. Sign in:
   npx --yes @microsoft/rayfin-cli@latest login
2. Scaffold:
   npx --yes @microsoft/rayfin-cli@latest --yes init "apps/wind-turbine-rayfin" --template dataapp --project-name "wind-turbine-rayfin" --workspace-id "<workspace-id>"
3. Dry run deploy:
   npx rayfin up --workspace-id "<workspace-id>" --dry-run --yes
4. Actual deploy:
   npx rayfin up --workspace-id "<workspace-id>" --yes

## Validation performed

1. Rayfin auth status verified.
2. Dry-run deployment plan verified.
3. Real deployment completed successfully.
4. Scaffolded app tests passed:
   npm run test

## Notes

1. rayfin.yml in apps/wind-turbine-rayfin now includes the deployed hosting URL in allowedRedirectUris.
2. The current scaffold is the baseline data app template. Next implementation step is replacing src/App.tsx placeholder content with the wind turbine Three.js scene and telemetry bindings.
