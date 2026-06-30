# Wind Turbine Rayfin Deployment Handoff

## Target workspace

1. Workspace URL: https://msit.powerbi.com/groups/c4e0ab47-88d2-452d-ac98-ad101b574cf3/list?experience=power-bi
2. Workspace ID: c4e0ab47-88d2-452d-ac98-ad101b574cf3
3. Workspace name resolved by Rayfin: pde_windturbine

## Prepared project

1. Project folder: apps/wind-turbine-rayfin
2. Template used: dataapp
3. Local Rayfin CLI version: 1.33.1

## Deployment result

1. Fabric AppBackend item ID: 118dd6c4-e00b-4b99-84f9-5571ec7a8b97
2. Fabric deep link: https://app.fabric.microsoft.com/groups/c4e0ab47-88d2-452d-ac98-ad101b574cf3/appbackends/118dd6c4-e00b-4b99-84f9-5571ec7a8b97?ctid=72f988bf-86f1-41af-91ab-2d7cd011db47
3. Static hosting URL: https://naive-cave-f911045ee7-westcentralus.webapp.msit.fabricapps.net
4. Deployment metadata file: apps/wind-turbine-rayfin/rayfin/.deployments.json

## Commands used

1. Sign in:
   npx --yes @microsoft/rayfin-cli@latest login
2. Scaffold:
   npx --yes @microsoft/rayfin-cli@latest --yes init "apps/wind-turbine-rayfin" --template dataapp --project-name "wind-turbine-rayfin" --workspace-id "c4e0ab47-88d2-452d-ac98-ad101b574cf3"
3. Dry run deploy:
   npx rayfin up --workspace-id "c4e0ab47-88d2-452d-ac98-ad101b574cf3" --dry-run --yes
4. Actual deploy:
   npx rayfin up --workspace-id "c4e0ab47-88d2-452d-ac98-ad101b574cf3" --yes

## Validation performed

1. Rayfin auth status verified.
2. Dry-run deployment plan verified.
3. Real deployment completed successfully.
4. Scaffolded app tests passed:
   npm run test

## Notes

1. rayfin.yml in apps/wind-turbine-rayfin now includes the deployed hosting URL in allowedRedirectUris.
2. The current scaffold is the baseline data app template. Next implementation step is replacing src/App.tsx placeholder content with the wind turbine Three.js scene and telemetry bindings.
