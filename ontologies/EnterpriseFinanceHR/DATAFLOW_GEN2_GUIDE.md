# Enterprise Finance + HR Dataflow Gen2

`Deploy-BIDataflowGen2.ps1` creates a Dataflow Gen2 that turns the synthetic Delta source tables in `EnterpriseFinanceHRLH` into BI-oriented tables:

| Output table | Purpose | Data safety |
|---|---|---|
| `bi_cost_center_monthly` | Budget, actual, and variance by fiscal period and cost center | Finance aggregate |
| `bi_forecast_variance` | Base-scenario forecast against budget | Finance aggregate |
| `bi_workforce_monthly` | Headcount, FTE, open positions, and vacancy rate by department and period | Workforce aggregate only |

The dataflow does not write to the ontology source tables. It is intended to run after the `EnterpriseFinanceHR_LoadTables` notebook has loaded the CSV files to Delta.

## Expanded templates

The `datafactory/` folder contains four source-controlled templates: `compensation`, `attendance`, `absence`, and `recruitment`. Each writes an aggregate-only `bi_*` table and includes the required `{{WORKSPACE_ID}}`, `{{LAKEHOUSE_ID}}`, `{{CONNECTION_ID}}`, and `{{GATEWAY_CLUSTER_ID}}` placeholders. `Deploy-HRDataflowsGen2.ps1` replaces these values only at deployment, requires an existing Lakehouse connection and gateway, handles Fabric long-running operations, and refreshes with `ApplyChangesIfNeeded`.

## Required setup

1. Deploy `EnterpriseFinanceHR` so `EnterpriseFinanceHRLH` and `EnterpriseFinanceHR_LoadTables` exist.
2. In Fabric, create or identify a Lakehouse connection for `EnterpriseFinanceHRLH`.
3. Get the connection GUID and its Power BI gateway `ClusterId`. These values are deliberately required parameters: the script never stores a credential or guesses a source.

## Deploy

```powershell
./ontologies/EnterpriseFinanceHR/Deploy-BIDataflowGen2.ps1 `
  -WorkspaceId "<workspace-id>" `
  -LakehouseId "<enterprise-finance-hr-lakehouse-id>" `
  -LakehouseConnectionId "<lakehouse-connection-id>" `
  -GatewayClusterId "<gateway-cluster-id>"
```

Use `-SkipRefresh` to save the definition without triggering a refresh. The normal first refresh uses `ApplyChangesIfNeeded` so the Dataflow Gen2 applies the current definition and creates its three target tables.

## BI use

Use the `bi_*` tables for Power BI visuals, semantic-model partitions, and the Finance + Workforce Rayfin app. Keep `dimemployee` out of broad-consumption semantic models; workforce reporting should remain grouped by department, cost center, and period unless an approved HR access path is in place.