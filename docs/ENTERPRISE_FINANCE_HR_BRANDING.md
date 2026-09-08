# Enterprise Finance + HR branding

## Workspace mark

[Workspace logo SVG](../assets/icons/enterprise-finance-hr-workspace.svg)

Use this mark for the `pde-demo-hr` workspace cover image or documentation. It uses the Northstar Planning colors:

- Deep teal: `#063f48`
- Teal: `#087174`
- Gold: `#f5b65b`
- Coral: `#df653c`

## Pipeline visual mark

[Pipeline visual logo SVG](../assets/icons/enterprise-finance-hr-taskflow.svg)

Use this mark for the `Enterprise Finance HR Aggregate Refresh` Data Pipeline documentation and runbook. It is a documentation asset; it is not a custom Fabric item icon.

## Workspace organization

The idempotent organizer is [Organize-EnterpriseFinanceHRFolders.ps1](../deploy/Organize-EnterpriseFinanceHRFolders.ps1).

The verified workspace layout is:

| Folder | Contents |
| --- | --- |
| `01 Data` | Lakehouses, SQL endpoints, Eventhouse, KQL database, load notebook, staging warehouse/lakehouse |
| `02 Planning` | Ontology, graph model, graph queries, Plan, semantic model |
| `03 Analytics` | Dataflows, dashboard, app backend, SQL reporting objects |
| `04 Automation` | Data agents, pipeline, quality gate notebook |
| Workspace root | Only Fabric-managed `__fabric_plan_sys` SQLDatabase and SQLEndpoint |

Fabric does not currently expose a reliable public API in this project for assigning a custom image directly to a workspace or Data Pipeline item. The SVG assets are therefore generated and ready for portal upload or documentation use; the app itself uses the same Northstar visual identity.

## Power BI report and Cowork pipeline

[Deploy-Report.ps1](../ontologies/EnterpriseFinanceHR/Deploy-Report.ps1) generates the
`Enterprise Finance + HR Executive Report` (PBIR v2 format) with a custom brand theme,
six pages, and GenAI visuals (Decomposition Tree, Key Influencers). It lives in
`03 Analytics` alongside the semantic model. Use `-PbipOutDir <folder>` to produce a
Desktop-openable PBIP project instead of deploying to the service.

The M365 Cowork pipeline ([COWORK_SCENARIO.md](../ontologies/EnterpriseFinanceHR/COWORK_SCENARIO.md))
asks business questions against the model, forecasts spend, generates a branded
PowerPoint deck, and schedules a Teams review meeting.

