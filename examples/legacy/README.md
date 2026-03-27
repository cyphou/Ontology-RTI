# Legacy Scripts

Scripts that have been superseded by the generic multi-domain deployment engine.

## Deploy-OilGasOntology.ps1

The original single-domain deployment script (~900 lines) for Oil & Gas Refinery.
Superseded by `Deploy-Ontology.ps1` → `deploy/Deploy-GenericOntology.ps1` which
handles all 6 domains with a single parameterized engine.

**Do not use for new deployments.** Use instead:

```powershell
.\Deploy-Ontology.ps1 -WorkspaceId "guid" -OntologyType OilGasRefinery
```
