# Enterprise Finance + HR Semantic Model

This Direct Lake model is built entirely from synthetic sample data. It supports finance and workforce planning aggregates; it must not be used for automated employment, compensation, leave, or recruiting decisions.

Use the `bi_*` aggregate tables created by the Dataflow Gen2 templates for broad-consumption reporting. Keep `dimemployee`, `dimcandidate`, and the employee-level compensation, attendance, absence, recruitment-event, and offer fact tables restricted to approved HR reporting paths. Existing roles are design guidance only: production access requires Entra group assignments and row-level security. Object-level security is not fully expressed in this source TMDL, so implement and validate OLS in the deployed semantic model before granting broad access.

Measures on fact tables are aggregate calculations. Build report visuals at department, cost-center, fiscal-period, pay-grade, leave-category, recruiting-stage, or source level, with minimum group-size suppression where a group might expose an individual.