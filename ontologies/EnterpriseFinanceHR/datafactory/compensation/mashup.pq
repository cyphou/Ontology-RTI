section Section1;
shared CompensationSource = Lakehouse.Contents([HierarchicalNavigation = null]){[workspaceId = "{{WORKSPACE_ID}}"]}[Data]{[lakehouseId = "{{LAKEHOUSE_ID}}"]}[Data]{[Id = "factemployeecompensationsnapshot", ItemKind = "Table"]}[Data];
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Compensation_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared Compensation = Table.Group(CompensationSource, {"FiscalPeriodId", "PayGradeId"}, {{"TotalCompensation", each List.Sum([AnnualizedAmount]), type number}, {"FTE", each List.Sum([Fte]), type number}});
shared Compensation_DataDestination = let
	Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
	Workspace = Pattern{[workspaceId = "{{WORKSPACE_ID}}"]}[Data],
	Lakehouse = Workspace{[lakehouseId = "{{LAKEHOUSE_ID}}"]}[Data],
	Target = Lakehouse{[Id = "bi_compensation_monthly", ItemKind = "Table"]}?[Data]?
in
	Target;