section Section1;
shared RequisitionSource = Lakehouse.Contents([HierarchicalNavigation = null]){[workspaceId = "{{WORKSPACE_ID}}"]}[Data]{[lakehouseId = "{{LAKEHOUSE_ID}}"]}[Data]{[Id = "factjobrequisition", ItemKind = "Table"]}[Data];
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Recruitment_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared Recruitment = Table.Group(RequisitionSource, {"DepartmentId", "RequisitionStatus"}, {{"Openings", each List.Sum([Openings]), type number}, {"Requisitions", each Table.RowCount(_), Int64.Type}});
shared Recruitment_DataDestination = let
	Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
	Workspace = Pattern{[workspaceId = "{{WORKSPACE_ID}}"]}[Data],
	Lakehouse = Workspace{[lakehouseId = "{{LAKEHOUSE_ID}}"]}[Data],
	Target = Lakehouse{[Id = "bi_recruitment_summary", ItemKind = "Table"]}?[Data]?
in
	Target;