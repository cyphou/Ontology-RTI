section Section1;
shared AbsenceSource = Lakehouse.Contents([HierarchicalNavigation = null]){[workspaceId = "{{WORKSPACE_ID}}"]}[Data]{[lakehouseId = "{{LAKEHOUSE_ID}}"]}[Data]{[Id = "factabsenceepisode", ItemKind = "Table"]}[Data];
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Absence_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared Absence = Table.Group(AbsenceSource, {"LeaveTypeId", "AbsenceReasonCategoryId"}, {{"AbsenceHours", each List.Sum([AbsenceHours]), type number}, {"AbsenceEpisodes", each Table.RowCount(_), Int64.Type}});
shared Absence_DataDestination = let
	Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
	Workspace = Pattern{[workspaceId = "{{WORKSPACE_ID}}"]}[Data],
	Lakehouse = Workspace{[lakehouseId = "{{LAKEHOUSE_ID}}"]}[Data],
	Target = Lakehouse{[Id = "bi_absence_summary", ItemKind = "Table"]}?[Data]?
in
	Target;