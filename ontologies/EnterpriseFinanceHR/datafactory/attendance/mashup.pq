section Section1;
shared AttendanceSource = Lakehouse.Contents([HierarchicalNavigation = null]){[workspaceId = "{{WORKSPACE_ID}}"]}[Data]{[lakehouseId = "{{LAKEHOUSE_ID}}"]}[Data]{[Id = "facttimeattendancedaily", ItemKind = "Table"]}[Data];
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Attendance_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared Attendance = Table.Group(AttendanceSource, {"CalendarDateId", "AttendanceCodeId"}, {{"ScheduledHours", each List.Sum([ScheduledHours]), type number}, {"WorkedHours", each List.Sum([WorkedHours]), type number}, {"ApprovedHours", each List.Sum([ApprovedHours]), type number}, {"OvertimeHours", each List.Sum([OvertimeHours]), type number}});
shared Attendance_DataDestination = let
	Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
	Workspace = Pattern{[workspaceId = "{{WORKSPACE_ID}}"]}[Data],
	Lakehouse = Workspace{[lakehouseId = "{{LAKEHOUSE_ID}}"]}[Data],
	Target = Lakehouse{[Id = "bi_attendance_daily", ItemKind = "Table"]}?[Data]?
in
	Target;