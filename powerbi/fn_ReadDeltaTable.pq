(
    DeltaTableFolderContent as table,
    optional DeltaTableOptions as record
) as table =>

let

    DeltaTableVersion = if DeltaTableOptions = null then null else Record.FieldOrDefault(DeltaTableOptions, "Version", null),
    PartitionFilterFunction = if DeltaTableOptions = null then (x) => true else Record.FieldOrDefault(DeltaTableOptions, "PartitionFilterFunction", (x) => true),

    DeltaTableFolderContent_wFullPath = 
    let
        Source = DeltaTableFolderContent,
        #"Added Full_Path" = Table.AddColumn(DeltaTableFolderContent, "Full_Path", each Text.Replace([Folder Path] & [Name], "=", "%3D"), Text.Type),
        #"Added File_Name" = Table.AddColumn(#"Added Full_Path", "File_Name", each if Text.Length([Extension]) > 0 then List.Last(Text.Split([Full_Path], Delimiter)) else null, type text),
        Buffered = Table.Buffer(#"Added File_Name")
    in
        Buffered,
        
    #"Delimiter" = 
    let
        Delimiter = if Text.Contains(DeltaTableFolderContent{0}[Folder Path], "//") then "/" else "\"
    in
        Delimiter,

    #"TableSchema" = 
    let
        ExpressionText = "type table [" & Text.Combine(metadata_columns[TableDataType], ", ") & "]",
        BufferedExpression = List.Buffer({ExpressionText}){0},
        TableSchema = Expression.Evaluate(BufferedExpression, #shared)
    in
        TableSchema,

    #"_delta_log Folder" = 
    let
        Source = DeltaTableFolderContent_wFullPath,
        #"Filtered Rows" = Table.SelectRows(Source, each Text.Contains([Full_Path], Delimiter & "_delta_log" & Delimiter)),
        #"Added Version" = Table.AddColumn(#"Filtered Rows", "Version", each try Int64.From(Text.BeforeDelimiter([File_Name], ".")) otherwise -1, Int64.Type),
        #"Filtered RequestedVersion" = if DeltaTableVersion = null then #"Added Version" else Table.SelectRows(#"Added Version", each [Version] <= DeltaTableVersion),
        buffered = Table.Buffer(#"Filtered RequestedVersion")
    in
        buffered,

    #"DeltaTablePath" = 
    let
        DeltaTablePath = Text.Combine(List.RemoveLastN(Text.Split(#"_delta_log Folder"{0}[Full_Path], Delimiter), 2), Delimiter) & Delimiter
    in
        DeltaTablePath,

    #"_last_checkpoint" = 
    let
        #"_delta_log" = #"_delta_log Folder",
        #"Filtered Rows" = Table.SelectRows(_delta_log, each Text.EndsWith([Name], "_last_checkpoint")),
        #"Added Custom" = Table.AddColumn(#"Filtered Rows", "JsonContent", each Json.Document([Content])),
        JsonContent = #"Added Custom"{0}[JsonContent],
        CheckEmpty = if Table.RowCount(#"Filtered Rows") = 0 then [Size=-1, version=-1] else JsonContent,
        LatestCheckPointWithParts = if Record.HasFields(CheckEmpty, "parts") then CheckEmpty else Record.AddField(CheckEmpty, "parts", 1),

        #"Filtered Rows Version" = Table.SelectRows(#"_delta_log", each Text.EndsWith([Name], ".checkpoint.parquet")),
        MaxVersion = try Table.Group(#"Filtered Rows Version", {}, {{"MaxVersion", each List.Max([Version]), type number}}){0}[MaxVersion] otherwise -1,
        #"Filtered Rows MaxVersion" = Table.SelectRows(#"Filtered Rows Version", each [Version] = MaxVersion),
        CheckpointFromVersion = [version=try MaxVersion otherwise -1, size=-1, parts = Table.RowCount(#"Filtered Rows MaxVersion")],

        LastCheckpoint = Table.Buffer(Table.FromRecords({if DeltaTableVersion = null then LatestCheckPointWithParts else CheckpointFromVersion})){0}
    in
        LastCheckpoint,

    #"Checkpoint Files" = 
    let
        LastCheckpointFile = {1..Record.Field(_last_checkpoint, "parts")},
        #"Converted to Table" = Table.FromList(LastCheckpointFile, Splitter.SplitByNothing(), {"part"}, null, ExtraValues.Error),
        #"Add Version" = Table.AddColumn(#"Converted to Table", "version", each Record.Field(_last_checkpoint, "version")),
        #"Add SingleFile" = Table.AddColumn(#"Add Version", "file_name", each Text.PadStart(Text.From([version]), 20, "0") & ".checkpoint.parquet", Text.Type),
        #"Add MultipleFiles" = Table.AddColumn(#"Add Version", "file_name", each Text.PadStart(Text.From([version]), 20, "0") & ".checkpoint." & Text.PadStart(Text.From([part]), 10, "0") & "." & Text.PadStart(Text.From(Record.Field(_last_checkpoint, "parts")), 10, "0") & ".parquet", Text.Type),
        AllFiles = Table.SelectColumns(if Record.Field(_last_checkpoint, "parts") = 1 then #"Add SingleFile" else #"Add MultipleFiles", "file_name"),
        AllFiles_BufferedList = List.Buffer(Table.ToList(AllFiles)),
        Content = Table.SelectRows(#"_delta_log Folder", each List.Count(List.Select(AllFiles_BufferedList, (inner) => Text.EndsWith([Name], inner))) > 0)
    in
        Content,

    #"Logs Checkpoint" = 
    let
        Source = #"Checkpoint Files",
        #"Parsed Logs" = Table.AddColumn(Source, "Custom", each Parquet.Document([Content])),
        #"Expanded Logs" = Table.ExpandTableColumn(#"Parsed Logs", "Custom", {"add", "remove", "metaData", "commitInfo", "protocol"}, {"add", "remove", "metaData", "commitInfo", "protocol"}),
        #"Removed Other Columns" = Table.SelectColumns(#"Expanded Logs",{"Version", "add", "remove", "metaData", "commitInfo", "protocol"})
    in
        #"Removed Other Columns",

    #"Latest Log Files" = 
    let
        Source = #"_delta_log Folder",
        #"Filtered Rows" = Table.SelectRows(Source, each ([Extension] = ".json")),
        #"Filtered Rows1" = Table.SelectRows(#"Filtered Rows", each [Version] > Record.Field(_last_checkpoint, "version"))
    in
        #"Filtered Rows1",

    #"Logs JSON" = 
    let
        Source = #"Latest Log Files",
        #"Added Custom" = Table.AddColumn(Source, "JsonContent", each Lines.FromBinary([Content])),
        #"Expanded JsonContent" = Table.ExpandListColumn(#"Added Custom", "JsonContent"),
        #"Parsed Logs" = Table.TransformColumns(#"Expanded JsonContent",{{"JsonContent", Json.Document}}),
        #"Expanded Logs" = Table.ExpandRecordColumn(#"Parsed Logs", "JsonContent", {"add", "remove", "metaData", "commitInfo", "protocol"}),
        #"Removed Other Columns" = Table.SelectColumns(#"Expanded Logs",{"Version", "add", "remove", "metaData", "commitInfo", "protocol"})
    in
        #"Removed Other Columns",

    #"Logs ALL" = 
    let
        Source = Table.Combine({#"Logs Checkpoint", #"Logs JSON"}),
        #"Added timestamp" = Table.AddColumn(Source, "log_timestamp", each if [add] <> null then Record.Field([add], "modificationTime") else 
    if [remove] <> null then Record.Field([remove], "deletionTimestamp") else 
    if [commitInfo] <> null then Record.Field([commitInfo], "timestamp") else 
    if [metaData] <> null then Record.Field([metaData], "createdTime") else null, Int64.Type),
        #"Added datetime" = Table.AddColumn(#"Added timestamp", "log_datetime", each try #datetime(1970,1,1,0,0,0)+#duration(0,0,0,[log_timestamp]/1000) otherwise null, DateTime.Type)
    in
        #"Added datetime",

    #"metadata_columns" = 
    let
        Source = #"Logs ALL",
        #"Filtered Rows1" = Table.SelectRows(Source, each ([metaData] <> null)),
        MaxVersion = Table.Group(#"Filtered Rows1", {}, {{"MaxVersion", each List.Max([Version]), type number}}){0}[MaxVersion],
        #"Filtered Rows2" = Table.SelectRows(#"Filtered Rows1", each [Version] = MaxVersion),
        #"Kept First Rows" = Table.FirstN(#"Filtered Rows2",1),
        #"Removed Other Columns" = Table.SelectColumns(#"Kept First Rows",{"metaData"}),
        #"Expanded metaData" = Table.ExpandRecordColumn(#"Removed Other Columns", "metaData", {"schemaString", "partitionColumns"}, {"schemaString", "partitionColumns"}),
        #"Filtered Rows" = Table.SelectRows(#"Expanded metaData", each ([schemaString] <> null)),
        JSON = Table.TransformColumns(#"Filtered Rows",{{"schemaString", Json.Document}}),
        #"Expanded schemaString" = Table.ExpandRecordColumn(JSON, "schemaString", {"fields"}, {"fields"}),
        #"Expanded fields" = Table.ExpandListColumn(#"Expanded schemaString", "fields"),
        #"Expanded fields1" = Table.ExpandRecordColumn(#"Expanded fields", "fields", {"name", "type", "nullable", "metadata"}, {"name", "type", "nullable", "metadata"}),
        #"Added Custom" = Table.Buffer(Table.AddColumn(#"Expanded fields1", "isPartitionedBy", each List.Contains([partitionColumns], [name]), Logical.Type)),
        #"Added Custom1" = Table.AddColumn(#"Added Custom", "PBI_DataType", 
    each if [type] = "long" then [PBI_DataType=Int64.Type, PBI_Text="Int64.Type", PBI_Transformation=Int64.From]
    else if [type] = "integer" then [PBI_DataType=Int32.Type, PBI_Text="Int32.Type", PBI_Transformation=Int32.From]
    else if [type] = "short" then [PBI_DataType=Int16.Type, PBI_Text="Int16.Type", PBI_Transformation=Int16.From]
    else if [type] = "byte" then [PBI_DataType=Int8.Type, PBI_Text="Int8.Type", PBI_Transformation=Int8.From]
    else if [type] = "float" then [PBI_DataType=Single.Type, PBI_Text="Single.Type", PBI_Transformation=Single.From]
    else if [type] = "double" then [PBI_DataType=Double.Type, PBI_Text="Double.Type", PBI_Transformation=Double.From]
    else if [type] = "string" then [PBI_DataType=Text.Type, PBI_Text="Text.Type", PBI_Transformation=Text.From]
    else if [type] = "timestamp" then [PBI_DataType=DateTime.Type, PBI_Text="DateTime.Type", PBI_Transformation=DateTime.From]
    else if [type] = "boolean" then [PBI_DataType=Logical.Type, PBI_Text="Logical.Type", PBI_Transformation=Logical.From]
    else [PBI_DataType=Text.Type, PBI_Text="Text.Type", PBI_Transformation=Text.From]),
        #"Expanded PBI_DataType" = Table.ExpandRecordColumn(#"Added Custom1", "PBI_DataType", {"PBI_DataType", "PBI_Text", "PBI_Transformation"}, {"PBI_DataType", "PBI_Text", "PBI_Transformation"}),
        #"Added Custom2" = Table.AddColumn(#"Expanded PBI_DataType", "ChangeDataType", each {[name], [PBI_DataType]}, type list),
        #"Added Custom3" = Table.AddColumn(#"Added Custom2", "TableDataType", each [name] & "=" & (if [nullable] then "nullable " else "") & Text.From([PBI_Text]), type text),
        #"Added Custom4" = Table.AddColumn(#"Added Custom3", "ColumnTransformation", each {[name], [PBI_Transformation]}, type list),
        #"Buffered Fields" = Table.Buffer(#"Added Custom4")
    in
        #"Buffered Fields",

    #"Data" = 
    let
        Source = #"Logs ALL",
        #"Added Counter" = Table.AddColumn(Source, "Counter", each if [remove] <> null then -1 else if [add] <> null then 1 else null, Int8.Type),
        #"Added file_name" = Table.AddColumn(#"Added Counter", "file_name", each if [add] <> null then Record.Field([add], "path") else if [remove] <> null then Record.Field([remove], "path") else null, Text.Type),
        #"Filtered Rows" = Table.SelectRows(#"Added file_name", each ([file_name] <> null)),
        #"Added partitionValuesTable" = Table.AddColumn(#"Filtered Rows", "partitionValuesTable", each if [add] <> null then if Value.Is(Record.Field([add], "partitionValues"), Record.Type) then Record.ToTable(Record.Field([add], "partitionValues")) else Table.RenameColumns(Record.Field([add], "partitionValues"), {"Key", "Name"}) else null, type nullable table),
        #"Added partitionValuesJSON" = Table.AddColumn(#"Added partitionValuesTable", "partitionValuesJSON", each Text.FromBinary(Json.FromValue([partitionValuesTable]))),
        #"Grouped Rows1" = Table.Group(#"Added partitionValuesJSON", {"file_name"}, {{"partitionValuesJSON", each List.Max([partitionValuesJSON]), type nullable text}, {"isRelevant", each List.Sum([Counter]), type nullable text}}),
        #"Relevant Files" = Table.SelectRows(#"Grouped Rows1", each ([isRelevant] > 0)),
        #"Added partitionValuesTable2" = Table.AddColumn(#"Relevant Files", "partitionValuesTable", each try Table.FromRecords(Json.Document([partitionValuesJSON])) otherwise null),
        #"Added partitionValuesRecord" = Table.AddColumn(#"Added partitionValuesTable2", "partitionValuesRecord", each Record.TransformFields(Record.FromTable([partitionValuesTable]), Table.SelectRows(#"metadata_columns", each [isPartitionedBy] = true)[ColumnTransformation]), Expression.Evaluate("type [" & Text.Combine(Table.SelectRows(#"metadata_columns", each [isPartitionedBy] = true)[TableDataType], ", ") & "]", #shared)),
        #"Filtered Rows1" = Table.SelectRows(#"Added partitionValuesRecord", each PartitionFilterFunction([partitionValuesRecord])),
        #"Expanded partitionValuesRecord" = Table.ExpandRecordColumn(#"Filtered Rows1", "partitionValuesRecord", Table.SelectRows(#"metadata_columns", each [isPartitionedBy] = true)[name]),
        #"Added Full_Path" = Table.AddColumn(#"Expanded partitionValuesRecord", "Full_Path", each Text.Replace(DeltaTablePath & Text.Replace([file_name], "=", "%3D"), "/", Delimiter), Text.Type),
        #"Removed Columns3" = Table.RemoveColumns(#"Added Full_Path",{"file_name", "partitionValuesJSON", "isRelevant", "partitionValuesTable"}),
        #"Buffered RelevantFiles" = Table.Buffer(#"Removed Columns3"),
        #"Merged Queries" = Table.NestedJoin(#"Buffered RelevantFiles", {"Full_Path"}, DeltaTableFolderContent_wFullPath, {"Full_Path"}, "DeltaTable Folder", JoinKind.Inner),
        #"Removed Columns" = Table.RemoveColumns(#"Merged Queries",{"Full_Path"}),
        #"Expanded DeltaTable Folder" = Table.ExpandTableColumn(#"Removed Columns", "DeltaTable Folder", {"Content"}, {"Content"}),
        #"Added Custom1" = Table.AddColumn(#"Expanded DeltaTable Folder", "Data", each Parquet.Document([Content]), Expression.Evaluate("type table [" & Text.Combine(metadata_columns[TableDataType], ", ") & "]", #shared)),
        #"Removed Columns1" = Table.RemoveColumns(#"Added Custom1",{"Content"}),
        #"Expanded Data" = Table.ExpandTableColumn(#"Removed Columns1", "Data", Table.SelectRows(metadata_columns, each not [isPartitionedBy])[name]),
        #"Reordered Columns" = Table.ReorderColumns(#"Expanded Data", metadata_columns[name])
    in
        #"Reordered Columns"

in #"Data"