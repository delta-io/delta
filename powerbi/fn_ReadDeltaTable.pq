let
    fn_ReadDeltaTable = (
        DeltaTableFolderContent as table,
        optional DeltaTableOptions as record
    ) as table =>

    let

        DeltaTableVersion = if DeltaTableOptions = null then null else Record.FieldOrDefault(DeltaTableOptions, "Version", null),
        PartitionFilterFunction = if DeltaTableOptions = null then (x) => true else if Record.FieldOrDefault(DeltaTableOptions, "PartitionFilterFunction", null) = null then (x) => true else Record.Field(DeltaTableOptions, "PartitionFilterFunction"),
        StatsFilterFunction = if DeltaTableOptions = null then (x, y) => true else if Record.FieldOrDefault(DeltaTableOptions, "StatsFilterFunction", null) = null then (x, y) => true else Record.Field(DeltaTableOptions, "StatsFilterFunction"),
        UseFileBuffer = if DeltaTableOptions = null then false else if Record.FieldOrDefault(DeltaTableOptions, "UseFileBuffer", null) = null then false else Record.Field(DeltaTableOptions, "UseFileBuffer"),
        IterateFolderContent = if DeltaTableOptions = null then false else if Record.FieldOrDefault(DeltaTableOptions, "IterateFolderContent", null) = null then false else Record.Field(DeltaTableOptions, "IterateFolderContent"),
        TimeZoneOffset = if DeltaTableOptions = null then null else Record.FieldOrDefault(DeltaTableOptions, "TimeZoneOffset", null),
        TimeZoneOffsetDuration = Duration.FromText(Text.TrimStart(TimeZoneOffset, "+")),

        Delimiter = if Text.Contains(DeltaTableFolderContent{0}[Folder Path], "//") then "/" else "\",

        DeltaTableFolderContent_wFullPath = 
        let
            Source = DeltaTableFolderContent,

            fn_ReadContentRecursive = (tbl as table) as table => 
                let
                    subFolders = Table.SelectRows(tbl, each Value.Is(_[Content], type table)),
                    binaries = Table.SelectRows(tbl, each Value.Is(_[Content], type binary)),
                    combinedContent = if Table.RowCount(subFolders) > 0 then Table.Combine({binaries, @fn_ReadContentRecursive(Table.Combine(subFolders[Content]))}) else binaries
                in
                    combinedContent,

            Content = if IterateFolderContent then fn_ReadContentRecursive(Source) else Source,

            #"Added Full_Path" = Table.AddColumn(Content, "Full_Path", each Text.Replace([Folder Path] & [Name], "=", "%3D"), Text.Type),
            #"Added File_Name" = Table.AddColumn(#"Added Full_Path", "File_Name", each if Text.Length([Extension]) > 0 then List.Last(Text.Split([Full_Path], Delimiter)) else null, type text),
            Buffered = Table.Buffer(#"Added File_Name")
        in
            Buffered,

        PQ_DataTypes = 
        let
            Source = [
                Any.Type = Any.Type,
                None.Type = None.Type,
                Day.Type = Day.Type,
                Duration.Type = Duration.Type,
                Record.Type = Record.Type,
                Precision.Type = Precision.Type,
                Number.Type = Number.Type,
                Binary.Type = Binary.Type,
                Byte.Type = Byte.Type,
                Character.Type = Character.Type,
                Text.Type = Text.Type,
                Function.Type = Function.Type,
                Null.Type = Null.Type,
                List.Type = List.Type,
                Type.Type = Type.Type,
                Logical.Type = Logical.Type,
                Int8.Type = Int8.Type,
                Int16.Type = Int16.Type,
                Int32.Type = Int32.Type,
                Int64.Type = Int64.Type,
                Single.Type = Single.Type,
                Double.Type = Double.Type,
                Decimal.Type = Decimal.Type,
                Currency.Type = Currency.Type,
                Percentage.Type = Percentage.Type,
                Guid.Type = Guid.Type,
                Date.Type = Date.Type,
                DateTime.Type = DateTime.Type,
                DateTimeZone.Type = DateTimeZone.Type,
                Time.Type = Time.Type,
                Table.Type = Table.Type
            ]
        in
        Source,

        #"TableSchema" = 
        let
            ExpressionText = "type table [" & Text.Combine(metadata_columns[TableDataType], ", ") & "]",
            BufferedExpression = List.Buffer({ExpressionText}){0},
            TableSchema = Expression.Evaluate(BufferedExpression, PQ_DataTypes)
        in
            TableSchema,

        LogSchema = type [txn=record, add=record, remove=record, metaData=record, commitInfo=record, protocol=record],

        #"_delta_log Folder" = 
        let
            Source = DeltaTableFolderContent_wFullPath,
            #"Filtered Rows" = Table.SelectRows(Source, each Text.Contains([Full_Path], Delimiter & "_delta_log" & Delimiter)),
            #"Added Version" = Table.AddColumn(#"Filtered Rows", "Version", each try Int64.From(Text.BeforeDelimiter([File_Name], ".")) otherwise -1, Int64.Type),
            MaxVersion = Table.Group(#"Added Version", {}, {{"MaxVersion", each List.Max([Version]), type number}}){0}[MaxVersion],
            #"Filtered RequestedVersion" = if DeltaTableVersion = null then #"Added Version" 
        else if DeltaTableVersion < 0 then Table.SelectRows(#"Added Version", each [Version] <= MaxVersion + DeltaTableVersion)
        else Table.SelectRows(#"Added Version", each [Version] <= DeltaTableVersion),
            BufferedTable = Table.Buffer(#"Filtered RequestedVersion"),
            BufferedContent = Table.TransformColumns(BufferedTable,{{"Content", Binary.Buffer}})
        in
            BufferedContent,

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
            #"Parsed Logs" = Table.AddColumn(Source, "LogInfo", each Parquet.Document([Content])),
            #"Combine LogInfo and Version" = Table.Combine(Table.TransformRows(#"Parsed Logs", each fn_AddColumnsToTable([Version=_[Version]], _[LogInfo])))
        in
            #"Combine LogInfo and Version",

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

        fn_GetPowerBIDataTypeInformation = 
        (type_value as any, optional is_nullable as nullable logical) as text =>
        let 
            par_is_nullable = if is_nullable = null then true else is_nullable,

            ret = if Value.Is(type_value, Record.Type) then 
                    if type_value[type] = "struct" then "[" & Text.Combine(List.Transform(type_value[fields], each _[name] & " = " & @fn_GetPowerBIDataTypeInformation(_[type], _[nullable])), ", ") & "]"
                    else if type_value[type] = "array" then "{" & @fn_GetPowerBIDataTypeInformation(type_value[elementType], type_value[containsNull]) & "}"
                    else if type_value[type] = "map" then "table [Key=" & @fn_GetPowerBIDataTypeInformation(type_value[keyType], false) & ", Value=" & @fn_GetPowerBIDataTypeInformation(type_value[valueType], type_value[valueContainsNull]) & "]"
                    else "Any.Type"
                else if type_value = "string" then "Text.Type"
                else if type_value = "long" then "Int64.Type"
                else if type_value = "integer" then "Int32.Type"
                else if type_value = "short" then "Int16.Type"
                else if type_value = "byte" then "Int8.Type"
                else if type_value = "float" then "Single.Type"
                else if type_value = "double" then "Double.Type"
                else if type_value = "date" then "Date.Type"
                else if type_value = "timestamp" and TimeZoneOffset = null then "DateTime.Type"
                else if type_value = "timestamp" and TimeZoneOffset <> null then "DateTimeZone.Type"
                else if type_value = "boolean" then "Logical.Type"
                else if type_value = "binary" then "Binary.Type"
                else if type_value = "null" then "Any.Type"
                else if Text.StartsWith(type_value, "decimal") then "Number.Type"                
                else "Any.Type",

            ret_nullable = (if par_is_nullable then "nullable " else "") & ret
        in
            ret_nullable,

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
            #"Expanded fieldList" = Table.ExpandListColumn(#"Expanded schemaString", "fields"),
            #"Expanded fields" = Table.ExpandRecordColumn(#"Expanded fieldList", "fields", {"name", "type", "nullable", "metadata"}, {"name", "type", "nullable", "metadata"}),
            #"Changed Type" = Table.TransformColumnTypes(#"Expanded fields",{{"name", type text}, {"nullable", type logical}}),
            #"Added isPartitionedBy" = Table.Buffer(Table.AddColumn(#"Changed Type", "isPartitionedBy", each List.Contains([partitionColumns], [name]), Logical.Type)),
            #"Invoked fn_GetPowerBIDataType" = Table.AddColumn(#"Added isPartitionedBy", "PBI_Text", each fn_GetPowerBIDataTypeInformation([type], [nullable]), type text),
            #"Added PBI_DataType" = Table.AddColumn(#"Invoked fn_GetPowerBIDataType", "PBI_DataType", each Expression.Evaluate("type " & [PBI_Text], PQ_DataTypes), type type),
            #"Added PBI_Transformation" = Table.AddColumn(#"Added PBI_DataType", "PBI_Transformation", each 
                        if [type] = "string" then Text.From
                        else if [type] = "long" then Int64.From
                        else if [type] = "integer" then Int32.From
                        else if [type] = "short" then Int16.From
                        else if [type] = "byte" then Int8.From
                        else if [type] = "float" then Single.From
                        else if [type] = "double" then Double.From
                        else if [type] = "date" then Date.From
                        else if [type] = "timestamp" and TimeZoneOffset = null then DateTime.From
                        else if [type] = "timestamp" and TimeZoneOffset <> null then (x) as nullable datetimezone => DateTime.AddZone(x + TimeZoneOffsetDuration, Duration.Hours(TimeZoneOffsetDuration), Duration.Minutes(TimeZoneOffsetDuration))
                        else if [type] = "boolean" then Logical.From
                        else if [type] = "binary" then Binary.From
                        else if (Value.Is([type], type text) and Text.StartsWith([type], "decimal")) then Number.From
                        else (x) as nullable any => x, type function),
            #"Added ChangeDataType" = Table.AddColumn(#"Added PBI_Transformation", "ChangeDataType", each {[name], [PBI_DataType]}, type list),
            #"Added TableDataType" = Table.AddColumn(#"Added ChangeDataType", "TableDataType", each "#""" & [name] & """=" & Text.From([PBI_Text]), type text),
            #"Added ColumnTransformation" = Table.AddColumn(#"Added TableDataType", "ColumnTransformation", each {[name], [PBI_Transformation]}, type list),
            #"Buffered Fields" = Table.Buffer(#"Added ColumnTransformation")
        in
            #"Buffered Fields",

        fn_AddColumnsToTable = 
        (cols as record, tbl as table) as table =>
        let 
            colName = List.First(Record.FieldNames(cols)),
            cols_new = Record.RemoveFields(cols, colName),
            tbl_new = Table.AddColumn(tbl, colName, (x) => Record.Field(cols, colName), Value.Type(Record.Field(cols, colName))),

            ret = if Record.FieldCount(cols) = 0 then tbl else if Record.FieldCount(cols_new) = 0 then tbl_new else @fn_AddColumnsToTable(cols_new, tbl_new)
        in
            ret,

        #"Files with Stats" = 
        let
            Source = #"Logs ALL",
            #"Added Counter" = Table.AddColumn(Source, "Counter", each if [remove] <> null then -1 else if [add] <> null then 1 else null, Int8.Type),
            #"Added file_name" = Table.AddColumn(#"Added Counter", "file_name", each if [add] <> null then Record.Field([add], "path") else if [remove] <> null then Record.Field([remove], "path") else null, Text.Type),
            #"Filtered Rows" = Table.SelectRows(#"Added file_name", each ([file_name] <> null)),
            #"Added partitionValuesTable" = Table.AddColumn(#"Filtered Rows", "partitionValuesTable", each if [add] <> null then if Value.Is(Record.Field([add], "partitionValues"), Record.Type) then Record.ToTable(Record.Field([add], "partitionValues")) else Table.RenameColumns(Record.Field([add], "partitionValues"), {"Key", "Name"}) else null, type nullable table),
            #"Added partitionValuesJSON" = Table.AddColumn(#"Added partitionValuesTable", "partitionValuesJSON", each Text.FromBinary(Json.FromValue([partitionValuesTable]))),
            #"Added stats" = Table.AddColumn(#"Added partitionValuesJSON", "stats", each if [add] <> null then 
                if Value.Is(Record.Field([add], "stats"), type text) 
                then Record.Field([add], "stats") 
                else "{}"
                else null, type text),
            #"Grouped Rows1" = Table.Group(#"Added stats", {"file_name"}, {{"partitionValuesJSON", each List.Max([partitionValuesJSON]), type nullable text}, {"stats", each List.Max([stats]), type nullable text}, {"isRelevant", each List.Sum([Counter]), type nullable text}}),
            #"Relevant Files" = Table.SelectRows(#"Grouped Rows1", each ([isRelevant] > 0)),
            #"Added partitionValuesTable2" = Table.AddColumn(#"Relevant Files", "partitionValuesTable", each try Table.FromRecords(Json.Document([partitionValuesJSON])) otherwise null),
            #"Added partitionValuesRecord" = Table.AddColumn(#"Added partitionValuesTable2", "partitionValuesRecord", each Record.TransformFields(Record.FromTable([partitionValuesTable]), Table.SelectRows(#"metadata_columns", each [isPartitionedBy] = true)[ColumnTransformation]), Expression.Evaluate("type [" & Text.Combine(Table.SelectRows(#"metadata_columns", each [isPartitionedBy] = true)[TableDataType], ", ") & "]", PQ_DataTypes)),
            #"Expanded partitionValuesRecord" = Table.ExpandRecordColumn(#"Added partitionValuesRecord", "partitionValuesRecord", Table.SelectRows(#"metadata_columns", each [isPartitionedBy] = true)[name]),
            #"Parse stats to JSON" = Table.AddColumn(#"Expanded partitionValuesRecord", "JSON", each Json.Document([stats]), type [minValues=list, maxValues=list, numRecords=Int64.Type, nullCount=Int64.Type]),
            #"Expanded Stats" = Table.ExpandRecordColumn(#"Parse stats to JSON", "JSON", {"minValues", "maxValues", "numRecords", "nullCount"}, {"minValues", "maxValues", "numRecords", "nullCount"}),
            #"Removed Columns" = Table.RemoveColumns(#"Expanded Stats",{"partitionValuesJSON", "stats", "isRelevant", "partitionValuesTable"}),
            #"Apply PartitionFilterFunction" = Table.SelectRows(#"Removed Columns", each PartitionFilterFunction(_)),
            #"Apply StatsFilterFunction" = Table.SelectRows(#"Apply PartitionFilterFunction", each StatsFilterFunction([minValues], [maxValues]))
        in
            #"Apply StatsFilterFunction",

        #"Data" = 
        let
            #"Added Full_Path" = Table.AddColumn(#"Files with Stats", "Full_Path", each Text.Replace(DeltaTablePath & Text.Replace([file_name], "=", "%3D"), "/", Delimiter), Text.Type),
            #"Removed FilteringColumns" = Table.RemoveColumns(#"Added Full_Path",{"minValues", "maxValues", "numRecords", "nullCount"}),
            #"Buffered RelevantFiles" = Table.Buffer(#"Removed FilteringColumns"),
            #"Merged Queries" = Table.NestedJoin(#"Buffered RelevantFiles", {"Full_Path"}, DeltaTableFolderContent_wFullPath, {"Full_Path"}, "DeltaTable Folder", JoinKind.Inner),
            #"Removed Full_Path" = Table.RemoveColumns(#"Merged Queries",{"Full_Path"}),
            #"Expanded DeltaTable Folder" = Table.ExpandTableColumn(#"Removed Full_Path", "DeltaTable Folder", {"Content"}, {"Content"}),
            BufferFile = if UseFileBuffer then Table.TransformColumns(#"Expanded DeltaTable Folder",{{"Content", Binary.Buffer}}) else #"Expanded DeltaTable Folder",
            #"Read Parquet" = Table.AddColumn(BufferFile, "Data", each Parquet.Document([Content]), type table),
            #"Removed Binary Column" = Table.RemoveColumns(#"Read Parquet",{"Content"}),
            #"Combine Partition Values" = Table.CombineColumnsToRecord(#"Removed Binary Column", "cols", List.RemoveItems(Table.ColumnNames(#"Removed Binary Column"), {"Data"})),
            #"Combine Files" = Table.Combine(Table.TransformRows(#"Combine Partition Values", each fn_AddColumnsToTable(_[cols], _[Data])), TableSchema),
            #"Changed Type" = Table.TransformColumns(#"Combine Files",Table.SelectRows(metadata_columns, each [type] = "timestamp")[ColumnTransformation]),
            #"Table with TimeZoneOffset" = if TimeZoneOffset = null then #"Combine Files" else #"Changed Type",
            #"Reordered Columns" = Table.ReorderColumns(#"Table with TimeZoneOffset", metadata_columns[name]),

            FinalDeltaTable = Table.View(
                #"Reordered Columns", 
                [
                    GetType = () => TableSchema,
                    GetRowCount = () => List.Sum(#"Files with Stats"[numRecords])
                    //,OnSelectRows = (condition) => Table.FirstN(#"Reordered Columns", 3)
                ]
            )
        in
            FinalDeltaTable
    in 
        #"Data",

    documentation = [
        Documentation.Name =  "fn_ReadDeltaTable",
        Documentation.Description = "Takes the file/folder list of a Delta Lake table and returns the content as a table object in Power Query.",
        Documentation.LongDescription = "Takes the file/folder list of a Delta Lake table and returns the content as a table object in Power Query. An optional 2nd parameter can be used to for special features like Time Travel, Partition Elimination, etc.",
        Documentation.Category = "Table",
        Documentation.Source = "https://github.com/delta-io/connectors/blob/master/powerbi/fn_ReadDeltaTable.pq",
        Documentation.Version = "1.0",
        Documentation.Author = "Gerhard Brueckl, paiqo GmbH",
        Documentation.Examples = {[Description =  "  ",
            Code = "let
    Source = AzureStorage.Blobs(""https://gbadls01.blob.core.windows.net/public""),
    #""Filtered Rows"" = Table.SelectRows(Source, each Text.StartsWith([Name], ""powerbi_delta/FactInternetSales_part.delta/"")),
    DeltaTable = fn_ReadDeltaTable(#""Filtered Rows"", [Version=7])
in
    DeltaTable",
            Result = "#table( {""ProductKey"", ""OrderDateKey"", ""Value""}, { {""A"", ""2020-01-01"", 123} ,{""B"", ""2020-04-02"", 45} } )"]}]
  
in
    Value.ReplaceType(fn_ReadDeltaTable, Value.ReplaceMetadata(Value.Type(fn_ReadDeltaTable), documentation))