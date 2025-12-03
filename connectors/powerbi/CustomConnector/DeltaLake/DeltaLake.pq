section DeltaLake;

[DataSource.Kind="DeltaLake", Publish="DeltaLake.Publish"]
shared DeltaLake.Contents = Value.ReplaceType(DeltaLakeImpl, DeltaLakeType);

shared DeltaLakeType = type function (
    Url as (Uri.Type meta [
        DataSource.Path = true,
        Documentation.FieldCaption = "Url",
        Documentation.FieldDescription = "The fully qualified HTTP storage path.",
        Documentation.SampleValues = {"https://accountname.blob.core.windows.net/public"}
    ] as any) ,
    optional options as (type nullable [
        optional Version = (type nullable number meta [
            DataSource.Path = false,
            Documentation.FieldCaption = "Version",
            Documentation.FieldDescription = "A numeric value that defines historic specific version of the Delta Lake table you want to read. This is similar to specifying VERSION AS OF when querying the Delta Lake table via SQL. Default is the most recent/current version.",
            Documentation.SampleValues = {123}
        ]),
        optional UseFileBuffer = (type nullable logical meta [
        DataSource.Path = false,
        Documentation.FieldCaption = "Use File Buffer",
        Documentation.FieldDescription = "Not all data sources support the more performant streaming of binary files and you may receive an error message like ""Parquet.Document cannot be used with streamed binary values."" To mitigate this issue, you can set this option to ""True""",
        Documentation.AllowedValues = { 
            true meta [
                DataSource.Path = false,
                Documentation.Name = "True",
                Documentation.Caption = "True"
            ], 
            false meta [
                DataSource.Path = false,
                Documentation.Name = "False",
                Documentation.Caption = "False"
            ]
         }               
        ])
    ] meta [    
        DataSource.Path = false,
        Documentation.FieldCaption = "Advanced Options"
    ])
    )
    as table meta [
        Documentation.Name = "Delta Lake",
        Documentation.LongDescription = "Delta Lake Connector"        
    ];

 
//////////////////////////////////////
///////// Trigger NAV Table //////////
////////////////////////////////////// 

DeltaLakeImpl = (Url as text, optional options as record) as table =>
    let
        Folder = Url,
        Version = Record.FieldOrDefault(options, "Version", null),
        UseFileBuffer = if Record.Field(options, "UseFileBuffer") is null then false else Record.Field(options, "UseFileBuffer"),
        Table = DeltaLakeNavTable(Folder, Version, UseFileBuffer)
    in
        Table;

//////////////////////////////////////
/////// Get Content from Blob ////////
//////////////////////////////////////

DeltaLakeContentBlob = (Folder as text) as table =>
    let
        container = Text.BeforeDelimiter(Folder, "/", 3),
        prefix = Text.AfterDelimiter(Folder, "/", 3),
        url = container & "?restype=container&comp=list&prefix=" & prefix, 
        headers = if Extension.CurrentCredential()[AuthenticationKind] = "Implicit" then [] else [Headers=SignRequest(url,prefix)],
        response =
            let
                waitForResult = Value.WaitFor(
                    (iteration) =>
                        let
                            result = Web.Contents(url,headers), 
                            buffered = Binary.Buffer(result),
                            status = Value.Metadata(result)[Response.Status],
                            actualResult = if buffered <> null and status = 200 then buffered else null
                        in
                            actualResult,
                    (iteration) => #duration(0, 0, 0, 0),
                    5)
            in
                waitForResult,  
        Source = Xml.Tables(response),
        //Source = Xml.Tables(Web.Contents(url,headers)),        
        Blobs = Source{0}[Blobs],
        Blob = Blobs{0}[Blob],
        AddurlContent = Table.AddColumn(Blob, "urlContent", each container & "/" & [Name]),
        AddContent = Table.AddColumn(AddurlContent, "Content", each Web.Contents([urlContent],[Headers=SignRequest([urlContent])])),
        AddDeltaTable = Table.AddColumn(AddContent, "DeltaTable", each Text.BeforeDelimiter([Name], ".delta", {0, RelativePosition.FromEnd}), type text),
        AddDeltaTableDatabase = Table.AddColumn(AddDeltaTable, "DeltaTableDatabase", each Text.BetweenDelimiters([DeltaTable], "/", "/", {0, RelativePosition.FromEnd}, {0, RelativePosition.FromEnd})),
        AddDeltaTableFolder = Table.AddColumn(AddDeltaTableDatabase, "DeltaTableFolder", each Text.BeforeDelimiter([DeltaTable], "/", {1, RelativePosition.FromEnd}), type text),
        AddDeltaTableFolderDepth = Table.AddColumn(AddDeltaTableFolder, "DeltaTableFolderDepth", each List.Count(List.Select(Text.Split([DeltaTableFolder], "/"), each _ <> "" ) )),
        AddDeltaTableFolderList = Table.AddColumn(AddDeltaTableFolderDepth, "DeltaTableFolderList", each List.Select(Text.Split([DeltaTableFolder], "/"), each _ <> "" )),
        AddDeltaTablePath = Table.AddColumn(AddDeltaTableFolderList, "DeltaTablePath", each Text.BeforeDelimiter([DeltaTable], "/", {0, RelativePosition.FromEnd})),
        AddDeltaTableReplaceValue = Table.ReplaceValue(AddDeltaTablePath,"/",".",Replacer.ReplaceText,{"DeltaTable"}),
        AddFolderPath = Table.AddColumn(AddDeltaTableReplaceValue, "Folder Path", each Replacer.ReplaceText(container & "/" & [Name],"/" & [Name],"")),
        AddFolderPathEnd = Table.AddColumn(AddFolderPath, "Folder Path End", each Text.AfterDelimiter([Name], "/", {0, RelativePosition.FromEnd}), type text),
        AddExtension = Table.AddColumn(AddFolderPathEnd, "Extension", each if Text.Contains([Folder Path End],".") then "." & Text.AfterDelimiter([Folder Path End], ".", {0, RelativePosition.FromEnd}) else "", type text)
    in
        AddExtension;

//////////////////////////////////////
///// Get Content from ADLSGen2 //////
//////////////////////////////////////

DeltaLakeContentADLSGen2 = (Folder as text) as table =>
    let
        filesystem = Text.BeforeDelimiter(Folder, "/", 3),
        directoryCheck = Text.AfterDelimiter(Folder, "/", 3),
        directory = if directoryCheck = "" then "" else Text.AfterDelimiter(Folder, "/", {0, RelativePosition.FromEnd}),
        url = filesystem & "?recursive=true&resource=filesystem&directory=" & directory,
        headers = if Extension.CurrentCredential()[AuthenticationKind] = "Implicit" then [] else [Headers=SignRequest(url,directory)],
        response =
            let
                waitForResult = Value.WaitFor(
                    (iteration) =>
                        let
                            result = Web.Contents(url,headers), 
                            buffered = Binary.Buffer(result),
                            status = Value.Metadata(result)[Response.Status],
                            actualResult = if buffered <> null and status = 200 then buffered else null
                        in
                            actualResult,
                    (iteration) => #duration(0, 0, 0, 0),
                    5)
            in
                waitForResult,  
        Source = Json.Document(response),        
        //Source = Json.Document(Web.Contents(url,headers)),
        ConvertedToTable = Table.FromRecords({Source}),
        ExpandedPaths = Table.ExpandListColumn(ConvertedToTable, "paths"),
        ExpandedPathsColumns = Table.ExpandRecordColumn(ExpandedPaths, "paths", {"contentLength", "etag", "group", "isDirectory", "lastModified", "name", "owner", "permissions"}, {"contentLength", "etag", "group", "isDirectory", "lastModified", "name", "owner", "permissions"}),
        ChangedType = Table.TransformColumnTypes(ExpandedPathsColumns,{{"contentLength", Int64.Type}, {"etag", type text}, {"group", type text}, {"isDirectory", type logical}, {"lastModified", type datetime}, {"name", type text}, {"owner", type text}, {"permissions", type text}}),
        Rename = Table.RenameColumns(ChangedType,{"name","Name"}), 
        AddurlContent = Table.AddColumn(Rename, "urlContent", each filesystem & "/" & [Name]),
        AddContent = Table.AddColumn(AddurlContent, "Content", each Web.Contents([urlContent],[Headers=SignRequest([urlContent])])),   
        AddDeltaTable = Table.AddColumn(AddContent, "DeltaTable", each Text.BeforeDelimiter([Name], ".delta/", {0, RelativePosition.FromEnd}), type text),
        AddDeltaTableDatabase = Table.AddColumn(AddDeltaTable, "DeltaTableDatabase", each Text.BetweenDelimiters([DeltaTable], "/", "/", {0, RelativePosition.FromEnd}, {0, RelativePosition.FromEnd})),                                                                                                  
        AddDeltaTableFolder = Table.AddColumn(AddDeltaTableDatabase, "DeltaTableFolder", each Text.BeforeDelimiter([DeltaTable], "/", {1, RelativePosition.FromEnd}), type text),
        AddDeltaTableFolderDepth = Table.AddColumn(AddDeltaTableFolder, "DeltaTableFolderDepth", each List.Count(List.Select(Text.Split([DeltaTableFolder], "/"), each _ <> "" ) )),
        AddDeltaTableFolderList = Table.AddColumn(AddDeltaTableFolderDepth, "DeltaTableFolderList", each List.Select(Text.Split([DeltaTableFolder], "/"), each _ <> "" )),
        AddDeltaTablePath = Table.AddColumn(AddDeltaTableFolderList, "DeltaTablePath", each Text.BeforeDelimiter([DeltaTable], "/", {0, RelativePosition.FromEnd})),
        AddDeltaTableReplaceValue = Table.ReplaceValue(AddDeltaTablePath,"/",".",Replacer.ReplaceText,{"DeltaTable"}),
        AddFolderPath = Table.AddColumn(AddDeltaTableReplaceValue, "Folder Path", each Replacer.ReplaceText(Folder & "/" & [Name],"/" & [Name],"")),
        AddFolderPathEnd = Table.AddColumn(AddFolderPath, "Folder Path End", each Text.AfterDelimiter([Name], "/", {0, RelativePosition.FromEnd}), type text),
        AddExtension = Table.AddColumn(AddFolderPathEnd, "Extension", each if Text.Contains([Folder Path End],".") then "." & Text.AfterDelimiter([Folder Path End], ".", {0, RelativePosition.FromEnd}) else "", type text)
    in
        AddExtension;

///////////////////////////////
/////////// SIGN-IN  //////////
///////////////////////////////

SignRequest = (url, optional filter as text) =>
    let
        parts = Uri.Parts(url),
        account = Text.Split(parts[Host], "."){0},
        resource = "/" & account & Text.Split(parts[Path], "?"){0},
        date = DateTimeZone.ToText(DateTimeZone.UtcNow(), "r"),
        stringToSign = 
            //ADLS List Files
            if Text.Contains(url,"?recursive=true&resource=filesystem") then "GET#(lf)#(lf)#(lf)#(lf)#(lf)#(lf)#(lf)#(lf)#(lf)#(lf)#(lf)#(lf)x-ms-date:" & date & "#(lf)x-ms-version:2018-11-09#(lf)" & resource & "#(lf)recursive:true#(lf)resource:filesystem#(lf)directory:" & filter
            //Blob List Files
            else if Text.Contains(url,"?restype=container&comp=list") then "GET#(lf)#(lf)#(lf)#(lf)#(lf)#(lf)#(lf)#(lf)#(lf)#(lf)#(lf)#(lf)x-ms-date:" & date & "#(lf)x-ms-version:2018-11-09#(lf)" & resource & "#(lf)comp:list#(lf)prefix:" & filter & "#(lf)restype:container"
            //Get ADLS/Blob Files
            else "GET#(lf)#(lf)#(lf)#(lf)#(lf)#(lf)#(lf)#(lf)#(lf)#(lf)#(lf)#(lf)x-ms-date:" & date & "#(lf)x-ms-version:2018-11-09#(lf)" & resource,
        payload = Text.ToBinary(stringToSign, TextEncoding.Utf8),
        password = Extension.CurrentCredential()[Password]?,
        key = if password = null then "" else password,
        secret = try Binary.FromText(key, BinaryEncoding.Base64) otherwise #binary({0}),
        signature = Binary.ToText(Crypto.CreateHmac(CryptoAlgorithm.SHA256, secret, payload), BinaryEncoding.Base64)
    in
        if Extension.CurrentCredential()[AuthenticationKind] = "Implicit" then []
            else
        [
            Authorization = "SharedKey " & account & ":" & signature,
            Accept = "*/*",
            #"x-ms-version" = "2018-11-09",
            #"x-ms-date" = date
        ];
        

///////////////////////
///// NAV Table ///////
///////////////////////

DeltaLakeNavTable = (Folder as text, optional Version as nullable number, optional UseFileBuffer as nullable logical) as table =>
    let       
       Parts = Uri.Parts(Folder),
       Content = 
                    if Text.Contains(Parts[Host], "dfs") then DeltaLakeContentADLSGen2(Folder) 
                    else if Text.Contains(Parts[Host], "blob") then DeltaLakeContentBlob(Folder) 
                    else
                        let
                            Output = error Error.Record("Error", "Path or system not supported", "Error")
                        in
                            Output,
        Distinct = Table.Distinct(Content,{"DeltaTable","Folder Path"}),
        DistinctFiltered = Table.SelectRows(Distinct, each [DeltaTable] <> null and [DeltaTable] <> ""),
        NameKeyColumn = Table.DuplicateColumn(DistinctFiltered,"DeltaTable","NameKey", type text),
        VersionColumn = Table.AddColumn(NameKeyColumn,"Version", each Version),
        UseFileBufferColumn = Table.AddColumn(VersionColumn,"UseFileBuffer", each UseFileBuffer),
        ItemKindColumn = Table.AddColumn(UseFileBufferColumn,"ItemKind", each "Table"),
        ItemNameColumn = Table.AddColumn(ItemKindColumn,"ItemName", each "Table"),
        IsLeafColumn = Table.AddColumn(ItemNameColumn,"IsLeaf", each true),
        source = IsLeafColumn,
        NavViewTables = Table.NavigationTableView(() => source, {"Folder Path","NameKey","Version","UseFileBuffer"}, fn_ReadDeltaTable, [
           Name = each Text.AfterDelimiter([DeltaTable], ".", {0, RelativePosition.FromEnd}),  //[DeltaTable],
           ItemKind = each [ItemKind],
           ItemName = each [ItemName],
           IsLeaf = each [IsLeaf],
           DeltaTablePathNav = each [DeltaTablePath]
        ]),        
        
        NavDistinctDatabase = Table.Distinct(source,"DeltaTablePath"),
        NavDatabases = #table(
            {"Name","Key","Data","ItemKind", "ItemName", "IsLeaf"},
            Table.TransformRows(NavDistinctDatabase, each 
            {[DeltaTablePath], [Name], 
                                     Table.SelectRows(NavViewTables,
                                                         (InnerRow) => //Nested iteration introduced with Table.SelectRows function
                                                            _[DeltaTablePath] = InnerRow[DeltaTablePathNav] 
                                                          )
            , "Database", "Database", false}
            )),
        NavTableDatabases = Table.ToNavigationTable(NavDatabases, {"Key"}, "Name", "Data", "ItemKind", "ItemName", "IsLeaf")

//        
//         NavFolders = #table(
//                     {"Name","Key","Data","ItemKind", "ItemName", "IsLeaf"},
//                     List.Transform(source[DeltaTableFolderList], each 
//                      {_, [Name], _, "Database", "Database", false}
//                     )),
//         NavTableFolder = Table.ToNavigationTable(NavFolders, {"Key"}, "Name", "Data", "ItemKind", "ItemName", "IsLeaf")

    in
        NavTableDatabases;




/////////////////////////////////
///// Read Delta Function ///////
/////////////////////////////////

fn_ReadDeltaTable = (Folder as text, DeltaTable as text, optional Version as nullable number, optional UseFileBuffer as logical, optional DeltaTableOptions as record) as table =>

    let
        DeltaTableVersion = Version, //= if DeltaTableOptions = null then null else Record.FieldOrDefault(DeltaTableOptions, "Version", null),
        PartitionFilterFunction = if DeltaTableOptions = null then (x) => true else if Record.FieldOrDefault(DeltaTableOptions, "PartitionFilterFunction", null) = null then (x) => true else Record.Field(DeltaTableOptions, "PartitionFilterFunction"),
        UseFileBuffer = UseFileBuffer, // if DeltaTableOptions = null then false else if Record.FieldOrDefault(DeltaTableOptions, "UseFileBuffer", null) = null then false else Record.Field(DeltaTableOptions, "UseFileBuffer"),
        IterateFolderContent = if DeltaTableOptions = null then false else if Record.FieldOrDefault(DeltaTableOptions, "IterateFolderContent", null) = null then false else Record.Field(DeltaTableOptions, "IterateFolderContent"),
        TimeZoneOffset = if DeltaTableOptions = null then null else Record.FieldOrDefault(DeltaTableOptions, "TimeZoneOffset", null),
        TimeZoneOffsetDuration = Duration.FromText(Text.TrimStart(TimeZoneOffset, "+")),

        DeltaTableFolderContent = 
            let 
                Parts = Uri.Parts(Folder),
                Content = 
                    if Text.Contains(Parts[Host], "dfs") then DeltaLakeContentADLSGen2(Folder) 
                    else if Text.Contains(Parts[Host], "blob") then DeltaLakeContentBlob(Folder) 
                    else
                        let
                            Output = error Error.Record("Error", "Path or system not supported", "Error")
                        in
                            Output,
                ContentFiltered = Table.SelectRows(Content, each ([DeltaTable] = DeltaTable))
            in
                ContentFiltered,

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

            #"_delta_log Folder" = 
                let
                    Source = DeltaTableFolderContent_wFullPath,
                    #"Filtered Rows" = Table.SelectRows(Source, each Text.Contains([Full_Path], Delimiter & "_delta_log" & Delimiter)),
                    #"Added Version" = Table.AddColumn(#"Filtered Rows", "Version", each try Int64.From(Text.BeforeDelimiter([File_Name], ".")) otherwise -1, Int64.Type),
                    #"Filtered RequestedVersion" = if DeltaTableVersion = null then #"Added Version" else Table.SelectRows(#"Added Version", each [Version] <= DeltaTableVersion),
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
                    #"Expanded fieldList" = Table.ExpandListColumn(#"Expanded schemaString", "fields"),
                    #"Expanded fields" = Table.ExpandRecordColumn(#"Expanded fieldList", "fields", {"name", "type", "nullable", "metadata"}, {"name", "type", "nullable", "metadata"}),
                    #"Added isPartitionedBy" = Table.Buffer(Table.AddColumn(#"Expanded fields", "isPartitionedBy", each List.Contains([partitionColumns], [name]), Logical.Type)),
                    #"Added PBI_DataType" = Table.AddColumn(#"Added isPartitionedBy", "PBI_DataType", 
                        each if [type] = "string" then [PBI_DataType=Text.Type, PBI_Text="Text.Type", PBI_Transformation=Text.From]
                        else if [type] = "long" then [PBI_DataType=Int64.Type, PBI_Text="Int64.Type", PBI_Transformation=Int64.From]
                        else if [type] = "integer" then [PBI_DataType=Int32.Type, PBI_Text="Int32.Type", PBI_Transformation=Int32.From]
                        else if [type] = "short" then [PBI_DataType=Int16.Type, PBI_Text="Int16.Type", PBI_Transformation=Int16.From]
                        else if [type] = "byte" then [PBI_DataType=Int8.Type, PBI_Text="Int8.Type", PBI_Transformation=Int8.From]
                        else if [type] = "float" then [PBI_DataType=Single.Type, PBI_Text="Single.Type", PBI_Transformation=Single.From]
                        else if [type] = "double" then [PBI_DataType=Double.Type, PBI_Text="Double.Type", PBI_Transformation=Double.From]
                        else if [type] = "string" then [PBI_DataType=Text.Type, PBI_Text="Text.Type", PBI_Transformation=Text.From]
                        else if [type] = "date" then [PBI_DataType=Date.Type, PBI_Text="Date.Type", PBI_Transformation=Date.From]
                        else if [type] = "timestamp" and TimeZoneOffset = null then [PBI_DataType=DateTime.Type, PBI_Text="DateTime.Type", PBI_Transformation=DateTime.From]
                        else if [type] = "timestamp" and TimeZoneOffset <> null then [PBI_DataType=DateTimeZone.Type, PBI_Text="DateTimeZone.Type", PBI_Transformation=(x) as nullable datetimezone => DateTime.AddZone(x + TimeZoneOffsetDuration, Duration.Hours(TimeZoneOffsetDuration), Duration.Minutes(TimeZoneOffsetDuration))]
                        else if [type] = "boolean" then [PBI_DataType=Logical.Type, PBI_Text="Logical.Type", PBI_Transformation=Logical.From]
                        else if [type] = "binary" then [PBI_DataType=Binary.Type, PBI_Text="Binary.Type", PBI_Transformation=Binary.From]
                        else if [type] = "null" then [PBI_DataType=Any.Type, PBI_Text="Any.Type", PBI_Transformation=(x) as any => x]
                        else if Text.StartsWith([type], "decimal") then [PBI_DataType=Number.Type, PBI_Text="Number.Type", PBI_Transformation=Number.From]
                        else [PBI_DataType=Any.Type, PBI_Text="Any.Type", PBI_Transformation=(x) as any => x]),
                    #"Expanded PBI_DataType" = Table.ExpandRecordColumn(#"Added PBI_DataType", "PBI_DataType", {"PBI_DataType", "PBI_Text", "PBI_Transformation"}, {"PBI_DataType", "PBI_Text", "PBI_Transformation"}),
                    #"Added ChangeDataType" = Table.AddColumn(#"Expanded PBI_DataType", "ChangeDataType", each {[name], [PBI_DataType]}, type list),
                    #"Added TableDataType" = Table.AddColumn(#"Added ChangeDataType", "TableDataType", each "#""" & [name] & """=" & (if [nullable] then "nullable " else "") & Text.From([PBI_Text]), type text),
                    #"Added ColumnTransformation" = Table.AddColumn(#"Added TableDataType", "ColumnTransformation", each {[name], [PBI_Transformation]}, type list),
                    #"Buffered Fields" = Table.Buffer(#"Added ColumnTransformation")
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
                    #"Added partitionValuesRecord" = Table.AddColumn(#"Added partitionValuesTable2", "partitionValuesRecord", each Record.TransformFields(Record.FromTable([partitionValuesTable]), Table.SelectRows(#"metadata_columns", each [isPartitionedBy] = true)[ColumnTransformation]), Expression.Evaluate("type [" & Text.Combine(Table.SelectRows(#"metadata_columns", each [isPartitionedBy] = true)[TableDataType], ", ") & "]", PQ_DataTypes)),
                    #"Filtered Rows1" = Table.SelectRows(#"Added partitionValuesRecord", each PartitionFilterFunction([partitionValuesRecord])),
                    #"Expanded partitionValuesRecord" = Table.ExpandRecordColumn(#"Filtered Rows1", "partitionValuesRecord", Table.SelectRows(#"metadata_columns", each [isPartitionedBy] = true)[name]),
                    #"Added Full_Path" = Table.AddColumn(#"Expanded partitionValuesRecord", "Full_Path", each Text.Replace(DeltaTablePath & Text.Replace([file_name], "=", "%3D"), "/", Delimiter), Text.Type),
                    #"Removed Columns3" = Table.RemoveColumns(#"Added Full_Path",{"file_name", "partitionValuesJSON", "isRelevant", "partitionValuesTable"}),
                    #"Buffered RelevantFiles" = Table.Buffer(#"Removed Columns3"),
                    #"Merged Queries" = Table.NestedJoin(#"Buffered RelevantFiles", {"Full_Path"}, DeltaTableFolderContent_wFullPath, {"Full_Path"}, "DeltaTable Folder", JoinKind.Inner),
                    #"Removed Columns" = Table.RemoveColumns(#"Merged Queries",{"Full_Path"}),
                    #"Expanded DeltaTable Folder" = Table.ExpandTableColumn(#"Removed Columns", "DeltaTable Folder", {"Content"}, {"Content"}),
                    BufferFile = if UseFileBuffer then Table.TransformColumns(#"Expanded DeltaTable Folder",{{"Content", Binary.Buffer}}) else #"Expanded DeltaTable Folder",
                    #"Added Custom1" = Table.AddColumn(BufferFile, "Data", each Parquet.Document([Content]), Expression.Evaluate("type table [" & Text.Combine(metadata_columns[TableDataType], ", ") & "]", PQ_DataTypes)),
                    #"Removed Columns1" = Table.RemoveColumns(#"Added Custom1",{"Content"}),
                    #"Expanded Data" = Table.ExpandTableColumn(#"Removed Columns1", "Data", Table.SelectRows(metadata_columns, each not [isPartitionedBy])[name]),
                    #"Changed Type" = Table.TransformColumns(#"Expanded Data",Table.SelectRows(metadata_columns, each [type] = "timestamp")[ColumnTransformation]),
                    #"Reordered Columns" = Table.ReorderColumns(if TimeZoneOffset = null then #"Expanded Data" else #"Changed Type", metadata_columns[name])
                in
                    #"Reordered Columns"

    in 
        #"Data";

//////////////////////////////////////
//////////Data Source kind////////////
//////////////////////////////////////        

DeltaLake = [
    // Needed for use with Power BI Service.
    TestConnection = (dataSourcePath) =>
        let
            json = Json.Document(dataSourcePath),
            Url = json[Url]
        in
            { "DeltaLake.Contents" , dataSourcePath, [Version = 1, UseFileBuffer = false] },
     Authentication = [
// Not working with Blob Storage due to error message "The specified resource does not exist" for reading out the tenant information
//         Aad = [
//             AuthorizationUri = (dataSourcePath) =>
//                 GetAuthorizationUrlFromWwwAuthenticate(
//                     GetServiceRootFromDataSourcePath(dataSourcePath)
//                 ),
//             Resource = "https://storage.azure.com/" 
//         ],
        Aad = [
            AuthorizationUri = "https://login.microsoftonline.com/common/oauth2/authorize",
            Resource =  "https://storage.azure.com/"
        ],
        Key = [
            Label = "Access key",
            KeyLabel = "Access key"
        ],
        Implicit = []
    ]
];

//////////////////////////////////////
//////Data Source UI publishing///////
////////////////////////////////////// 

DeltaLake.Publish = [
    SupportsDirectQuery = false,
    Beta = true,
    Category = "Other",
    ButtonText = { "Delta Lake", Extension.LoadString("ButtonHelp") },//{ Extension.LoadString("ButtonTitle"), Extension.LoadString("ButtonHelp") },
    LearnMoreUrl = "https://powerbi.microsoft.com/",
    SourceImage = DeltaLake.Icons,
    SourceTypeImage = DeltaLake.Icons
];

DeltaLake.Icons = [
    Icon16 = { Extension.Contents("DeltaLake16.png"), Extension.Contents("DeltaLake20.png"), Extension.Contents("DeltaLake24.png"), Extension.Contents("DeltaLake32.png") },
    Icon32 = { Extension.Contents("DeltaLake32.png"), Extension.Contents("DeltaLake40.png"), Extension.Contents("DeltaLake48.png"), Extension.Contents("DeltaLake64.png") }
];


//////////////////////////////////////
////////// Helper Function ///////////
////////////////////////////////////// 

Table.NavigationTableView =
(
    baseTable as function,
    keyColumns as list,
    dataCtor as function,
    descriptor as record
) as table =>
    let
        transformDescriptor = (key, value) =>
            let
                map = [
                    Name = "NavigationTable.NameColumn",
                    Data = "NavigationTable.DataColumn",
                    Tags = "NavigationTable.TagsColumn",
                    ItemKind = "NavigationTable.ItemKindColumn",
                    ItemName = "Preview.DelayColumn",
                    IsLeaf = "NavigationTable.IsLeafColumn"
                ]
            in
                if value is list
                    then [Name=value{0}, Ctor=value{1}, MetadataName = Record.FieldOrDefault(map, key)]
                    else [Name=key, Ctor=value, MetadataName = Record.FieldOrDefault(map, key)],
        fields = List.Combine({
            List.Transform(keyColumns, (key) => [Name=key, Ctor=(row) => Record.Field(row, key), MetadataName=null]),
            if Record.HasFields(descriptor, {"Data"}) then {}
                else {transformDescriptor("Data", (row) => Function.Invoke(dataCtor, Record.ToList(Record.SelectFields(row, keyColumns))))},
            Table.TransformRows(Record.ToTable(descriptor), each transformDescriptor([Name], [Value]))
        }),
        metadata = List.Accumulate(fields, [], (m, d) => let n = d[MetadataName] in if n = null then m else Record.AddField(m, n, d[Name])),
        tableKeys = List.Transform(fields, each [Name]),
        tableValues = List.Transform(fields, each [Ctor]),
        tableType = Type.ReplaceTableKeys(
            Value.Type(#table(tableKeys, {})),
            {[Columns=keyColumns, Primary=true]}
        ) meta metadata,
        reduceAnd = (ast) => if ast[Kind] = "Binary" and ast[Operator] = "And" then List.Combine({@reduceAnd(ast[Left]), @reduceAnd(ast[Right])}) else {ast},
        matchFieldAccess = (ast) => if ast[Kind] = "FieldAccess" and ast[Expression] = RowExpression.Row then ast[MemberName] else ...,
        matchConstant = (ast) => if ast[Kind] = "Constant" then ast[Value] else ...,
        matchIndex = (ast) => if ast[Kind] = "Binary" and ast[Operator] = "Equals"
            then
                if ast[Left][Kind] = "FieldAccess"
                    then Record.AddField([], matchFieldAccess(ast[Left]), matchConstant(ast[Right]))
                    else Record.AddField([], matchFieldAccess(ast[Right]), matchConstant(ast[Left]))
            else ...,
        lazyRecord = (recordCtor, keys, baseRecord) =>
            let record = recordCtor() in List.Accumulate(keys, [], (r, f) => Record.AddField(r, f, () => (if Record.FieldOrDefault(baseRecord, f, null) <> null then Record.FieldOrDefault(baseRecord, f, null) else Record.Field(record, f)), true)),
        getIndex = (selector, keys) => Record.SelectFields(Record.Combine(List.Transform(reduceAnd(RowExpression.From(selector)), matchIndex)), keys)
    in
        Table.View(null, [
            GetType = () => tableType,
            GetRows = () => #table(tableType, List.Transform(Table.ToRecords(baseTable()), (row) => List.Transform(tableValues, (ctor) => ctor(row)))),
            OnSelectRows = (selector) =>
                let
                    index = try getIndex(selector, keyColumns) otherwise [],
                    default = Table.SelectRows(GetRows(), selector)
                in
                    if Record.FieldCount(index) <> List.Count(keyColumns) then default
                    else Table.FromRecords({
                        index & lazyRecord(
                            () => Table.First(default),
                            List.Skip(tableKeys, Record.FieldCount(index)),
                            Record.AddField([], "Data", () => Function.Invoke(dataCtor, Record.ToList(index)), true))
                        },
                        tableType)
        ]);


// Implement this function to retrieve or calculate the service URL based on the data source path parameters
GetServiceRootFromDataSourcePath = (dataSourcePath) as text => 
    let       
        Json = Json.Document(dataSourcePath),
        Extract = Record.Field(Json, "Folder"),
        Parts = Uri.Parts(Extract),        
        Url =  
           //ADLS List Files
            if Text.Contains(Parts[Host], "dfs") then Extract & "?recursive=true&resource=filesystem"
            //Blob List Files
            else if Text.Contains(Parts[Host], "blob") then Extract & "?restype=container&comp=list"
            //Get ADLS/Blob Files
            else "Error: Invalid Path"
    in
        Url;          

GetAuthorizationUrlFromWwwAuthenticate = (url as text) as text =>
    let
        // Sending an unauthenticated request to the service returns
        // a 302 status with WWW-Authenticate header in the response. The value will
        // contain the correct authorization_uri.
        // 
        // Example:
        // Bearer authorization_uri="https://login.microsoftonline.com/{tenant_guid}/oauth2/authorize"
        responseCodes = {302, 401},
        endpointResponse = Web.Contents(url, [
            ManualCredentials = true,
            ManualStatusHandling = responseCodes
        ])
    in
        if (List.Contains(responseCodes, Value.Metadata(endpointResponse)[Response.Status]?)) then
            let
                headers = Record.FieldOrDefault(Value.Metadata(endpointResponse), "Headers", []),
                wwwAuthenticate = Record.FieldOrDefault(headers, "WWW-Authenticate", ""),
                split = Text.Split(Text.Trim(wwwAuthenticate), " "),
                authorizationUri = List.First(List.Select(split, each Text.Contains(_, "authorization_uri=")), null)
            in
                if (authorizationUri <> null) then
                    // Trim and replace the double quotes inserted before the url
                    Text.Replace(Text.Trim(Text.Trim(Text.AfterDelimiter(authorizationUri, "=")), ","), """", "")
                else
                    error Error.Record("DataSource.Error", "Unexpected WWW-Authenticate header format or value during authentication.", [
                        #"WWW-Authenticate" = wwwAuthenticate
                    ])
        else
            error Error.Unexpected("Unexpected response from server during authentication.");


Value.WaitFor = (producer as function, interval as function, optional count as number) as any =>
    let
        list = List.Generate(
            () => {0, null},
            (state) => state{0} <> null and (count = null or state{0} < count),
            (state) => if state{1} <> null then {null, state{1}} else {1 + state{0}, Function.InvokeAfter(() => producer(state{0}), interval(state{0}))},
            (state) => state{1})
    in
        List.Last(list);

Table.ToNavigationTable = (
    table as table,
    keyColumns as list,
    nameColumn as text,
    dataColumn as text,
    itemKindColumn as text,
    itemNameColumn as text,
    isLeafColumn as text
) as table =>
    let
        tableType = Value.Type(table),
        newTableType = Type.AddTableKey(tableType, keyColumns, true) meta 
        [
            NavigationTable.NameColumn = nameColumn, 
            NavigationTable.DataColumn = dataColumn,
            NavigationTable.ItemKindColumn = itemKindColumn, 
            Preview.DelayColumn = itemNameColumn, 
            NavigationTable.IsLeafColumn = isLeafColumn
        ],
        navigationTable = Value.ReplaceType(table, newTableType)
    in
        navigationTable;