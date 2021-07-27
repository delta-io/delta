# Reading Delta Lake tables natively in PowerBI
The provided PowerQuery/M function allows you to read a Delta Lake table directly from any storage supported by PowerBI. Common storages which have also been tested include Azure Data Lake Store, Azure Blob Storage or a local folder or file share.

# Features
- Read Delta Lake table into PowerBI without having a cluster (Spark, Databricks, Azure Synapse) up and running
- Online/Scheduled Refresh in the PowerBI service 
- Support all storage systems that are supported by PowerBI
    - Azure Data Lake Store (tested)
    - Azure Blob Storage (tested)
    - Local Folder or Network Share (tested)
    - AWS S3 (not yet tested)
    - Local Hadoop / HDFS (partially tested, check `UseFileBuffer` option)
- Support for Partition Elimination to leverage the partitioning schema of the Delta Lake table
- Support for Delta Lake time travel - e.g. `VERSION AS OF`

# Usage
1. In PowerBI desktop, go to Home -> Queries -> Transform Data
2. Once you are in the Power Query Editor use Home -> New Source -> Blank query
3. Go to Home -> Query -> Advanced Editor
4. Paste the code of the custom function: [fn_ReadDeltaTable.pq](fn_ReadDeltaTable.pq) and name the query `fn_ReadDeltaTable`
5. Connect to your storage - e.g. create a PQ query with the following code and call it `Blob_Content`
```
let
    Source = AzureStorage.Blobs("https://gbadls01.blob.core.windows.net/public"),
    #"Filtered Rows" = Table.SelectRows(Source, each Text.StartsWith([Name], "powerbi_delta/FactInternetSales_part.delta/")),
in
    #"Filtered Rows"
```
6. Open your query that contains the function and select `Blob_Content` in the parameter `DeltaTableFolderContent`
7. Click `Invoke`
7. A new PQ query will be created for you showing the contents of the Delta Lake table

# Parameters
The function supports two parameters of which the second is optional:
1. DeltaTableFolderContent
2. DeltaTableOptions


## Parameter DeltaTableFolderContent
A table that contains a file/folder listing of your Delta Lake table. PowerBI supports a wide set of storage services which you can use for this. There are however some mandatory things this file/folder listing has to cotain:
- a sub-folder `_delta_log` (which holds the Delta Log files and also ensures that the parent folder is the root of the Delta Lake table)
- mandatory columns `Name`, `Folder Path`, `Content`, `Extension`
These are all returned by default for common Storage connectors like Azure Data Lake Storage Gen2 or Azure Blob Storaage

## Parameter DeltaTableOptions
An optional record that be specified to control the following options:
- `Version` - a numeric value that defines historic specific version of the Delta Lake table you want to read. This is similar to specifying `VERSION AS OF` when querying the Delta Lake table via SQL. Default is the most recent/current version.
- `UseFileBuffer` - some data sources do not support streaming of binary files and you may receive an error message like **"Parquet.Document cannot be used with streamed binary values."**. To mitigate this issue, you can set `UseFileBuffer=true`. Details about this issue and implications are desribed [here](https://blog.crossjoin.co.uk/2021/03/07/parquet-files-in-power-bi-power-query-and-the-streamed-binary-values-error/)
- `PartitionFilterFunction` - a fuction that is used to filter out partitions before actually reading the files. The function has to take 1 parameter of type `record` and must return a `logical` type (true/false). The record that is passed in can then be used to specify the partition filter. For each file in the delta table the metadata is checked against this function. If it is not matched, it is discarded from the final list of files that make up the Delta Lake table.
Assuming your Delta Lake table is partitioned by Year and Month and you want to filter for `Year=2021` and `Month="Jan"` your function may look like this:
```
(PartitionValues as record) as logical =>
    Record.Field(PartitionValues, "Year") = 2021 and Record.Field(PartitionValues, "Month") = "Jan"
```

If you are lazy you can also use this shorter version without explicit type definitions:
```
(x) => Record.Field(x, "Year") = 2021 and Record.Field(x, "Month") = "Jan"
```
or even more lightweight
```
(x) => x[Year] = 2021 and x[Month] = "Jan"
```

It supports all possible variations that are supported by Power Query/M so you can also build complex partition filters.
- additional options may be added in the future!

# Known limitations
- Time Travel
   - currently only supports `VERSION AS OF`
   - `TIMESTAMP AS OF` not yet supported

# Examples
The examples below can be used *as-is* in Power BI desktop. If you are prompted for authentication, just select `Anonymous` for your authentication method.
> Note: In the examples the root folder of the Delta Lake table ends with `.delta`. This is not mandatory and can be any path.

## Using Delta Lake Time Travel
To use Delta Lake Time Travel you need to specify the `Version`-option as part of the second argument. The following example reads the Version 123 of a Delta Lake table from an Azure Blob Storage. 
```
let
    Source = AzureStorage.Blobs("https://gbadls01.blob.core.windows.net/public"),
    #"Filtered Rows" = Table.SelectRows(Source, each Text.StartsWith([Name], "powerbi_delta/FactInternetSales_part.delta/")),
    DeltaTable = fn_ReadDeltaTable(#"Filtered Rows", [Version=1])
in
    DeltaTable
```

## Using Delta Lake Partition Elimination
Partition Elimination is a crucial feature when working with large amounts of data. Without it, you would need to read the whole table and discard a majority of
```
let
    Source = AzureStorage.Blobs("https://gbadls01.blob.core.windows.net/public"),
    #"Filtered Rows" = Table.SelectRows(Source, each Text.StartsWith([Name], "powerbi_delta/FactInternetSales_part.delta/")),
    DeltaTable = fn_ReadDeltaTable(#"Filtered Rows", [PartitionFilterFunction = (x) => Record.Field(x, "SalesTerritoryKey") >= 5])
in
    DeltaTable
```



# FAQ
**Q:** The Power Query UI does not show the second parameter. How can I use it?

**A:** To use the second parameter of the function you need to use the advanced editor. Power Query does currently not support parameters of type record in the UI

--------------------
**Q:** How can I use [Delta Lake Time Travel](https://databricks.com/blog/2019/02/04/introducing-delta-time-travel-for-large-scale-data-lakes.html)?

**A:** The function supports an optional second parameter to supply generic parameters. To query specific version of the Delta Lake table, you can provide a record with the field `Version` and the value of the version you want to query. For example: `fn_ReadDeltaTable(#"Filtered Rows", [Version=123])`
