# Reading Delta Lake tables natively in PowerBI

The provided PowerQuery/M function allows you to read a Delta Lake table directly from any storage supported by PowerBI. Common storages which have also been tested include Azure Data Lake Store, Azure Blob Storage or a local folder or file share.

# Features

- Read Delta Lake table into PowerBI without having a cluster (Spark, Databricks, Azure Synapse) up and running
- Online/Scheduled Refresh in the PowerBI service
- Support all storage systems that are supported by PowerBI
  - Azure Data Lake Store Gen2 (tested)
  - Azure Blob Storage (tested)
  - Local Folder or Network Share (tested)
  - Azure Data Lake Store Gen1 (tested)
  - Local Hadoop / HDFS (partially tested, check `UseFileBuffer` option)
- Support for Partition Elimination to leverage the partitioning schema of the Delta Lake table ([details](#PartitionFilterFunction))
- Support for File Pruning using file stats ([details](#StatsFilterFunction))
- Support all simple and complex data types (struct, map, array, ...)
- Added shortcut to read `COUNT` from `_delta_log` directly if possible
- Support for Delta Lake time travel - e.g. `VERSION AS OF`
  - also supports negative values for `VERSION AS OF` to easily access the previous version using a value of `-1`
- Support for `TimeZoneOffset` to automatically convert all timestamps to a given timezone - e.g. `+2:00`
- Support for `minReaderVersion` up to `2`

# Usage

1. In PowerBI desktop, go to Home -> Queries -> Transform Data
2. Once you are in the Power Query Editor use Home -> New Source -> Blank query
3. Go to Home -> Query -> Advanced Editor
4. Paste the code of the custom function: [fn_ReadDeltaTable.pq](fn_ReadDeltaTable.pq) and name the query `fn_ReadDeltaTable`
5. Connect to your storage - e.g. create a PQ query with the following code (paste it via the Advanced Editor) and call it `Blob_Content`

```m
let
    Source = AzureStorage.Blobs("https://gbadls01.blob.core.windows.net/public"),
    #"Filtered Rows" = Table.SelectRows(Source, each Text.StartsWith([Name], "powerbi_delta/FactInternetSales_part.delta/"))
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
### **Version**
A numeric value that defines historic specific version of the Delta Lake table you want to read. This is similar to specifying 
`VERSION AS OF` When querying the Delta Lake table via SQL. Default is the most recent/current version.
You can also specify a negative value to go backwards from the most recent/current version number.
e.g using a value of `-1` to load the previous version of the Delta table.
### **UseFileBuffer** 
Some data sources do not support streaming of binary files and you may receive an error message like **"Parquet.Document cannot be used with streamed binary values."**. To mitigate this issue, you can set `UseFileBuffer=true`. Details about this issue and implications are desribed [here](https://blog.crossjoin.co.uk/2021/03/07/parquet-files-in-power-bi-power-query-and-the-streamed-binary-values-error/).
Please be aware that this option can have negative performance impact!
### **PartitionFilterFunction**
A fuction that is used to filter out partitions before actually reading the files. The function has to take 1 parameter of type `record` and must return a `logical` type (true/false). The record that is passed in can then be used to specify the partition filter. For each file in the delta table the metadata is checked against this function. If it is not matched, it is discarded from the final list of files that make up the Delta Lake table.
Assuming your Delta Lake table is partitioned by Year and Month and you want to filter for `Year=2021` and `Month="Jan"` your function may look like this:
```m
(PartitionValues as record) as logical =>
    Record.Field(PartitionValues, "Year") = 2021 and Record.Field(PartitionValues, "Month") = "Jan"
```

If you are lazy you can also use this shorter version without explicit type definitions:

```m
(x) => Record.Field(x, "Year") = 2021 and Record.Field(x, "Month") = "Jan"
```

or even more lightweight

```m
(x) => x[Year] = 2021 and x[Month] = "Jan"
```

It supports all possible variations that are supported by Power Query/M so you can also build complex partition filters.

### **StatsFilterFunction**

A fuction that is used to filter out files based on the min/max values in the delta log before actually reading the files. The function has to take 2 parameter of type `record` and must return a `logical` type (true/false). The first record passed to the function are the `minValues`, the second record are the `maxValues` from the file statistics. They can then be used in a similar way as the [PartitionFilterFunction](#partitionfilterfunction):
Assuming your Delta Lake table is partitioned by Year and Month and you want to filter for `Year=2021` and `Month="Jan"` your function may look like this:

```m
= (
    minValues as record,
    maxValues as record
) as logical =>

Record.Field(minValues, "ProductKey") <= 220 and Record.Field(maxValues, "OrderDateKey") >= 20080731
```

### **IterateFolderContent**

Some data sources (like Azure Data Lake Store Gen1) do not automatically expand all sub-folders to get the single files. To make the function work with those data sources you can set `IterateFolderContent=true`.
Please be aware that this option can have negative performance impact!

### **TimeZoneOffset**

Apache Parquet has no built-in data type for timestamps with offset hence all timestamps are stored physically as UTC. As Delta Lake is also based on Apache Parquet, this also applies here. So, to explicitly change the timezone for all timestamps that are read from the Delta Lake table, you can use `TimeZoneOffset="+02:00"`. The resulting columns will then be of type DateTimeZone with the offset of `+02:00` and the DateTime-value shifted by +2 hours. The parameter supports the following format only: `[+|-][HH:mm]`
### **additional options may be added in the future!**

# Known limitations

- Time Travel
  - currently only supports `VERSION AS OF`
  - `TIMESTAMP AS OF` not yet supported
- complex data types in combination with `minReaderVersion >= 2`

# Examples

The examples below can be used *as-is* in Power BI desktop. If you are prompted for authentication, just select `Anonymous` for your authentication method.
> **Note:** In the examples the root folder of the Delta Lake table ends with `.delta`. This is not mandatory and can be any path.

## Using Delta Lake Time Travel

To use Delta Lake Time Travel you need to specify the `Version`-option as part of the second argument. The following example reads the Version 1 of a Delta Lake table from an Azure Blob Storage. 
```m
let
    Source = AzureStorage.Blobs("https://gbadls01.blob.core.windows.net/public"),
    #"Filtered Rows" = Table.SelectRows(Source, each Text.StartsWith([Name], "powerbi_delta/FactInternetSales_part.delta/")),
    DeltaTable = fn_ReadDeltaTable(#"Filtered Rows", [Version=1])
in
    DeltaTable
```

## Using Delta Lake Partition Elimination

Partition Elimination is a crucial feature when working with large amounts of data. Without it, you would need to read the whole table and discard a majority of the rows afterwards which is not very efficient. This can be accomplished by using the `PartitionFilterFunction`-option as part of the second argument. In the example below our table is partitioned by `SalesTerritoryKey` (integer) and we only want to load data from Sales Territories where the `SalesTerritoryKey` is greater or equal to `5`:

```m
let
    Source = AzureStorage.Blobs("https://gbadls01.blob.core.windows.net/public"),
    #"Filtered Rows" = Table.SelectRows(Source, each Text.StartsWith([Name], "powerbi_delta/FactInternetSales_part.delta/")),
    DeltaTable = fn_ReadDeltaTable(#"Filtered Rows", [PartitionFilterFunction = (x) => Record.Field(x, "SalesTerritoryKey") >= 5])
in
    DeltaTable
```

## Reading from Azure Data Lake Store Gen1

To read diretly from an Azure Data Lake Store Gen1 folder, you need to specify the options `UseFileBuffer=true` and `IterateFolderContent=true`:

```m
let
    Source = DataLake.Contents("adl://myadlsgen1.azuredatalakestore.net/DeltaSamples/FactInternetSales_part.delta", [PageSize=null]),
    DeltaTable = fn_ReadDeltaTable(Source, [UseFileBuffer = true, IterateFolderContent = true])
in
    DeltaTable
```

## Reading from Azure Data Lake Store Gen2

You can also read directly from an Azure Data Lake Store Gen2 using the snippet below. If you want/need to use `HierarchicalNavigation = true` you may add `IterateFolderContent=true` to the options of `fn_ReadDeltaTable`. This may speed up overall performance - but usually varies from case to case so please test this on your own data first!

```m
let
    Source = AzureStorage.DataLake("https://gbadls01.dfs.core.windows.net/public/powerbi_delta/DimProduct.delta", [HierarchicalNavigation = false]),
    DeltaTable = fn_ReadDeltaTable(Source, [PartitionFilterFunction=(x) => x[Year] = 2021 and x[Month] = "Jan"])
in
    DeltaTable
```

# FAQ

**Q:** The Power Query UI does not show the second parameter. How can I use it?

**A:** To use the second parameter of the function you need to use the advanced editor. Power Query does currently not support parameters of type record in the UI

--------------------
**Q:** How can I use [Delta Lake Time Travel](https://databricks.com/blog/2019/02/04/introducing-delta-time-travel-for-large-scale-data-lakes.html)?

**A:** The function supports an optional second parameter to supply generic parameters. To query specific version of the Delta Lake table, you can provide a record with the field `Version` and the value of the version you want to query. For example, to read Version 123 of your Delta Table, you can use the following M code: `fn_ReadDeltaTable(DeltaTableFolderContents, [Version=123])`

--------------------
**Q:** The data source I am using does not work with the `fn_ReadDeltaTable` function - what can I do?

**A:** Please open a support ticket/issue in the git repository.
