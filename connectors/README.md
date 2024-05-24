> [!IMPORTANT]
Delta Standalone is no longer actively maintained. As Delta added more features (such as deletion vectors and column mapping) in the latest versions of the Delta protocol, the current Standalone APIs no longer work on Delta tables with these newer features. It requires significant effort from the connector developer to support higher versions of Delta protocol features. A new initiative called [Delta Kernel](https://github.com/delta-io/delta/blob/master/kernel) has been developed to support higher Delta protocol versions with much simpler APIs and to keep the work required for connectors to support the newer table features as simple as upgrading the Kernel dependency version. Connector developers are advised to use the [Delta Kernel](https://github.com/delta-io/delta/blob/master/kernel). 

Soon, all connectors in this repo (e.g., the Flink-Delta connector) will be updated to use [Delta Kernel](https://github.com/delta-io/delta/blob/master/kernel).


## Delta Standalone

Delta Standalone, formerly known as the Delta Standalone Reader (DSR), is a JVM library to read **and write** Delta tables. Unlike https://github.com/delta-io/delta, this project doesn't use Spark to read or write tables and it has only a few transitive dependencies. It can be used by any application that cannot use a Spark cluster.
- To compile the project, run `build/sbt standalone/compile`
- To test the project, run `build/sbt standalone/test`
- To publish the JAR, run `build/sbt standaloneCosmetic/publishM2`

See [Delta Standalone](https://docs.delta.io/latest/delta-standalone.html) for detailed documentation.

## Hive Connector

Read Delta tables directly from Apache Hive using the [Hive Connector](/hive/README.md). See the dedicated [README.md](/hive/README.md) for more details.

## Flink/Delta Connector

Use the [Flink/Delta Connector](flink/README.md) to read and write Delta tables from Apache Flink applications. The connector includes a sink for writing to Delta tables from Apache Flink, and a source for reading Delta tables using Apache Flink (still in progress.) See the dedicated [README.md](/flink/README.md) for more details.

## sql-delta-import

[sql-delta-import](/sql-delta-import/readme.md) allows for importing data from a JDBC source into a Delta table.

## Power BI connector
The connector for [Microsoft Power BI](https://powerbi.microsoft.com/) is basically just a custom Power Query function that allows you to read a Delta table from any file-based [data source supported by Microsoft Power BI](https://docs.microsoft.com/en-us/power-bi/connect-data/desktop-data-sources). Details can be found in the dedicated [README.md](/powerbi/README.md).

