# Hive Catalog Tests - Temporarily Disabled for Flink 2.0

## Status: DISABLED (Awaiting Flink 2.0 Hive Connector)

The following Hive Catalog integration tests are temporarily disabled because
Apache Flink 2.0 does not yet have a released Hive connector.

### Disabled Test Classes (5):
1. HiveCatalogDeltaCatalogITCase.java
2. HiveCatalogDeltaEndToEndTableITCase.java  
3. HiveCatalogDeltaFlinkSqlITCase.java
4. HiveCatalogDeltaSinkTableITCase.java
5. HiveCatalogDeltaSourceTableITCase.java

### Disabled Support Classes (7):
1. HiveCatalogExtension.java
2. HiveJUnitExtension.java
3. HiveMetaStoreCore.java
4. HiveMetaStoreJUnitExtension.java
5. HiveServerContext.java
6. ThriftHiveMetaStoreCore.java
7. ThriftHiveMetaStoreJUnitExtension.java

### Dependencies Missing:
- `org.apache.flink:flink-connector-hive_2.12:2.x.x` (not released yet)
- `org.apache.hadoop.hive:hive-metastore:*`
- `org.apache.hadoop.hive:hive-exec:*`

### Re-enabling Strategy:
Once Apache Flink releases the Hive connector for Flink 2.x:
1. Update build.sbt with correct Hive connector dependency
2. Re-enable these test classes  
3. Update HiveCatalog support in CatalogLoader.java
4. Run tests to verify compatibility

### References:
- Main issue: #5228
- Related: CatalogLoader.java HiveCatalog support

**NOTE**: All InMemory Catalog tests remain enabled and fully functional.
