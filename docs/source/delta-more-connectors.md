
# Other connectors

#### Apache Druid
This [connector](https://druid.apache.org/docs/latest/development/extensions-contrib/delta-lake/) allows [Apache Druid](https://druid.apache.org/) to read from <Delta>.

#### Apache Pulsar
This [connector](https://github.com/streamnative/pulsar-io-lakehouse/blob/master/docs/delta-lake-demo.md) allows [Apache Pulsar](https://pulsar.apache.org/) to read from and write to <Delta>.

#### ClickHouse
[ClickHouse](https://clickhouse.com/) is a column-oriented database that allows users to run SQL queries on <Delta> tables. This [connector](https://clickhouse.com/docs/en/engines/table-engines/integrations/deltalake) provides a read-only integration with existing <Delta> tables in Amazon S3.

#### Dagster
Use the [Delta Lake IO Manager](https://delta-io.github.io/delta-rs/integrations/delta-lake-dagster/) to read from and write to <Delta> tables in your [Dagster](https://dagster.io/) orchestration pipelines.

#### FINOS Legend
An [extension](https://github.com/finos/legend-community-delta/blob/main/README.md) to the [FINOS](https://landscape.finos.org/) Legend framework for Apache Spark™ / Delta Lake based environment, combining best of open data standards with open source technologies. This connector allows Trino to read from and write to Delta Lake.

#### Hopsworks
This [connectors](https://docs.hopsworks.ai/latest/user_guides/fs/feature_group/create/#batch-write-api) allows [Hopsworks Feature Store](https://www.hopsworks.ai/dictionary/feature-store) store, manage, and serve feature data in Delta Lake.

#### Apache Hive
This integration enables reading Delta tables from Apache Hive. For details on installing the integration, see the [Delta Lake repository](https://github.com/delta-io/delta/tree/master/connectors/hive).

#### Kafka Delta Ingest
This [project](https://github.com/delta-io/kafka-delta-ingest) builds a highly efficient daemon for streaming data through Apache Kafka into Delta Lake.

#### SQL Delta Import
This [utility](https://github.com/delta-io/delta/blob/master/connectors/sql-delta-import/readme.md) is for importing data from a JDBC source into a Delta Lake table.

#### StarRocks
[StarRocks](https://www.starrocks.io/), a Linux Foundation project, is a next-generation sub-second MPP OLAP database for full analytics scenarios, including multi-dimensional analytics, real-time analytics, and ad-hoc queries. StarRocks has the [ability to read](https://docs.starrocks.io/docs/introduction/StarRocks_intro/) from Delta Lake.

.. <Delta> replace:: Delta Lake
