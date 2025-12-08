# Delta Flink Connector

## 1. Introduction

The **Delta Flink Connector** enables Apache Flink streaming jobs to write data into **Delta Lake tables**.

### Supported Flink Versions
- **Flink v1.20.x**
- **Flink v2.0**

### Connector Overview
- Built on **Flink Connector V2 API**
- Based on **Delta Kernel**
- **Sink-only** connector (no source support yet)
- Uses a **single global committer**
- Provides **exactly-once delivery semantics**

### Key Features
- **Exactly-once semantics**
  - Integrated with Flink checkpointing
  - Single global committer ensures atomic commits
- **Memory control for highly partitioned tables**
  - Limits the number of concurrently open files
  - Prevents OOM when writing to tables with many partitions
- **Fast, memory-efficient incremental checkpoints**
  - Avoids rewriting large checkpoint metadata
- **File rolling**
  - Control file size and file count
  - Helps mitigate small-file problems

---

## 2. Build & Deployment

### Build the Connector

The project is built using `sbt`.

```bash
sbt -DflinkVersion={1.20|2.0|default} flink/test

sbt -DflinkVersion={1.20|2.0|default} flink/assembly
```

- `flinkVersion` is **optional**
- If not specified, a default Flink version is used

### Build Output

After a successful build:
- An **assembly JAR** will be generated
- The JAR already contains **Delta Kernel**
- The JAR must be made available to Flink (see quick start below)

### S3 / Cloud Storage Dependencies

To access **S3 or S3-compatible storage**, an **AWS SDK bundle** must be provided separately.

- Tested with:
  - **AWS SDK bundle `bundle-2.23.x`**
- The bundle must be available on the Flink classpath

---

## 3. Quick Start (Copy & Run)

This repository includes a local Flink environment for quick testing via Docker Compose.

### Steps

1. **Build the connector**
   ```bash
   sbt flink/assembly
   ```

2. **Copy the assembly JAR into the docker folder**

   We provide two Flink version under `docker`
   ```bash
   cp flink/target/delta-flink-<flink_version>-*.jar flink/docker/<flink_version>/usrlib
   ```
3. **(Only the first time) Download Additional Jars**
   ```bash
   cd docker/<flink_version>/usrlib
   wget https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.23.19/bundle-2.23.19.jar 
   wget https://repo1.maven.org/maven2/com/google/guava/guava/33.5.0-jre/guava-33.5.0-jre.jar
   ```
4. **Start the local Flink cluster**
   ```bash
   cd docker/<flink_version>
   docker compose up -d
   ```

This will start:
- 1 JobManager
- Multiple TaskManagers

The setup is intended for local testing and development workflows.

---

## 4. Usage

The Delta sink can be used with:
- **DataStream API**
- **Table / SQL API**

---

### 4.1 DataStream API (Java)

#### Write to Local Filesystem (Path-based Table)

```java
import java.util.Map;
import org.apache.flink.table.data.RowData;

DeltaSink sink =
    DeltaSink.builder()
        .withFlinkSchema(flinkSchema)
        .withConfigurations(
            Map.of(
                "type", "hadoop",
                "hadoop.table_path", "file:///table-path"
            )
        )
        .build();
```

#### Write to Unity Catalog (Catalog-managed Table)

```java
import java.util.Map;
import org.apache.flink.table.data.RowData;

DeltaSink sink =
    DeltaSink.builder()
        .withFlinkSchema(flinkSchema)
        .withConfigurations(
            Map.of(
                "type", "unitycatalog",
                "unitycatalog.name", "ab",
                "unitycatalog.table_name", "ab.cd.ef",
                "unitycatalog.endpoint", "http://localhost:8080/",
                "unitycatalog.token", "wow"
            )
        )
        .build();
```

#### Write to Unity Catalog Table Using Path-based Access

```java
import java.util.Map;
import org.apache.flink.table.data.RowData;

DeltaSink sink =
    DeltaSink.builder()
        .withFlinkSchema(flinkSchema)
        .withConfigurations(
            Map.of(
                "type", "ucpath",
                "unitycatalog.name", "ab",
                "unitycatalog.table_name", "ab.cd.ef",
                "unitycatalog.endpoint", "http://uc.deploy.com/",
                "unitycatalog.token", "wow"
            )
        )
        .build();
```

---

### 4.2 Table / SQL API

#### Path-based Delta Table

```sql
CREATE TEMPORARY TABLE sink (
  id BIGINT,
  dt STRING
) WITH (
  'connector' = 'delta',
  'table_path' = '<path>',
  'partitions' = 'dt',
  'uid' = 'someuid'
);
```

#### Unity Catalog Table

```sql
CREATE TEMPORARY TABLE sink (
  id BIGINT,
  dt STRING
) WITH (
  'connector' = 'delta',
  'table_name' = 'name',
  'unitycatalog.name' = '<catalog-name>',
  'unitycatalog.endpoint' = '<endpoint>',
  'unitycatalog.token' = '<token>',
  'partitions' = 'dt',
  'uid' = 'someuid'
);
```

---

## 5. Configuration

Configurations are divided into:
- **Global configuration** (cluster/job level)
- **Per-table configuration** (table/sink instance level)

---

### 5.1 Global Configuration (`delta-flink.properties`)

Global configurations are loaded from:

```
delta-flink.properties
```

This file must be available on the **classpath**.

#### Available Global Keys

```text
# Retry settings
sink.retry.max_attempt=10
sink.retry.delay_ms=100
sink.retry.max_delay_ms=30000

# Memory / concurrency control
sink.writer.num_concurrent_file=1000
table.thread_pool_size=8

# Table metadata cache
table.cache.enable=true
table.cache.size=200
table.cache.expire_ms=300000

# UC credential refresh
credentials.refresh.thread_pool_size=10
credentials.refresh.ahead_ms=180000        # refresh 3 minutes before expiration
```

#### Notes
- Retry uses exponential backoff:
  - The i-th retry waits for: `delay-ms * (2 ^ i)`
  - Retry stops if delay exceeds `sink.retry.max-delay-ms`
- `sink.writer.num-concurrent-file` limits number of concurrent open files (OOM protection)
- Table cache helps performance for repeated table access
- Credential refresh options help with short-lived credentials (e.g., UC vending)

---

### 5.2 Per-table Configuration

Per-table configurations can be provided via:
- DataStream API `withConfigurations(...)`
- SQL `WITH (...)`

There are two categories of per-table config:

1. **Delta table properties**
  - All keys starting with `delta.` are passed to Delta Kernel and stored in the table.
2. **Sink-only properties**
  - Affect runtime behavior and are **not stored** in Delta table metadata.


| Key                   | Type,   | Default   | Description                                                                                                            |
|-----------------------|---------|-----------|------------------------------------------------------------------------------------------------------------------------|
| checkpoint.frequency  | Double  | 0.0       | Probability [0.0–1.0] to create Delta checkpoint on commit. `0.0` disables checkpoints, `1.0` checkpoints every commit |
| checksum.enable       | Boolean | true      | Generate checksum files on commit                                                                                      |
| file_rolling.strategy | String  | size      | size / count                                                                                                           |
| file_rolling.size     | Integer | 104857600 | Number of bytes per file                                                                                               |
| file_rolling.count    | Integer |           | Max records per file                                                                                                   |
| schema_evolution.mode | String  | no        | no → strict, newcolumn → allow adding new columns                                                                      |

---

### 5.3 Default Delta Properties Provided by the Sink

The sink sets defaults for certain Delta properties, which can be overridden by user configs:

```text
delta.feature.v2Checkpoint = supported
```

---

## 6. Schema Evolution

The Delta sink **does not automatically evolve the table schema**.

Instead:
- It detects schema changes during job execution
- It decides whether the change is allowed based on `schema_evolution.mode`

### Supported Modes

- `NO`
  - Do not allow any schema change
- `NEW_COLUMN`
  - Allow adding new columns only

If an unsupported schema change is detected, the sink will fail the job.

---

## 7. Security & Credentials

### 7.1 Unity Catalog (UC)

When using Unity Catalog:
- Clients must provide a **Personal Access Token (PAT)**
- UC handles **credential vending**
- Temporary credentials are managed/rotated automatically by UC

Typical config keys:
- `unitycatalog.endpoint`
- `unitycatalog.token`

### 7.2 Path-based Access (Without UC)

When using path-based access without UC support:
- Users can configure static S3 credentials in:

```
/opt/flink/conf/core-site.xml
```

Example:

```xml
<configuration>
  <property>
    <name>fs.s3a.access.key</name>
    <value>YOUR_ACCESS_KEY</value>
  </property>

  <property>
    <name>fs.s3a.secret.key</name>
    <value>YOUR_SECRET_KEY</value>
  </property>

  <!-- Optional for some environments -->
  <property>
    <name>fs.s3a.endpoint</name>
    <value>https://s3.amazonaws.com</value>
  </property>
</configuration>
```

---

## 8. Compatibility Matrix (To Be Filled)

Fill this in as compatibility is finalized.

| Flink Version | Delta Kernel Version | Delta Table Version | Unity Catalog Version | Notes |
|--------------|----------------------|---------------------|-----------------------|-------|
| 1.20.x       | ?                    | ?                   | ?                     |       |
| 2.x          | ?                    | ?                   | ?                     |       |

Questions to fill in:
- What is the minimum supported Delta table version?
- Are there differences between UC-managed tables and path-based tables?
- Do we have any known issues with specific Flink patch versions?
- Are there specific Delta Kernel versions required for each Flink major version?

---
## 9. Recommended Tuning for High-Partition Tables

This section provides practical defaults for large, highly-partitioned Delta tables where the sink may need to write to many partitions concurrently.

### Recommended Defaults

#### Limit Concurrent Open Files (OOM Prevention)

The sink uses an internal limit on the number of concurrently opened output files to prevent excessive memory usage when the table has many partitions.

Approximate memory usage observed:
- **1000** concurrently opened files → **~400 MB** memory
- **2000** concurrently opened files → **~1 GB** memory

**Default behavior:**  
The connector uses a conservative default of **1000** concurrent opened files.

**Recommendation:**  
Keep the concurrent file limit near **1000** for high-partition tables unless you have validated that your TaskManagers have sufficient memory headroom.

> Configuration key: `sink.writer.num_concurrent_file` (global config in `delta-flink.properties`)

---

#### File Rolling Configuration (Reduce Small Files)

To reduce small files while keeping file sizes manageable, we recommend enabling file rolling based on record size.

**Recommended rolling setup:**
- Rolling strategy: **record-size**
- Rolling size: **50 MB**

> Configuration keys (per-table config):
- `file_rolling.strategy`
- `file_rolling.size`

Example (SQL):
```sql
WITH (
  'file_rolling.strategy' = 'size',
  'file_rolling.size' = '50MB'
)
```
## 9. Limitations

- Sink-only connector (no source support yet)
- Single global committer
- Requires external AWS SDK bundle for S3 access
