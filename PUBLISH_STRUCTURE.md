# Delta Spark 发布结构说明

## publishM2 会发布哪些 JAR？

### 发布的模块（使用 releaseSettings）

只有 **1 个** delta-spark 相关的 jar 会被发布：

#### 1. delta-spark.jar
- **SBT 模块**: `spark`
- **Maven Artifact**: `delta-spark_2.12` (或 `delta-spark_2.13`)
- **内容**: 
  - delta-spark-v1 的所有 classes（来自 `spark/src/main/`）
  - delta-spark-v2 的所有 classes（来自 `kernel-spark/src/main/`）
  - delta-spark-shaded 的所有 classes（来自 `spark-shaded/src/main/`）
  - Python 文件（从 `python/` 目录）
- **发布配置**: `releaseSettings` → `publishArtifact := true`

### 不发布的模块（使用 skipReleaseSettings）

以下 3 个模块 **不会** 单独发布 jar：

#### 1. delta-spark-v1
- **配置**: `skipReleaseSettings` → `publishArtifact := false`
- **原因**: 内部模块，其 classes 会被打包到最终的 `delta-spark.jar` 中

#### 2. delta-spark-v2  
- **配置**: `skipReleaseSettings` → `publishArtifact := false`
- **原因**: 内部模块，其 classes 会被打包到最终的 `delta-spark.jar` 中

#### 3. delta-spark-shaded
- **配置**: `skipReleaseSettings` → `publishArtifact := false`
- **原因**: 内部模块，其 classes 会被打包到最终的 `delta-spark.jar` 中

## delta-spark.jar 包含的内容

最终发布的 `delta-spark.jar` 通过以下配置组合所有内容：

```scala
Compile / packageBin / mappings := {
  val v1 = (`delta-spark-v1` / Compile / packageBin / mappings).value
  val v2 = (`delta-spark-v2` / Compile / packageBin / mappings).value
  val shaded = (`delta-spark-shaded` / Compile / packageBin / mappings).value
  v1 ++ v2 ++ shaded
}
```

### 详细内容列表

#### 来自 delta-spark-v1 (`spark/src/main/`)
- `org.apache.spark.sql.delta.*` - Delta Lake 核心功能
- `io.delta.sql.*` - Delta SQL 扩展
- `io.delta.tables.*` - Delta Tables API
- `io.delta.dynamodbcommitcoordinator.*` - DynamoDB 协调器
- ANTLR 生成的 parser 类
- Python 文件（`delta/*.py`）
- `META-INF/services/*` - 服务注册文件

#### 来自 delta-spark-v2 (`kernel-spark/src/main/`)
- `io.delta.kernel.spark.*` - Kernel-based Spark connector
  - `catalog.SparkTable` - DSv2 Table 实现
  - `read.*` - 读取相关类（Scan, Batch, PartitionReader）
  - `utils.*` - 工具类（Schema, Expression, Serialization）
- `io.delta.sql.DeltaSparkSessionExtension` - V2 扩展
- `org.apache.spark.sql.delta.catalog.DeltaCatalog` - V2 Catalog

#### 来自 delta-spark-shaded (`spark-shaded/src/main/`)
- Delegation 代码（如果添加）
  - 例如：统一的 `DeltaCatalog` 入口，根据配置选择 V1 或 V2
  - 例如：`DeltaSparkSessionExtension` 可以同时注册 V1 和 V2

## 发布命令

```bash
# 发布到本地 Maven 仓库
sbt publishM2

# 只发布 delta-spark 模块
sbt spark/publishM2

# 发布到 Sonatype
sbt publishSigned
```

## Maven 依赖示例

用户只需要依赖一个 jar：

```xml
<dependency>
    <groupId>io.delta</groupId>
    <artifactId>delta-spark_2.12</artifactId>
    <version>3.4.0-SNAPSHOT</version>
</dependency>
```

这一个 jar 就包含了 V1、V2 和 delegation 层的所有功能。

## 其他会发布的 Delta 相关模块

除了 `delta-spark.jar`，以下模块也会被 publishM2：

1. **delta-kernel-api.jar** - Kernel API
2. **delta-kernel-defaults.jar** - Kernel Defaults 实现
3. **delta-storage.jar** - Storage 层
4. **delta-storage-s3-dynamodb.jar** - S3/DynamoDB 存储
5. **delta-iceberg.jar** - Iceberg 集成
6. **delta-hudi.jar** - Hudi 集成
7. **delta-sharing-spark.jar** - Delta Sharing
8. **delta-contribs.jar** - 贡献模块
9. **delta-connect-*.jar** - Delta Connect 模块
10. **delta-standalone*.jar** - Standalone 连接器
11. **delta-hive*.jar** - Hive 连接器
12. **delta-flink.jar** - Flink 连接器

但这些都是独立的 jar，与 `delta-spark.jar` 分开发布。

## 总结

**回答你的问题**：

1. **publishM2 会生成几个 delta-spark jar？**
   - 只有 **1 个**：`delta-spark_2.12-3.4.0-SNAPSHOT.jar` (约 7.5MB)
   - 位置：`spark-combined/target/scala-2.12/`

2. **delta-spark jar 包含哪些内容？**
   
   **来自 delta-spark-v1** (约 7.4MB)：
   ```
   org/apache/spark/sql/delta/*         - Delta Lake 核心
   io/delta/sql/*                       - SQL 扩展  
   io/delta/tables/*                    - Tables API
   io/delta/dynamodbcommitcoordinator/* - DynamoDB
   delta/*.py                           - Python 文件
   META-INF/services/*                  - 服务注册
   ```
   
   **来自 delta-spark-v2** (约 34KB)：
   ```
   io/delta/kernel/spark/catalog/*      - DSv2 Catalog
   io/delta/kernel/spark/read/*         - 读取实现
   io/delta/kernel/spark/utils/*        - 工具类
   io/delta/sql/DeltaSparkSessionExtension - V2 扩展
   org/apache/spark/sql/delta/catalog/DeltaCatalog - V2 Catalog
   ```
   
   **来自 delta-spark-shaded** (约 288B)：
   ```
   (delegation 代码，如果添加)
   ```

3. **v1, v2, shaded 三个内部模块会单独发布吗？**
   - **不会**，它们有 `skipReleaseSettings` 配置
   - 它们只是内部模块，用于组织代码
   - 所有代码最终都打包进同一个 `delta-spark.jar`

## 验证

生成的 jar 文件：
```bash
# 内部模块（不发布）
spark/target/scala-2.12/delta-spark-v1_2.12-3.4.0-SNAPSHOT.jar        # 7.4M
kernel-spark/target/scala-2.12/delta-spark-v2_2.12-3.4.0-SNAPSHOT.jar # 34K
spark-shaded/target/scala-2.12/delta-spark-shaded_2.12-3.4.0-SNAPSHOT.jar # 288B

# 最终发布的 jar（组合了上面三个）
spark-combined/target/scala-2.12/delta-spark_2.12-3.4.0-SNAPSHOT.jar    # 7.5M
```

关键类验证：
```bash
# V1 类
org.apache.spark.sql.delta.DeltaLog
org.apache.spark.sql.delta.catalog.DeltaCatalog

# V2 类  
io.delta.kernel.spark.table.SparkTable
io.delta.kernel.spark.read.SparkScan
io.delta.sql.DeltaSparkSessionExtension
```

