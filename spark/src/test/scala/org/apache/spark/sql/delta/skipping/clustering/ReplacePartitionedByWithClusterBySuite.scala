/*
 * Copyright (2021) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.delta.skipping.clustering

import java.io.File

import org.apache.spark.sql.delta.skipping.ClusteredTableTestUtils
import org.apache.spark.sql.delta.skipping.clustering.temp.{AlterTableReplacePartitionedByWithClusterBy, ClusterBySpec}
import org.apache.spark.sql.delta.{AdaptiveMetadataPreviewTableFeature, DeltaAnalysisException, DeltaConfigs, DeltaLog, DeltaOperations, DeltaUnsupportedOperationException}
import org.apache.spark.sql.delta.actions.{AddFile, TableFeatureProtocolUtils}
import org.apache.spark.sql.delta.commands.ReplacePartitionedByUtils
import org.apache.spark.sql.delta.coordinatedcommits.CatalogOwnedTestBaseSuite
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.stats.DataSkippingDeltaTestsUtils
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.util.JsonUtils

import com.fasterxml.jackson.databind.DeserializationFeature

import org.apache.spark.sql.{AnalysisException, DataFrame, QueryTest, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedTable
import org.apache.spark.sql.streaming.{StreamingQueryException, Trigger}
import org.apache.spark.sql.types.{ByteType, DateType, DecimalType, IntegerType, StringType, StructField, TimestampType}

class ReplacePartitionedByWithClusterBySuite
  extends QueryTest
  with ClusteredTableTestUtils
  with DeltaSQLCommandTest
  with DataSkippingDeltaTestsUtils {

  private val opName = "REPLACE PARTITIONED BY WITH CLUSTER BY"

  private def createPartitionedTable(path: String, partitionColumns: String*): Unit = {
    spark.range(0, 100, 1, 4)
      .selectExpr("id", "cast(id % 5 as string) as p", "cast(id % 3 as int) as q", "id * 2 as v")
      .write.format("delta").partitionBy(partitionColumns: _*).save(path)
  }

  private def readTable(path: String) = spark.read.format("delta").load(path)

  private def statsOf(file: AddFile): Map[String, Any] =
    JsonUtils.fromJson[Map[String, Any]](file.stats)

  test("parser") {
    val parser = spark.sessionState.sqlParser
    val none = parser.parsePlan("ALTER TABLE t REPLACE PARTITIONED BY WITH CLUSTER BY NONE")
    assert(none === AlterTableReplacePartitionedByWithClusterBy(
      UnresolvedTable(Seq("t"), "ALTER TABLE ... REPLACE PARTITIONED BY WITH CLUSTER BY"), None))
    val cols = parser.parsePlan(
      "ALTER TABLE a.t REPLACE PARTITIONED BY WITH CLUSTER BY (c1, s.c2)")
    assert(cols === AlterTableReplacePartitionedByWithClusterBy(
      UnresolvedTable(Seq("a", "t"), "ALTER TABLE ... REPLACE PARTITIONED BY WITH CLUSTER BY"),
      Some(ClusterBySpec(Seq(Seq("c1"), Seq("s", "c2"))))))
    val partitionColumns = parser.parsePlan("ALTER TABLE t REPLACE PARTITIONED BY WITH CLUSTER BY")
    assert(partitionColumns === AlterTableReplacePartitionedByWithClusterBy(
      UnresolvedTable(Seq("t"), "ALTER TABLE ... REPLACE PARTITIONED BY WITH CLUSTER BY"), None,
      clusterByPartitionColumns = true))
    // `with` is not reserved.
    parser.parsePlan("VACUUM with")
  }

  test("CLUSTER BY NONE drops the partitioning without rewriting data files") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      createPartitionedTable(path, "p", "q")
      val deltaLog = DeltaLog.forTable(spark, path)
      val before = deltaLog.update()
      val expected = readTable(path).collect()

      sql(s"ALTER TABLE delta.`$path` REPLACE PARTITIONED BY WITH CLUSTER BY NONE")

      val after = deltaLog.update()
      assert(after.version === before.version + 1)
      assert(after.metadata.partitionColumns.isEmpty)
      assert(after.schema === before.schema)
      // No new table features, the table is a plain unpartitioned table.
      assert(after.protocol === before.protocol)
      assert(!ClusteredTableUtils.isSupported(after.protocol))
      assert(after.allFiles.collect().map(_.path).toSet ===
        before.allFiles.collect().map(_.path).toSet)
      assert(after.allFiles.collect().forall(_.partitionValues.isEmpty))
      // All columns of a narrow table have statistics without listing them.
      assert(statsColumnsOf(deltaLog).isEmpty)
      checkAnswer(readTable(path), expected)

      val lastCommit = deltaLog.history.getHistory(Some(1)).head
      assert(lastCommit.operation === opName)
      assert(lastCommit.operationParameters("oldPartitioningColumns") === "p,q")
      assert(lastCommit.operationParameters("newClusteringColumns") === "")

      val changes = deltaLog.getChanges(after.version).next()._2
      assert(changes.collect { case a: AddFile => a }.forall(!_.dataChange))

      // Time travel to the partitioned version still works.
      checkAnswer(
        spark.read.format("delta").option("versionAsOf", before.version).load(path), expected)
    }
  }

  test("statistics are synthesized for the former partition columns") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      createPartitionedTable(path, "p", "q")
      sql(s"ALTER TABLE delta.`$path` REPLACE PARTITIONED BY WITH CLUSTER BY NONE")
      val deltaLog = DeltaLog.forTable(spark, path)
      deltaLog.update().allFiles.collect().foreach { f =>
        val stats = statsOf(f)
        val p = stats("minValues").asInstanceOf[Map[String, Any]]("p")
        assert(stats("maxValues").asInstanceOf[Map[String, Any]]("p") === p)
        assert(stats("nullCount").asInstanceOf[Map[String, Any]]("p") === 0)
        assert(stats("minValues").asInstanceOf[Map[String, Any]].contains("q"))
        assert(stats("minValues").asInstanceOf[Map[String, Any]].contains("id"))
      }
      // Each former partition value is in its own set of files, which can be skipped.
      val numFiles = deltaLog.update().numOfFiles
      val numFilesForP3 = filesRead(spark, deltaLog, "p = '3'", checkEmptyUnusedFilters = false)
      assert(numFilesForP3 > 0 && numFilesForP3 < numFiles)
      checkAnswer(
        readTable(path).where("p = '3'").selectExpr("count(*)"), Row(20L))
      val numFilesForQ1 = filesRead(spark, deltaLog, "q = 1", checkEmptyUnusedFilters = false)
      assert(numFilesForQ1 > 0 && numFilesForQ1 < numFiles)
    }
  }

  test("writes, DML and OPTIMIZE after dropping the partitioning") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      createPartitionedTable(path, "p")
      sql(s"ALTER TABLE delta.`$path` REPLACE PARTITIONED BY WITH CLUSTER BY NONE")
      val deltaLog = DeltaLog.forTable(spark, path)
      val filesBefore = deltaLog.update().allFiles.collect().map(_.path).toSet

      spark.range(100, 110).selectExpr("id", "'9' as p", "cast(id % 3 as int) as q", "id * 2 as v")
        .write.format("delta").mode("append").save(path)
      val newFiles = deltaLog.update().allFiles.collect().filterNot(f => filesBefore(f.path))
      assert(newFiles.nonEmpty)
      // New files are written at the root of the table.
      assert(newFiles.forall(f => !f.path.contains("/") && f.partitionValues.isEmpty))

      sql(s"UPDATE delta.`$path` SET v = -1 WHERE p = '1'")
      sql(s"DELETE FROM delta.`$path` WHERE p = '2'")
      sql(s"""MERGE INTO delta.`$path` t USING (SELECT 0L AS id, 'x' AS p) s ON t.id = s.id
             |WHEN MATCHED THEN UPDATE SET t.p = s.p""".stripMargin)
      checkAnswer(readTable(path).where("p = '1'").selectExpr("count(*)", "max(v)"),
        Row(20L, -1L))
      checkAnswer(readTable(path).where("p = '2'").selectExpr("count(*)"), Row(0L))
      checkAnswer(readTable(path).where("id = 0").select("p"), Row("x"))
      checkAnswer(readTable(path).selectExpr("count(*)"), Row(90L))

      sql(s"OPTIMIZE delta.`$path`")
      checkAnswer(readTable(path).selectExpr("count(*)"), Row(90L))
      assert(deltaLog.update().metadata.partitionColumns.isEmpty)
    }
  }

  test("CLUSTER BY columns converts a partitioned table into a clustered table") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      createPartitionedTable(path, "p")
      val expected = readTable(path).collect()
      sql(s"ALTER TABLE delta.`$path` REPLACE PARTITIONED BY WITH CLUSTER BY (p, id)")
      val deltaLog = DeltaLog.forTable(spark, path)
      val snapshot = deltaLog.update()
      assert(snapshot.metadata.partitionColumns.isEmpty)
      assert(ClusteredTableUtils.isSupported(snapshot.protocol))
      verifyClusteringColumnsInDomainMetadata(snapshot, Seq("p", "id"))
      checkAnswer(readTable(path), expected)
      val lastCommit = deltaLog.history.getHistory(Some(1)).head
      assert(lastCommit.operation === opName)
      assert(lastCommit.operationParameters("newClusteringColumns") === "p,id")

      // The table can be clustered and further altered like any clustered table.
      sql(s"OPTIMIZE delta.`$path`")
      checkAnswer(readTable(path), expected)
      sql(s"ALTER TABLE delta.`$path` CLUSTER BY (id)")
      verifyClusteringColumnsInDomainMetadata(deltaLog.update(), Seq("id"))
    }
  }

  test("CLUSTER BY without columns clusters the table by its partition columns") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      createPartitionedTable(path, "p", "q")
      val deltaLog = DeltaLog.forTable(spark, path)
      val filesBefore = deltaLog.update().allFiles.collect().map(_.path).toSet
      val expected = readTable(path).collect()

      sql(s"ALTER TABLE delta.`$path` REPLACE PARTITIONED BY WITH CLUSTER BY")

      val snapshot = deltaLog.update()
      assert(snapshot.metadata.partitionColumns.isEmpty)
      assert(ClusteredTableUtils.isSupported(snapshot.protocol))
      verifyClusteringColumnsInDomainMetadata(snapshot, Seq("p", "q"))
      assert(snapshot.allFiles.collect().map(_.path).toSet === filesBefore)
      checkAnswer(readTable(path), expected)
      val lastCommit = deltaLog.history.getHistory(Some(1)).head
      assert(lastCommit.operation === opName)
      assert(lastCommit.operationParameters("oldPartitioningColumns") === "p,q")
      assert(lastCommit.operationParameters("newClusteringColumns") === "p,q")
    }
  }

  test("CLUSTER BY without columns fails with more partition columns than clustering columns") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      spark.range(0, 10)
        .selectExpr("id", "id % 2 as a", "id % 3 as b", "id % 4 as c", "id % 5 as d", "id % 6 as e")
        .write.format("delta").partitionBy("a", "b", "c", "d", "e").save(path)
      val deltaLog = DeltaLog.forTable(spark, path)
      val versionBefore = deltaLog.update().version
      checkError(
        exception = intercept[DeltaAnalysisException] {
          sql(s"ALTER TABLE delta.`$path` REPLACE PARTITIONED BY WITH CLUSTER BY")
        },
        condition = "DELTA_CLUSTER_BY_INVALID_NUM_COLUMNS",
        parameters = Map("numColumnsLimit" -> "4", "actualNumColumns" -> "5"))
      assert(deltaLog.update().version === versionBefore)
      // The clustering columns can be given explicitly instead.
      sql(s"ALTER TABLE delta.`$path` REPLACE PARTITIONED BY WITH CLUSTER BY (a, b)")
      verifyClusteringColumnsInDomainMetadata(deltaLog.update(), Seq("a", "b"))
    }
  }

  test("works with catalog tables") {
    withTable("tbl") {
      sql("CREATE TABLE tbl (id BIGINT, p STRING) USING delta PARTITIONED BY (p)")
      sql("INSERT INTO tbl VALUES (1, 'a'), (2, 'b'), (3, null)")
      sql("ALTER TABLE tbl REPLACE PARTITIONED BY WITH CLUSTER BY NONE")
      checkAnswer(sql("SELECT * FROM tbl"), Seq(Row(1L, "a"), Row(2L, "b"), Row(3L, null)))
      checkAnswer(sql("SELECT id FROM tbl WHERE p IS NULL"), Row(3L))
      val detail = sql("DESCRIBE DETAIL tbl").select("partitionColumns").head()
      assert(detail.getSeq[String](0).isEmpty)
    }
  }

  test("works with column mapping") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      sql(s"""CREATE TABLE delta.`$path` (id BIGINT, p STRING) USING delta PARTITIONED BY (p)
             |TBLPROPERTIES ('delta.columnMapping.mode' = 'name')""".stripMargin)
      sql(s"INSERT INTO delta.`$path` VALUES (1, 'a'), (2, 'b')")
      sql(s"ALTER TABLE delta.`$path` RENAME COLUMN p TO p2")
      sql(s"ALTER TABLE delta.`$path` REPLACE PARTITIONED BY WITH CLUSTER BY NONE")
      checkAnswer(readTable(path), Seq(Row(1L, "a"), Row(2L, "b")))
      checkAnswer(readTable(path).where("p2 = 'b'").select("id"), Row(2L))
      val deltaLog = DeltaLog.forTable(spark, path)
      assert(filesRead(spark, deltaLog, "p2 = 'b'", checkEmptyUnusedFilters = false) === 1)
    }
  }

  test("works with deletion vectors and row tracking") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      withSQLConf(
          "spark.databricks.delta.properties.defaults.enableDeletionVectors" -> "true",
          "spark.databricks.delta.properties.defaults.enableRowTracking" -> "true") {
        createPartitionedTable(path, "p")
      }
      sql(s"DELETE FROM delta.`$path` WHERE id % 10 = 0")
      val deltaLog = DeltaLog.forTable(spark, path)
      val before = deltaLog.update()
      assert(before.allFiles.collect().exists(_.deletionVector != null))
      val expected = readTable(path).selectExpr("*", "_metadata.row_id").collect()

      sql(s"ALTER TABLE delta.`$path` REPLACE PARTITIONED BY WITH CLUSTER BY NONE")

      val after = deltaLog.update()
      val beforeByPath = before.allFiles.collect().map(f => f.path -> f).toMap
      after.allFiles.collect().foreach { f =>
        val old = beforeByPath(f.path)
        assert(f.deletionVector === old.deletionVector)
        assert(f.baseRowId === old.baseRowId)
        assert(f.defaultRowCommitVersion === old.defaultRowCommitVersion)
      }
      checkAnswer(readTable(path).selectExpr("*", "_metadata.row_id"), expected)
    }
  }

  test("fails on an unpartitioned table") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      createPartitionedTable(path)
      val e = intercept[DeltaAnalysisException] {
        sql(s"ALTER TABLE delta.`$path` REPLACE PARTITIONED BY WITH CLUSTER BY NONE")
      }
      assert(e.getErrorClass === "DELTA_REPLACE_PARTITIONED_BY_ON_UNPARTITIONED_TABLE")
    }
  }

  test("fails when data files do not store the partition columns") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      sql(s"""CREATE TABLE delta.`$path` (id BIGINT, p STRING) USING delta PARTITIONED BY (p)
             |TBLPROPERTIES ('delta.writePartitionColumnsToParquet' = 'false')""".stripMargin)
      sql(s"INSERT INTO delta.`$path` VALUES (1, 'a'), (2, 'b')")
      val deltaLog = DeltaLog.forTable(spark, path)
      val versionBefore = deltaLog.update().version
      val e = intercept[DeltaAnalysisException] {
        sql(s"ALTER TABLE delta.`$path` REPLACE PARTITIONED BY WITH CLUSTER BY NONE")
      }
      assert(e.getErrorClass ===
        "DELTA_REPLACE_PARTITIONED_BY_PARTITION_COLUMNS_NOT_MATERIALIZED")
      assert(e.getMessage.contains("2 data file(s)"))
      assert(deltaLog.update().version === versionBefore)
      assert(deltaLog.update().metadata.partitionColumns === Seq("p"))
    }
  }

  test("fails on a table with IcebergCompatV2") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      sql(s"""CREATE TABLE delta.`$path` (id BIGINT, p STRING) USING delta PARTITIONED BY (p)
             |TBLPROPERTIES ('delta.columnMapping.mode' = 'name',
             |  'delta.enableIcebergCompatV2' = 'true')""".stripMargin)
      sql(s"INSERT INTO delta.`$path` VALUES (1, 'a')")
      val deltaLog = DeltaLog.forTable(spark, path)
      val versionBefore = deltaLog.update().version
      checkError(
        exception = intercept[DeltaUnsupportedOperationException] {
          sql(s"ALTER TABLE delta.`$path` REPLACE PARTITIONED BY WITH CLUSTER BY NONE")
        },
        condition = "DELTA_REPLACE_PARTITIONED_BY_UNSUPPORTED_TABLE_FEATURE",
        parameters = Map("feature" -> "delta.enableIcebergCompatV2"))
      assert(deltaLog.update().version === versionBefore)
    }
  }

  test("fails on a non-existing clustering column") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      createPartitionedTable(path, "p")
      checkError(
        exception = intercept[AnalysisException] {
          sql(s"ALTER TABLE delta.`$path` REPLACE PARTITIONED BY WITH CLUSTER BY (missing)")
        },
        condition = "DELTA_COLUMN_NOT_FOUND_IN_SCHEMA",
        matchPVals = true,
        parameters = Map("columnName" -> "missing", "tableSchema" -> "(?s).*"))
      assert(DeltaLog.forTable(spark, path).update().metadata.partitionColumns === Seq("p"))
    }
  }

  test("addPartitionValuesToStats") {
    val fields = Seq(
      StructField("s", StringType), StructField("i", IntegerType),
      StructField("d", DateType), StructField("t", TimestampType), StructField("n", StringType),
      StructField("dec", DecimalType(10, 2)), StructField("e", StringType),
      StructField("ei", IntegerType), StructField("x", IntegerType), StructField("b", ByteType))
    val stats =
      """{"numRecords":3,"minValues":{"id":1},"maxValues":{"id":3},"nullCount":{"id":0}}"""
    val values = Map("s" -> "abc", "i" -> " 7", "d" -> "2024-01-02",
      "t" -> "2024-01-02 03:04:05", "n" -> null, "dec" -> "1.5",
      // Empty strings and values that cannot be cast to the column type get no statistics.
      "e" -> "", "ei" -> "", "x" -> "abc", "b" -> "300")
    def addStats(stats: String, values: Map[String, String], fields: Seq[StructField]): String =
      ReplacePartitionedByUtils.addPartitionValuesToStats(stats, values, fields, 32, "UTC")
    val result = JsonUtils.fromJson[Map[String, Map[String, Any]]](
      addStats(stats, values, fields).replace("\"numRecords\":3,", ""))
    val expectedMinMax = Map("s" -> "abc", "i" -> 7, "d" -> "2024-01-02", "dec" -> 1.5)
    assert(result("minValues") === Map("id" -> 1) ++ expectedMinMax)
    assert(result("maxValues") === Map("id" -> 3) ++ expectedMinMax)
    assert(result("nullCount") ===
      Map("id" -> 0, "s" -> 0, "i" -> 0, "d" -> 0, "t" -> 0, "n" -> 3, "dec" -> 0))
    // Long strings are not added since collected string stats would be truncated.
    assert(!addStats(stats, Map("s" -> ("x" * 33)), fields.take(1)).contains("xxx"))
    // Files without stats are left untouched.
    assert(addStats(null, values, fields) === null)
    assert(addStats("", values, fields) === "")
  }

  test("an empty string partition value gets no statistics") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      sql(s"CREATE TABLE delta.`$path` (id BIGINT, p STRING) USING delta PARTITIONED BY (p)")
      sql(s"INSERT INTO delta.`$path` VALUES (1, 'a')")
      // Delta writes null partition values as null, but the protocol also allows an empty
      // string. Add such a file, which stores a null in the data.
      val manualDir = new File(dir, "manual")
      spark.range(2, 3).selectExpr("id", "cast(null as string) as p").coalesce(1)
        .write.parquet(manualDir.getCanonicalPath)
      val dataFile = manualDir.listFiles().filter(_.getName.endsWith(".parquet")).head
      val deltaLog = DeltaLog.forTable(spark, path)
      deltaLog.startTransaction().commit(
        Seq(AddFile(
          path = s"manual/${dataFile.getName}",
          partitionValues = Map("p" -> ""),
          size = dataFile.length(),
          modificationTime = dataFile.lastModified(),
          dataChange = true,
          stats = """{"numRecords":1}""")),
        DeltaOperations.ManualUpdate)

      sql(s"ALTER TABLE delta.`$path` REPLACE PARTITIONED BY WITH CLUSTER BY NONE")

      val file = deltaLog.update().allFiles.collect().find(_.path.startsWith("manual/")).get
      assert(!statsOf(file)("nullCount").asInstanceOf[Map[String, Any]].contains("p"))
      assert(!statsOf(file)("minValues").asInstanceOf[Map[String, Any]].contains("p"))
      // The file is not skipped.
      checkAnswer(readTable(path).where("p IS NULL").select("id"), Row(2L))
    }
  }

  private def statsColumnsOf(deltaLog: DeltaLog): Option[String] =
    deltaLog.update().metadata.configuration.get(DeltaConfigs.DATA_SKIPPING_STATS_COLUMNS.key)

  test("keeps the statistics of a wide table") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      // 40 data columns. Statistics are collected on the first 32 data columns, c0 to c31.
      // Partition column p is before c31, q is after it.
      val dataColumns = (0 until 40).map(i => s"id + $i as c$i")
      val columns = dataColumns.take(5) ++ Seq("cast(id % 5 as string) as p") ++
        dataColumns.drop(5) ++ Seq("cast(id % 2 as int) as q")
      spark.range(0, 100, 1, 4).selectExpr(columns: _*)
        .write.format("delta").partitionBy("p", "q").save(path)
      val deltaLog = DeltaLog.forTable(spark, path)
      assert(statsColumnsOf(deltaLog).isEmpty)
      val expected = readTable(path).collect()

      // Clustering by q requires statistics on q.
      sql(s"ALTER TABLE delta.`$path` REPLACE PARTITIONED BY WITH CLUSTER BY (q)")

      val indexedColumns = (0 until 32).map(i => s"c$i") ++ Seq("p", "q")
      assert(statsColumnsOf(deltaLog) === Some(indexedColumns.mkString(",")))
      checkAnswer(readTable(path), expected)
      val numFiles = deltaLog.update().numOfFiles
      for (predicate <- Seq("p = '3'", "q = 1", "c31 = 31")) {
        assert(filesRead(spark, deltaLog, predicate, checkEmptyUnusedFilters = false) < numFiles,
          predicate)
      }

      // New files have statistics on the same columns.
      val filesBefore = deltaLog.update().allFiles.collect().map(_.path).toSet
      spark.range(100, 110).selectExpr(columns: _*)
        .write.format("delta").mode("append").save(path)
      val newFiles = deltaLog.update().allFiles.collect().filterNot(f => filesBefore(f.path))
      assert(newFiles.nonEmpty)
      newFiles.foreach { f =>
        assert(statsOf(f)("minValues").asInstanceOf[Map[String, Any]].keySet ===
          indexedColumns.toSet)
      }
    }
  }

  test("adds the former partition columns to delta.dataSkippingStatsColumns") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      sql(s"""CREATE TABLE delta.`$path` (id BIGINT, v BIGINT, p STRING, flag BOOLEAN)
             |USING delta PARTITIONED BY (p, flag)
             |TBLPROPERTIES ('delta.dataSkippingStatsColumns' = 'id')""".stripMargin)
      sql(s"INSERT INTO delta.`$path` VALUES (1, 1, 'a', true), (2, 2, 'b', false)")
      val deltaLog = DeltaLog.forTable(spark, path)

      sql(s"ALTER TABLE delta.`$path` REPLACE PARTITIONED BY WITH CLUSTER BY NONE")

      // Boolean columns cannot be listed.
      assert(statsColumnsOf(deltaLog) === Some("id,p"))
      assert(filesRead(spark, deltaLog, "p = 'b'", checkEmptyUnusedFilters = false) === 1)
      checkAnswer(readTable(path).where("p = 'b'").select("id"), Row(2L))
    }
  }

  test("streaming queries across the conversion") {
    withTempDir { dir =>
      val path = new File(dir, "source").getCanonicalPath
      createPartitionedTable(path, "p")
      def runStream(name: String, schemaTracking: Boolean): DataFrame = {
        val checkpoint = new File(dir, s"checkpoint-$name").getCanonicalPath
        val sinkPath = new File(dir, s"sink-$name").getCanonicalPath
        val reader = spark.readStream.format("delta")
        val source =
          if (schemaTracking) reader.option("schemaTrackingLocation", checkpoint) else reader
        source.load(path).writeStream.format("delta")
          .option("checkpointLocation", checkpoint)
          .trigger(Trigger.AvailableNow())
          .start(sinkPath)
          .awaitTermination()
        spark.read.format("delta").load(sinkPath)
      }
      def assertStreamFails(name: String, schemaTracking: Boolean, errorClass: String): Unit = {
        val e = intercept[StreamingQueryException] { runStream(name, schemaTracking) }
        assert(e.getMessage.contains(errorClass))
      }
      runStream("plain", schemaTracking = false)
      runStream("tracking", schemaTracking = true)

      sql(s"ALTER TABLE delta.`$path` REPLACE PARTITIONED BY WITH CLUSTER BY NONE")
      spark.range(100, 110).selectExpr("id", "'9' as p", "cast(id % 3 as int) as q", "id * 2 as v")
        .write.format("delta").mode("append").save(path)
      val expected = readTable(path).collect()
      assert(expected.length === 110)

      // As for any change of the partitioning, a stream fails at the conversion, because the
      // files added after it cannot be read with the partitioning the stream started with.
      // Without schema tracking, the stream keeps failing when it is restarted.
      for (_ <- 1 to 2) {
        assertStreamFails("plain", schemaTracking = false, "DELTA_SCHEMA_CHANGED_WITH_VERSION")
      }
      // With schema tracking, the stream records the new metadata and continues after a restart.
      assertStreamFails("tracking", schemaTracking = true, "DELTA_STREAMING_METADATA_EVOLUTION")
      checkAnswer(runStream("tracking", schemaTracking = true), expected)
      // A new stream reads the converted table.
      checkAnswer(runStream("new", schemaTracking = false), expected)
    }
  }

  test("change data feed across the conversion") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      withSQLConf(DeltaConfigs.CHANGE_DATA_FEED.defaultTablePropertyKey -> "true") {
        createPartitionedTable(path, "p")
      }
      val deltaLog = DeltaLog.forTable(spark, path)
      sql(s"ALTER TABLE delta.`$path` REPLACE PARTITIONED BY WITH CLUSTER BY NONE")
      val conversionVersion = deltaLog.update().version
      sql(s"DELETE FROM delta.`$path` WHERE p = '1' AND id < 50")
      sql(s"INSERT INTO delta.`$path` VALUES (100, '9', 0, 200)")
      val lastVersion = deltaLog.update().version
      def changes(startingVersion: Long): DataFrame = spark.read.format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", startingVersion)
        .load(path)
        .select("id", "p", "_change_type")

      // The conversion commit has no change rows. Rows deleted after it, from files written
      // before it, have the values of the former partition column.
      checkAnswer(
        changes(conversionVersion),
        (1L until 50L by 5L).map(Row(_, "1", "delete")) :+ Row(100L, "9", "insert"))

      // A read that starts before the conversion is blocked, as for any change of the
      // partitioning of a non-empty table.
      checkError(
        exception = intercept[DeltaUnsupportedOperationException] {
          changes(0).collect()
        },
        condition = "DELTA_CHANGE_DATA_FEED_INCOMPATIBLE_SCHEMA_CHANGE",
        sqlState = Some("0AKDC"),
        matchPVals = true,
        parameters = Map(
          "start" -> "0",
          "end" -> lastVersion.toString,
          "readSchema" -> ".*",
          "readVersion" -> lastVersion.toString,
          "incompatibleVersion" -> "0"))
    }
  }

  test("keeps the precision of decimal statistics") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      sql(s"""CREATE TABLE delta.`$path` (id BIGINT, amt DECIMAL(38, 18), p STRING)
             |USING delta PARTITIONED BY (p)""".stripMargin)
      sql(s"""INSERT INTO delta.`$path` VALUES
             |(1, 1234567.123456789012345678, 'a'),
             |(2, 1234567.123456789012345679, 'a')""".stripMargin)
      val deltaLog = DeltaLog.forTable(spark, path)
      // Reads the statistics without rounding decimals through Double.
      val reader = JsonUtils.mapper.reader(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
      def amtStats(): Map[String, (BigDecimal, BigDecimal)] =
        deltaLog.update().allFiles.collect().map { f =>
          val stats = reader.readTree(f.stats)
          f.path -> (BigDecimal(stats.get("minValues").get("amt").decimalValue),
            BigDecimal(stats.get("maxValues").get("amt").decimalValue))
        }.toMap
      val statsBefore = amtStats()
      assert(statsBefore.values.map(_._1).min === BigDecimal("1234567.123456789012345678"))
      assert(statsBefore.values.map(_._2).max === BigDecimal("1234567.123456789012345679"))

      sql(s"ALTER TABLE delta.`$path` REPLACE PARTITIONED BY WITH CLUSTER BY NONE")

      assert(amtStats() === statsBefore)
      checkAnswer(
        readTable(path).where("amt = 1234567.123456789012345678").select("id"), Row(1L))
      checkAnswer(
        sql(s"SELECT MIN(amt), MAX(amt) FROM delta.`$path`"),
        Row(BigDecimal("1234567.123456789012345678"), BigDecimal("1234567.123456789012345679")))
    }
  }
}

/** Adaptive metadata tables must be catalog-managed, so they need a commit coordinator. */
class ReplacePartitionedByAdaptiveMetadataSuite
  extends QueryTest
  with CatalogOwnedTestBaseSuite
  with DeltaSQLCommandTest {

  override def catalogOwnedCoordinatorBackfillBatchSize: Option[Int] = Some(3)

  test("fails on a table with adaptive metadata") {
    val table = "replace_partitioned_by_amt"
    withTable(table) {
      sql(s"""CREATE TABLE $table (id BIGINT, p STRING) USING delta PARTITIONED BY (p)
             |TBLPROPERTIES (
             |  '${TableFeatureProtocolUtils.propertyKey(AdaptiveMetadataPreviewTableFeature)}' =
             |    'supported',
             |  'delta.columnMapping.mode' = 'id')""".stripMargin)
      sql(s"INSERT INTO $table VALUES (1, 'a')")
      val deltaLog = DeltaLog.forTable(spark, TableIdentifier(table))
      val versionBefore = deltaLog.update().version
      checkError(
        exception = intercept[DeltaUnsupportedOperationException] {
          sql(s"ALTER TABLE $table REPLACE PARTITIONED BY WITH CLUSTER BY NONE")
        },
        condition = "DELTA_REPLACE_PARTITIONED_BY_UNSUPPORTED_TABLE_FEATURE",
        parameters = Map("feature" -> AdaptiveMetadataPreviewTableFeature.name))
      assert(deltaLog.update().version === versionBefore)
      assert(deltaLog.update().metadata.partitionColumns === Seq("p"))
    }
  }
}
