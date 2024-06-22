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

package org.apache.spark.sql.delta.deletionvectors

import java.io.{File, FileNotFoundException}
import java.net.URISyntaxException

import org.apache.spark.sql.delta.{DeletionVectorsTableFeature, DeletionVectorsTestUtils, DeltaChecksumException, DeltaConfigs, DeltaExcludedBySparkVersionTestMixinShims, DeltaLog, DeltaMetricsUtils, DeltaTestUtilsForTempViews}
import org.apache.spark.sql.delta.DeltaTestUtils.createTestAddFile
import org.apache.spark.sql.delta.actions.{AddFile, DeletionVectorDescriptor, RemoveFile}
import org.apache.spark.sql.delta.actions.DeletionVectorDescriptor.EMPTY
import org.apache.spark.sql.delta.deletionvectors.DeletionVectorsSuite._
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.{DeltaExceptionTestUtils, DeltaSQLCommandTest}
import org.apache.spark.sql.delta.test.DeltaTestImplicits._
import org.apache.spark.sql.delta.util.JsonUtils
import com.fasterxml.jackson.databind.node.ObjectNode
import io.delta.tables.DeltaTable
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.format.converter.ParquetMetadataConverter
import org.apache.parquet.hadoop.ParquetFileReader

import org.apache.spark.SparkException
import org.apache.spark.sql.{DataFrame, QueryTest, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.plans.logical.{AppendData, Subquery}
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class DeletionVectorsSuite extends QueryTest
  with SharedSparkSession
  with DeltaSQLCommandTest
  with DeletionVectorsTestUtils
  with DeltaTestUtilsForTempViews
  with DeltaExceptionTestUtils
  with DeltaExcludedBySparkVersionTestMixinShims {
  import testImplicits._

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set(DeltaSQLConf.DELETION_VECTORS_USE_METADATA_ROW_INDEX.key, "false")
  }

  protected def hadoopConf(): Configuration = {
    // scalastyle:off hadoopconfiguration
    // This is to generate a Parquet file with two row groups
    spark.sparkContext.hadoopConfiguration
    // scalastyle:on hadoopconfiguration
  }

  test(s"read Delta table with deletion vectors") {
    def verifyVersion(version: Int, expectedData: Seq[Int]): Unit = {
      checkAnswer(
        spark.read.format("delta").option("versionAsOf", version.toString).load(table1Path),
        expectedData.toDF())
    }
    // Verify all versions of the table
    verifyVersion(0, expectedTable1DataV0)
    verifyVersion(1, expectedTable1DataV1)
    verifyVersion(2, expectedTable1DataV2)
    verifyVersion(3, expectedTable1DataV3)
    verifyVersion(4, expectedTable1DataV4)
  }

  test(s"read partitioned Delta table with deletion vectors") {
    def verify(version: Int, expectedData: Seq[Int], filterExp: String = "true"): Unit = {
      val query = spark.read.format("delta")
          .option("versionAsOf", version.toString)
          .load(table3Path)
          .filter(filterExp)
      val expected = expectedData.toDF("id")
          .withColumn("partCol", col("id") % 10)
          .filter(filterExp)

      checkAnswer(query, expected)
    }
    // Verify all versions of the table
    verify(0, expectedTable3DataV0)
    verify(1, expectedTable3DataV1)
    verify(2, expectedTable3DataV2)
    verify(3, expectedTable3DataV3)
    verify(4, expectedTable3DataV4)

    verify(4, expectedTable3DataV4, filterExp = "partCol = 3")
    verify(3, expectedTable3DataV3, filterExp = "partCol = 3 and id > 25")
    verify(1, expectedTable3DataV1, filterExp = "id > 25")
  }

  test("select metadata columns from a Delta table with deletion vectors") {
    assert(spark.read.format("delta").load(table1Path)
      .select("_metadata.file_path").distinct().count() == 22)
  }

  test("throw error when non-pinned TahoeFileIndex snapshot is used") {
    // Corner case where we still have non-pinned TahoeFileIndex when data skipping is disabled
    withSQLConf(DeltaSQLConf.DELTA_STATS_SKIPPING.key -> "false") {
      def assertError(dataFrame: DataFrame): Unit = {
        val ex = intercept[IllegalArgumentException] {
          dataFrame.collect()
        }
        assert(ex.getMessage contains
          "Cannot work with a non-pinned table snapshot of the TahoeFileIndex")
      }
      assertError(spark.read.format("delta").load(table1Path))
      assertError(spark.read.format("delta").option("versionAsOf", "2").load(table1Path))
    }
  }

  test("read Delta table with deletion vectors with a filter") {
    checkAnswer(
      spark.read.format("delta").load(table1Path).where("value in (300, 787, 239)"),
      // 300 is removed in the final table
      Seq(787, 239).toDF())
  }

  test("read Delta table with DV for a select files") {
    val deltaLog = DeltaLog.forTable(spark, table1Path)
    val snapshot = deltaLog.unsafeVolatileSnapshot

    // Select a subset of files with DVs and specific value range, this is just to test
    // that reading these files will respect the DVs
    var rowCount = 0L
    var deletedRowCount = 0L
    val selectFiles = snapshot.allFiles.collect().filter(
      addFile => {
        val stats = JsonUtils.mapper.readTree(addFile.stats).asInstanceOf[ObjectNode]
        // rowCount += stats.get("rowCount")
        val min = stats.get("minValues").get("value").toString
        val max = stats.get("maxValues").get("value").toString
        val selected = (min == "18" && max == "1988") ||
            (min == "33" && max == "1995") || (min == "13" && max == "1897")
        // TODO: these steps will be easier and also change (depending upon tightBounds value) once
        // we expose more methods on AddFile as part of the data skipping changes with DVs
        if (selected) {
          rowCount += stats.get("numRecords").asInt(0)
          deletedRowCount += Option(addFile.deletionVector).getOrElse(EMPTY).cardinality
        }
        selected
      }
      ).toSeq
    assert(selectFiles.filter(_.deletionVector != null).size > 1) // make at least one file has DV

    assert(deltaLog.createDataFrame(snapshot, selectFiles).count() == rowCount - deletedRowCount)
  }

  for (optimizeMetadataQuery <- BOOLEAN_DOMAIN)
    test("read Delta tables with DVs in subqueries: " +
      s"metadataQueryOptimizationEnabled=$optimizeMetadataQuery") {
      withSQLConf(DeltaSQLConf.DELTA_OPTIMIZE_METADATA_QUERY_ENABLED.key ->
        optimizeMetadataQuery.toString) {
        val table1 = s"delta.`${new File(table1Path).getAbsolutePath}`"
        val table2 = s"delta.`${new File(table2Path).getAbsolutePath}`"

        def assertQueryResult(query: String, expected1: Int, expected2: Int): Unit = {
          val df = spark.sql(query)
          assertPlanContains(df, Subquery.getClass.getSimpleName.stripSuffix("$"))
          val actual = df.collect()(0) // fetch only row in the result
          assert(actual === Row(expected1, expected2))
        }

        // same table used twice in the query
        val query1 = s"SELECT (SELECT COUNT(*) FROM $table1), (SELECT COUNT(*) FROM $table1)"
        assertQueryResult(query1, expectedTable1DataV4.size, expectedTable1DataV4.size)

        // two tables used in the query
        val query2 = s"SELECT (SELECT COUNT(*) FROM $table1), (SELECT COUNT(*) FROM $table2)"
        assertQueryResult(query2, expectedTable1DataV4.size, expectedTable2DataV1.size)
      }
    }

  test("insert into Delta table with DVs") {
    withTempDir { tempDir =>
      val source1 = new File(table1Path)
      val source2 = new File(table2Path)
      val target = new File(tempDir, "insertTest")

      // Copy the source2 DV table to a temporary directory
      FileUtils.copyDirectory(source1, target)

      // Insert data from source2 into source1 (copied to target)
      // This blind append generates a plan with `V2WriteCommand` which is a corner
      // case in `PrepareDeltaScan` rule
      val insertDf = spark.sql(s"INSERT INTO TABLE delta.`${target.getAbsolutePath}` " +
        s"SELECT * FROM delta.`${source2.getAbsolutePath}`")
      // [[AppendData]] is one of the [[V2WriteCommand]] subtypes
      assertPlanContains(insertDf, AppendData.getClass.getSimpleName.stripSuffix("$"))

      val dataInTarget = spark.sql(s"SELECT * FROM delta.`${target.getAbsolutePath}`")

      // Make sure the number of rows is correct.
      for (metadataQueryOptimization <- BOOLEAN_DOMAIN) {
        withSQLConf(DeltaSQLConf.DELTA_OPTIMIZE_METADATA_QUERY_ENABLED.key ->
          metadataQueryOptimization.toString) {
          assert(dataInTarget.count() == expectedTable2DataV1.size + expectedTable1DataV4.size)
        }
      }

      // Make sure the contents are the same
      checkAnswer(
        dataInTarget,
        spark.sql(
          s"SELECT * FROM delta.`${source1.getAbsolutePath}` UNION ALL " +
          s"SELECT * FROM delta.`${source2.getAbsolutePath}`")
      )
    }
  }

  test("DELETE with DVs - on a table with no prior DVs") {
    withDeletionVectorsEnabled() {
      withTempDir { dirName =>
        // Create table with 500 files of 2 rows each.
        val numFiles = 500
        val path = dirName.getAbsolutePath
        spark.range(0, 1000, step = 1, numPartitions = numFiles).write.format("delta").save(path)
        val tableName = s"delta.`$path`"

        val log = DeltaLog.forTable(spark, path)
        val beforeDeleteFilesWithStats = log.update().allFiles.collect()
        val beforeDeleteFiles = beforeDeleteFilesWithStats.map(_.path)

        val numFilesWithDVs = 100
        val numDeletedRows = numFilesWithDVs * 1
        spark.sql(s"DELETE FROM $tableName WHERE id % 2 = 0 AND id < 200")

        val snapshotAfterDelete = log.update()
        val afterDeleteFilesWithStats = snapshotAfterDelete.allFiles.collect()
        val afterDeleteFilesWithDVs = afterDeleteFilesWithStats.filter(_.deletionVector != null)
        val afterDeleteFiles = afterDeleteFilesWithStats.map(_.path)

        // Verify the expected no. of deletion vectors and deleted rows according to DV cardinality
        assert(afterDeleteFiles.length === numFiles)
        assert(afterDeleteFilesWithDVs.length === numFilesWithDVs)
        assert(afterDeleteFilesWithDVs.map(_.deletionVector.cardinality).sum == numDeletedRows)

        // Expect all DVs are written in one file
          assert(
          afterDeleteFilesWithDVs
            .map(_.deletionVector.absolutePath(new Path(path)))
            .toSet
            .size === 1)

        // Verify "tightBounds" is false for files that have DVs
        for (f <- afterDeleteFilesWithDVs) {
          assert(f.tightBounds.get === false)
        }

        // Verify all stats are the same except "tightBounds".
        // Drop "tightBounds" and convert the rest to JSON.
        val dropTightBounds: (AddFile => String) =
          _.stats.replaceAll("\"tightBounds\":(false|true)", "")
        val beforeStats = beforeDeleteFilesWithStats.map(dropTightBounds).sorted
        val afterStats = afterDeleteFilesWithStats.map(dropTightBounds).sorted
        assert(beforeStats === afterStats)

        // make sure the data file list is the same
        assert(beforeDeleteFiles === afterDeleteFiles)

        // Contents after the DELETE are as expected
        checkAnswer(
          spark.sql(s"SELECT * FROM $tableName"),
          Seq.range(0, 1000).filterNot(Seq.range(start = 0, end = 200, step = 2).contains(_)).toDF()
        )
      }
    }
  }

  Seq("name", "id").foreach { mode =>
    test(s"DELETE with DVs with column mapping mode=$mode") {
      withSQLConf("spark.databricks.delta.properties.defaults.columnMapping.mode" -> mode) {
        withTempDir { dirName =>
          val path = dirName.getAbsolutePath
          val data = (0 until 50).map(x => (x % 10, x, s"foo${x % 5}"))
          data.toDF("part", "col1", "col2").write.format("delta").partitionBy(
            "part").save(path)
          val tableLog = DeltaLog.forTable(spark, path)
          enableDeletionVectorsInTable(tableLog, true)
          spark.sql(s"DELETE FROM delta.`$path` WHERE col1 = 2")
          checkAnswer(spark.sql(s"select * from delta.`$path` WHERE col1 = 2"), Seq())
          verifyDVsExist(tableLog, 1)
        }
      }
    }

    testSparkMasterOnly(s"variant types DELETE with DVs with column mapping mode=$mode") {
      withSQLConf("spark.databricks.delta.properties.defaults.columnMapping.mode" -> mode) {
        withTempDir { dirName =>
          val path = dirName.getAbsolutePath
          val df = spark.range(0, 50).selectExpr(
            "id % 10 as part",
            "id",
            "parse_json(cast(id as string)) as v"
          )
          df.write.format("delta").partitionBy("part").save(path)
          val tableLog = DeltaLog.forTable(spark, path)
          enableDeletionVectorsInTable(tableLog, true)
          spark.sql(s"DELETE FROM delta.`$path` WHERE v::int = 2")
          checkAnswer(spark.sql(s"select * from delta.`$path` WHERE v::int = 2"), Seq())
          verifyDVsExist(tableLog, 1)
        }
      }
    }
  }

  test("DELETE with DVs - existing table already has DVs") {
    withSQLConf(DeltaSQLConf.DELETE_USE_PERSISTENT_DELETION_VECTORS.key -> "true") {
      withTempDir { tempDir =>
        val source = new File(table1Path)
        val target = new File(tempDir, "deleteTest")

        // Copy the source DV table to a temporary directory
        FileUtils.copyDirectory(source, target)

        val targetPath = s"delta.`${target.getAbsolutePath}`"
        val dataToRemove = Seq(1999, 299, 7, 87, 867, 456)
        val existingDVs = getFilesWithDeletionVectors(DeltaLog.forTable(spark, target))

        spark.sql(s"DELETE FROM $targetPath WHERE value in (${dataToRemove.mkString(",")})")

        // Check new DVs are created
        val newDVs = getFilesWithDeletionVectors(DeltaLog.forTable(spark, target))
        // expect the new DVs contain extra entries for the deleted rows.
        assert(
          existingDVs.map(_.deletionVector.cardinality).sum + dataToRemove.size ===
          newDVs.map(_.deletionVector.cardinality).sum
        )
        for (f <- newDVs) {
          assert(f.tightBounds.get === false)
        }

        // Check the data is valid
        val expectedTable1DataV5 = expectedTable1DataV4.filterNot(e => dataToRemove.contains(e))
        checkAnswer(spark.sql(s"SELECT * FROM $targetPath"), expectedTable1DataV5.toDF())
      }
    }
  }

  test("Metrics when deleting with DV") {
    withDeletionVectorsEnabled() {
      val tableName = "tbl"
      withTable(tableName) {
        spark.range(0, 10, 1, numPartitions = 2)
          .write.format("delta").saveAsTable(tableName)

        {
          // Delete one row from the first file, and the whole second file.
          val result = sql(s"DELETE FROM $tableName WHERE id >= 4")
          assert(result.collect() === Array(Row(6)))
          val opMetrics = DeltaMetricsUtils.getLastOperationMetrics(tableName)
          assert(opMetrics.getOrElse("numDeletedRows", -1) === 6)
          assert(opMetrics.getOrElse("numRemovedFiles", -1) === 1)
          assert(opMetrics.getOrElse("numDeletionVectorsAdded", -1) === 1)
          assert(opMetrics.getOrElse("numDeletionVectorsRemoved", -1) === 0)
          assert(opMetrics.getOrElse("numDeletionVectorsUpdated", -1) === 0)
        }

        {
          // Delete one row again.
          sql(s"DELETE FROM $tableName WHERE id = 3")
          val opMetrics = DeltaMetricsUtils.getLastOperationMetrics(tableName)
          assert(opMetrics.getOrElse("numDeletedRows", -1) === 1)
          assert(opMetrics.getOrElse("numRemovedFiles", -1) === 0)
          val initialNumDVs = 0
          val numDVUpdated = 1
          // An "updated" DV is "deleted" then "added" again.
          // We increment the count for "updated", "added", and "deleted".
          assert(
            opMetrics.getOrElse("numDeletionVectorsAdded", -1) ===
              initialNumDVs + numDVUpdated)
          assert(
            opMetrics.getOrElse("numDeletionVectorsRemoved", -1) ===
              initialNumDVs + numDVUpdated)
          assert(
            opMetrics.getOrElse("numDeletionVectorsUpdated", -1) ===
              numDVUpdated)
        }

        {
          // Delete all renaming rows.
          sql(s"DELETE FROM $tableName WHERE id IN (0, 1, 2)")
          val opMetrics = DeltaMetricsUtils.getLastOperationMetrics(tableName)
          assert(opMetrics.getOrElse("numDeletedRows", -1) === 3)
          assert(opMetrics.getOrElse("numRemovedFiles", -1) === 1)
          assert(opMetrics.getOrElse("numDeletionVectorsAdded", -1) === 0)
          assert(opMetrics.getOrElse("numDeletionVectorsRemoved", -1) === 1)
          assert(opMetrics.getOrElse("numDeletionVectorsUpdated", -1) === 0)
        }
      }
    }
  }

  for(targetDVFileSize <- Seq(2, 200, 2000000)) {
    test(s"DELETE with DVs - packing multiple DVs into one file: target max DV file " +
      s"size=$targetDVFileSize") {
      withSQLConf(
        DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.defaultTablePropertyKey -> "true",
        DeltaSQLConf.DELETE_USE_PERSISTENT_DELETION_VECTORS.key -> "true",
        DeltaSQLConf.DELETION_VECTOR_PACKING_TARGET_SIZE.key -> targetDVFileSize.toString) {
        withTempDir { dirName =>
          // Create table with 100 files of 2 rows each.
          val numFiles = 100
          val path = dirName.getAbsolutePath
          spark.range(0, 200, step = 1, numPartitions = numFiles)
            .write.format("delta").save(path)
          val tableName = s"delta.`$path`"

          val beforeDeleteFiles = DeltaLog.forTable(spark, path)
            .unsafeVolatileSnapshot.allFiles.collect().map(_.path)

          val numFilesWithDVs = 10
          val numDeletedRows = numFilesWithDVs * 1
          spark.sql(s"DELETE FROM $tableName WHERE id % 2 = 0 AND id < 20")

          // Verify the expected number of AddFiles with DVs
          val allFiles = DeltaLog.forTable(spark, path).unsafeVolatileSnapshot.allFiles.collect()
          assert(allFiles.size === numFiles)
          val addFilesWithDV = allFiles.filter(_.deletionVector != null)
          assert(addFilesWithDV.size === numFilesWithDVs)
          assert(addFilesWithDV.map(_.deletionVector.cardinality).sum == numDeletedRows)

          val expectedDVFileCount = targetDVFileSize match {
            // Each AddFile will have its own DV file
            case 2 => numFilesWithDVs
            // Each DV size is about 34bytes according the latest format.
            case 200 => numFilesWithDVs / (200 / 34).floor.toInt
            // Expect all DVs in one file
            case 2000000 => 1
            case default =>
              throw new IllegalStateException(s"Unknown target DV file size: $default")
          }
          // Expect all DVs are written in one file
          assert(
            addFilesWithDV.map(_.deletionVector.absolutePath(new Path(path))).toSet.size ===
            expectedDVFileCount)

          val afterDeleteFiles = allFiles.map(_.path)
          // make sure the data file list is the same
          assert(beforeDeleteFiles === afterDeleteFiles)

          // Contents after the DELETE are as expected
          checkAnswer(
            spark.sql(s"SELECT * FROM $tableName"),
            Seq.range(0, 200).filterNot(
              Seq.range(start = 0, end = 20, step = 2).contains(_)).toDF())
        }
      }
    }
  }

  test("JOIN with DVs - self-join a table with DVs") {
    val tableDf = spark.read.format("delta").load(table2Path)
    val leftDf = tableDf.withColumn("key", col("value") % 2)
    val rightDf = tableDf.withColumn("key", col("value") % 2 + 1)

    checkAnswer(
      leftDf.as("left").join(rightDf.as("right"), "key").drop("key"),
      Seq(1, 3, 5, 7).flatMap(l => Seq(2, 4, 6, 8).map(r => (l, r))).toDF()
    )
  }

  test("JOIN with DVs - non-DV table joins DV table") {
    val tableDf = spark.read.format("delta").load(table2Path)
    val tableDfV0 = spark.read.format("delta").option("versionAsOf", "0").load(table2Path)
    val leftDf = tableDf.withColumn("key", col("value") % 2)
    val rightDf = tableDfV0.withColumn("key", col("value") % 2 + 1)

    // Right has two more rows 0 and 9. 0 will be left in the join result.
    checkAnswer(
      leftDf.as("left").join(rightDf.as("right"), "key").drop("key"),
      Seq(1, 3, 5, 7).flatMap(l => Seq(0, 2, 4, 6, 8).map(r => (l, r))).toDF()
    )
  }

  test("MERGE with DVs - merge into DV table") {
    withTempDir { tempDir =>
      val source = new File(table1Path)
      val target = new File(tempDir, "mergeTest")
      FileUtils.copyDirectory(new File(table2Path), target)

      DeltaTable.forPath(spark, target.getAbsolutePath).as("target")
        .merge(
          spark.read.format("delta").load(source.getAbsolutePath).as("source"),
          "source.value = target.value")
        .whenMatched()
        .updateExpr(Map("value" -> "source.value + 10000"))
        .whenNotMatched()
        .insertExpr(Map("value" -> "source.value"))
        .execute()

      val snapshot = DeltaLog.forTable(spark, target).update()
      val allFiles = snapshot.allFiles.collect()
      val tombstones = snapshot.tombstones.collect()
      // DVs are removed
      for (ts <- tombstones) {
        assert(ts.deletionVector != null)
      }
      // target log should not contain DVs
      for (f <- allFiles) {
        assert(f.deletionVector == null)
        assert(f.tightBounds.get)
      }

      // Target table should contain "table2 records + 10000" and "table1 records \ table2 records".
      checkAnswer(
        spark.read.format("delta").load(target.getAbsolutePath),
        (expectedTable2DataV1.map(_ + 10000) ++
          expectedTable1DataV4.filterNot(expectedTable2DataV1.contains)).toDF()
      )
    }
  }

  test("UPDATE with DVs - update rewrite files with DVs") {
    withTempDir { tempDir =>
      FileUtils.copyDirectory(new File(table2Path), tempDir)
      val deltaLog = DeltaLog.forTable(spark, tempDir)

      DeltaTable.forPath(spark, tempDir.getAbsolutePath)
        .update(col("value") === 1, Map("value" -> (col("value") + 1)))

      val snapshot = deltaLog.update()
      val allFiles = snapshot.allFiles.collect()
      val tombstones = snapshot.tombstones.collect()
      // DVs are removed
      for (ts <- tombstones) {
        assert(ts.deletionVector != null)
      }
      // target log should contain two files, one with and one without DV
      assert(allFiles.count(_.deletionVector != null) === 1)
      assert(allFiles.count(_.deletionVector == null) === 1)
    }
  }

  test("UPDATE with DVs - update deleted rows updates nothing") {
    withTempDir { tempDir =>
      FileUtils.copyDirectory(new File(table2Path), tempDir)
      val deltaLog = DeltaLog.forTable(spark, tempDir)

      val snapshotBeforeUpdate = deltaLog.update()
      val allFilesBeforeUpdate = snapshotBeforeUpdate.allFiles.collect()

      DeltaTable.forPath(spark, tempDir.getAbsolutePath)
        .update(col("value")  === 0, Map("value" -> (col("value") + 1)))

      val snapshot = deltaLog.update()
      val allFiles = snapshot.allFiles.collect()
      val tombstones = snapshot.tombstones.collect()
      // nothing changed
      assert(tombstones.length === 0)
      assert(allFiles === allFilesBeforeUpdate)

      checkAnswer(
        spark.read.format("delta").load(tempDir.getAbsolutePath),
        expectedTable2DataV1.toDF()
      )
    }
  }

  test("INSERT + DELETE + MERGE + UPDATE with DVs") {
    withTempDir { tempDir =>
      val path = tempDir.getAbsolutePath
      val deltaLog = DeltaLog.forTable(spark, path)

      def checkTableContents(rows: DataFrame): Unit =
        checkAnswer(sql(s"SELECT * FROM delta.`$path`"), rows)

      // Version 0: DV is enabled on table
      {
        withSQLConf(
          DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.defaultTablePropertyKey -> "true") {
          spark.range(0, 10, 1, numPartitions = 2).write.format("delta").save(path)
        }
        val snapshot = deltaLog.update()
        assert(snapshot.protocol.isFeatureSupported(DeletionVectorsTableFeature))
        for (f <- snapshot.allFiles.collect()) {
          assert(f.tightBounds.get)
        }
      }
      // Version 1: DELETE one row from each file
      {
        withSQLConf(DeltaSQLConf.DELETE_USE_PERSISTENT_DELETION_VECTORS.key -> "true") {
          sql(s"DELETE FROM delta.`$path` WHERE id IN (1, 8)")
        }
        val (add, _) = getFileActionsInLastVersion(deltaLog)
        for (a <- add) {
          assert(a.deletionVector !== null)
          assert(a.deletionVector.cardinality === 1)
          assert(a.numPhysicalRecords.get === a.numLogicalRecords.get + 1)
          assert(a.tightBounds.get === false)
        }

        checkTableContents(Seq(0, 2, 3, 4, 5, 6, 7, 9).toDF())
      }
      // Version 2: UPDATE one row in the first file
      {
        sql(s"UPDATE delta.`$path` SET id = -1 WHERE id = 0")
        val (added, removed) = getFileActionsInLastVersion(deltaLog)
        assert(added.length === 2)
        assert(removed.length === 1)
        // Added files must be two, one containing DV and one not
        assert(added.count(_.deletionVector != null) === 1)
        assert(added.count(_.deletionVector == null) === 1)
        // Removed files must contain DV
        for (r <- removed) {
          assert(r.deletionVector !== null)
        }

        checkTableContents(Seq(-1, 2, 3, 4, 5, 6, 7, 9).toDF())
      }
      // Version 3: MERGE into the table using table2
      {
        DeltaTable.forPath(spark, path).as("target")
          .merge(
            spark.read.format("delta").load(table2Path).as("source"),
            "source.value = target.id")
          .whenMatched()
          .updateExpr(Map("id" -> "source.value"))
          .whenNotMatchedBySource().delete().execute()
        val (added, removed) = getFileActionsInLastVersion(deltaLog)
        assert(removed.length === 3)
        for (a <- added) {
          assert(a.deletionVector === null)
          assert(a.tightBounds.get)
        }
        // Two of three removed files have DV
        assert(removed.count(_.deletionVector != null) === 2)

        // -1 and 9 are deleted by "when not matched by source"
        checkTableContents(Seq(2, 3, 4, 5, 6, 7).toDF())
      }
      // Version 4: DELETE one row again
      {
        withSQLConf(DeltaSQLConf.DELETE_USE_PERSISTENT_DELETION_VECTORS.key -> "true") {
          sql(s"DELETE FROM delta.`$path` WHERE id IN (4)")
        }
        val (add, _) = getFileActionsInLastVersion(deltaLog)
        for (a <- add) {
          assert(a.deletionVector !== null)
          assert(a.deletionVector.cardinality === 1)
          assert(a.numPhysicalRecords.get === a.numLogicalRecords.get + 1)
          assert(a.tightBounds.get === false)
        }

        checkTableContents(Seq(2, 3, 5, 6, 7).toDF())
      }
    }
  }

  test("huge table: read from tables of 2B rows with existing DV of many zeros") {
    val canonicalTable5Path = new File(table5Path).getCanonicalPath

    val predicatePushDownEnabled =
      spark.conf.get(DeltaSQLConf.DELETION_VECTORS_USE_METADATA_ROW_INDEX)

    try {
      checkCountAndSum("value", table5Count, table5Sum, canonicalTable5Path)
    } catch {
      // TODO(SPARK-47731): Known issue. To be fixed in Spark 3.5 and/or Spark 4.0.
      case e: SparkException if predicatePushDownEnabled &&
        (e.getMessage.contains("More than Int.MaxValue elements") ||
          e.getCause.getMessage.contains("More than Int.MaxValue elements")) => () // Ignore.
    }
  }

  test("sanity check for non-incremental DV update") {
    val addFile = createTestAddFile()
    def bitmapToDvDescriptor(bitmap: RoaringBitmapArray): DeletionVectorDescriptor = {
      DeletionVectorDescriptor.inlineInLog(
        bitmap.serializeAsByteArray(RoaringBitmapArrayFormat.Portable),
        bitmap.cardinality)
    }
    val dv0 = bitmapToDvDescriptor(RoaringBitmapArray())
    val dv1 = bitmapToDvDescriptor(RoaringBitmapArray(0L, 1L))
    val dv2 = bitmapToDvDescriptor(RoaringBitmapArray(0L, 2L))
    val dv3 = bitmapToDvDescriptor(RoaringBitmapArray(3L))

    def removeRows(a: AddFile, dv: DeletionVectorDescriptor): (AddFile, RemoveFile) = {
      a.removeRows(
        deletionVector = dv,
        updateStats = true
      )
    }

    // Adding an empty DV to a file is allowed.
    removeRows(addFile, dv0)
    // Updating with the same DV is allowed.
    val (addFileWithDV1, _) = removeRows(addFile, dv1)
    removeRows(addFileWithDV1, dv1)
    // Updating with a different DV with the same cardinality and different rows should not be
    // allowed, but is expensive to detect it.
    removeRows(addFileWithDV1, dv2)

    // Updating with a DV with lower cardinality should throw.
    for (dv <- Seq(dv0, dv3)) {
      assertThrows[DeltaChecksumException] {
        removeRows(addFileWithDV1, dv)
      }
    }
  }

  test("Check no resource leak when DV files are missing (table corrupted)") {
    withTempDir { tempDir =>
      val source = new File(table2Path)
      val target = new File(tempDir, "resourceLeakTest")
      val targetPath = target.getAbsolutePath

      // Copy the source DV table to a temporary directory
      FileUtils.copyDirectory(source, target)

      val filesWithDvs = getFilesWithDeletionVectors(DeltaLog.forTable(spark, target))
      assert(filesWithDvs.size > 0)
      deleteDVFile(targetPath, filesWithDvs(0))

      val se = intercept[SparkException] {
        spark.sql(s"SELECT * FROM delta.`$targetPath`").collect()
      }
      assert(findIfResponsible[FileNotFoundException](se).nonEmpty,
        s"Expected a file not found exception as the cause, but got: [${se}]")
    }
  }

  test("absolute DV path with encoded special characters") {
    // This test uses hand-crafted path with special characters.
    // Do not test with a prefix that needs URL standard escaping.
    withTempDir(prefix = "spark") { dir =>
      writeTableHavingSpecialCharInDVPath(dir, pathIsEncoded = true)
      checkAnswer(
        spark.read.format("delta").load(dir.getCanonicalPath),
        Seq(1, 3, 5, 7, 9).toDF())
    }
  }

  test("absolute DV path with not-encoded special characters") {
    // This test uses hand-crafted path with special characters.
    // Do not test with a prefix that needs URL standard escaping.
    withTempDir(prefix = "spark") { dir =>
      writeTableHavingSpecialCharInDVPath(dir, pathIsEncoded = false)
      val e = interceptWithUnwrapping[URISyntaxException] {
        spark.read.format("delta").load(dir.getCanonicalPath).collect()
      }
      assert(e.getMessage.contains("Malformed escape pair"))
    }
  }

  private sealed case class DeleteUsingDVWithResults(
      scale: String,
      sqlRule: String,
      count: Long,
      sum: Long)
  private val deleteUsingDvSmallScale = DeleteUsingDVWithResults(
    "small",
    "value = 1",
    table5CountByValues.filterKeys(_ != 1).values.sum,
    table5SumByValues.filterKeys(_ != 1).values.sum)
  private val deleteUsingDvMediumScale = DeleteUsingDVWithResults(
    "medium",
    "value > 10",
    table5CountByValues.filterKeys(_ <= 10).values.sum,
    table5SumByValues.filterKeys(_ <= 10).values.sum)
  private val deleteUsingDvLargeScale = DeleteUsingDVWithResults(
    "large",
    "value != 21",
    table5CountByValues(21),
    table5SumByValues(21))

  // deleteUsingDvMediumScale and deleteUsingDvLargeScale runs too slow thus disabled.
  for (deleteSpec <- Seq(deleteUsingDvSmallScale)) {
    test(
      s"huge table: delete a ${deleteSpec.scale} number of rows from tables of 2B rows with DVs") {
      val predicatePushDownEnabled =
        spark.conf.get(DeltaSQLConf.DELETION_VECTORS_USE_METADATA_ROW_INDEX)
      withTempDir { dir =>
        try {
          FileUtils.copyDirectory(new File(table5Path), dir)
          val log = DeltaLog.forTable(spark, dir)

          withDeletionVectorsEnabled() {
            sql(s"DELETE FROM delta.`${dir.getCanonicalPath}` WHERE ${deleteSpec.sqlRule}")
          }
          val (added, _) = getFileActionsInLastVersion(log)
          assert(added.forall(_.deletionVector != null))

          checkCountAndSum("value", deleteSpec.count, deleteSpec.sum, dir.getCanonicalPath)
        } catch {
          // TODO(SPARK-47731): Known issue. To be fixed in Spark 3.5 and/or Spark 4.0.
          case e: SparkException if predicatePushDownEnabled &&
            (e.getMessage.contains("More than Int.MaxValue elements") ||
              e.getCause.getMessage.contains("More than Int.MaxValue elements")) => () // Ignore.
        }
      }
    }
  }

  private def checkCountAndSum(column: String, count: Long, sum: Long, tableDir: String): Unit = {
    checkAnswer(
      sql(s"SELECT count($column), sum($column) FROM delta.`$tableDir`"),
      Seq((count, sum)).toDF())
  }

  private def assertPlanContains(queryDf: DataFrame, expected: String): Unit = {
    val optimizedPlan = queryDf.queryExecution.analyzed.toString()
    assert(optimizedPlan.contains(expected), s"Plan is missing `$expected`: $optimizedPlan")
  }
}

object DeletionVectorsSuite {
  val table1Path = "src/test/resources/delta/table-with-dv-large"
  // Table at version 0: contains [0, 2000)
  val expectedTable1DataV0 = Seq.range(0, 2000)
  // Table at version 1: removes rows with id = 0, 180, 300, 700, 1800
  val v1Removed = Set(0, 180, 300, 700, 1800)
  val expectedTable1DataV1 = expectedTable1DataV0.filterNot(e => v1Removed.contains(e))
  // Table at version 2: inserts rows with id = 300, 700
  val v2Added = Set(300, 700)
  val expectedTable1DataV2 = expectedTable1DataV1 ++ v2Added
  // Table at version 3: removes rows with id = 300, 250, 350, 900, 1353, 1567, 1800
  val v3Removed = Set(300, 250, 350, 900, 1353, 1567, 1800)
  val expectedTable1DataV3 = expectedTable1DataV2.filterNot(e => v3Removed.contains(e))
  // Table at version 4: inserts rows with id = 900, 1567
  val v4Added = Set(900, 1567)
  val expectedTable1DataV4 = expectedTable1DataV3 ++ v4Added

  val table2Path = "src/test/resources/delta/table-with-dv-small"
  // Table at version 0: contains 0 - 9
  val expectedTable2DataV0 = Seq(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
  // Table at version 1: removes rows 0 and 9
  val expectedTable2DataV1 = Seq(1, 2, 3, 4, 5, 6, 7, 8)

  val table3Path = "src/test/resources/delta/partitioned-table-with-dv-large"
  // Table at version 0: contains [0, 2000)
  val expectedTable3DataV0 = Seq.range(0, 2000)
  // Table at version 1: removes rows with id = (0, 180, 308, 225, 756, 1007, 1503)
  val table3V1Removed = Set(0, 180, 308, 225, 756, 1007, 1503)
  val expectedTable3DataV1 = expectedTable3DataV0.filterNot(e => table3V1Removed.contains(e))
  // Table at version 2: inserts rows with id = 308, 756
  val table3V2Added = Set(308, 756)
  val expectedTable3DataV2 = expectedTable3DataV1 ++ table3V2Added
  // Table at version 3: removes rows with id = (300, 257, 399, 786, 1353, 1567, 1800)
  val table3V3Removed = Set(300, 257, 399, 786, 1353, 1567, 1800)
  val expectedTable3DataV3 = expectedTable3DataV2.filterNot(e => table3V3Removed.contains(e))
  // Table at version 4: inserts rows with id = 1353, 1567
  val table3V4Added = Set(1353, 1567)
  val expectedTable3DataV4 = expectedTable3DataV3 ++ table3V4Added

  // Table with DV table feature as supported but no DVs
  val table4Path = "src/test/resources/delta/table-with-dv-feature-enabled"
  val expectedTable4DataV0 = Seq(1L)

  // Table with DV, (1<<31)+10=2147483658 rows in total including 2147484 rows deleted. Parquet is
  // generated by:
  //   spark.range(0, (1L << 31) + 10, 1, numPartitions = 1)
  //     .withColumn(
  //       "value",
  //       when($"id" % 1000 === 0, 1).otherwise(($"id" / 100000000).cast(IntegerType)))
  // All "id % 1000 = 0" rows are marked as deleted.
  // Column "value" ranges from 0 to 21.
  // 99900000 rows with values 0 to 20 each, and 47436174 rows with value 21.
  val table5Path = "src/test/resources/delta/table-with-dv-gigantic"
  val table5Count = 2145336174L
  val table5Sum = 21975159654L
  val table5CountByValues = (0 to 20).map(_ -> 99900000L).toMap + (21 -> 47436174L)
  val table5SumByValues = (0 to 20).map(v => v -> v * 99900000L).toMap + (21 -> 21 * 47436174L)

  // Generate a table with special characters in DV path.
  // Content of this table is range(0, 10) with all even numbers deleted.
  def writeTableHavingSpecialCharInDVPath(path: File, pathIsEncoded: Boolean): Unit = {
    val tableHavingSpecialCharInDVTemplate = "src/test/resources/delta/table-with-dv-special-char"
    FileUtils.copyDirectory(new File(tableHavingSpecialCharInDVTemplate), path)
    val fullPath = new File(
      path,
      if (pathIsEncoded) "folder&with%25special%20char" else "folder&with%special char")
      .getCanonicalPath
    val logJson = new File(path, "_delta_log/00000000000000000000.json")
    val logJsonContent = FileUtils.readFileToString(logJson, "UTF-8")
    val newLogJsonContent = logJsonContent.replace(
      "{{FOLDER_WITH_SPECIAL_CHAR}}", fullPath)
    FileUtils.delete(logJson)
    FileUtils.write(logJson, newLogJsonContent, "UTF-8")
  }
}

class DeletionVectorsWithPredicatePushdownSuite extends DeletionVectorsSuite {
  // ~4MBs. Should contain 2 row groups.
  val multiRowgroupTable = "multiRowgroupTable"
  val multiRowgroupTableRowsNum = 1000000

  def assertParquetHasMultipleRowGroups(filePath: Path): Unit = {
    val parquetMetadata = ParquetFileReader.readFooter(
      hadoopConf,
      filePath,
      ParquetMetadataConverter.NO_FILTER)
    assert(parquetMetadata.getBlocks.size() > 1)
  }

  override def beforeAll(): Unit = {
    super.beforeAll()

    // 2MB rowgroups.
    hadoopConf().set("parquet.block.size", (2 * 1024 * 1024).toString)

    spark.range(0, multiRowgroupTableRowsNum, 1, 1).toDF("id")
      .write
      .option(DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.key, true.toString)
      .format("delta")
      .saveAsTable(multiRowgroupTable)

    val deltaLog = DeltaLog.forTable(spark, TableIdentifier(multiRowgroupTable))
    val files = deltaLog.update().allFiles.collect()

    assert(files.length === 1)
    assertParquetHasMultipleRowGroups(new Path(deltaLog.dataPath, files.head.path))

    spark.conf.set(DeltaSQLConf.DELETION_VECTORS_USE_METADATA_ROW_INDEX.key, "true")
  }

  override def afterAll(): Unit = {
    super.afterAll()
    sql(s"DROP TABLE IF EXISTS $multiRowgroupTable")
  }

  private def testPredicatePushDown(
      deletePredicates: Seq[String],
      selectPredicate: Option[String],
      expectedNumRows: Long,
      validationPredicate: String,
      vectorizedReaderEnabled: Boolean,
      readColumnarBatchAsRows: Boolean): Unit = {
    withTempDir { dir =>
      // This forces the code generator to not use codegen. As a result, Spark sets options to get
      // rows instead of columnar batches from the Parquet reader. This allows to test the relevant
      // code path in DeltaParquetFileFormat.
      val codeGenMaxFields = if (readColumnarBatchAsRows) "0" else "100"
      withSQLConf(
          SQLConf.WHOLESTAGE_MAX_NUM_FIELDS.key -> codeGenMaxFields,
          SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> vectorizedReaderEnabled.toString,
          SQLConf.FILES_MAX_PARTITION_BYTES.key -> "2MB") {
        sql(s"CREATE TABLE delta.`${dir.getCanonicalPath}` SHALLOW CLONE $multiRowgroupTable")

        val targetTable = io.delta.tables.DeltaTable.forPath(dir.getCanonicalPath)
        val deltaLog = DeltaLog.forTable(spark, dir.getCanonicalPath)
        val files = deltaLog.update().allFiles.collect()
        assert(files.length === 1)

        // Execute multiple delete statements. These require to reconsile the metadata column
        // between DV writing and scanning operations.
        deletePredicates.foreach(targetTable.delete)

        val targetTableDF = selectPredicate.map(targetTable.toDF.filter).getOrElse(targetTable.toDF)
        assertPredicatesArePushedDown(targetTableDF)
        // Make sure there are multiple row groups.
        assertParquetHasMultipleRowGroups(new Path(files.head.path))
        // Make sure we have 2 splits.
        assert(targetTableDF.rdd.partitions.size === 2)

        assert(targetTableDF.count() === expectedNumRows)
        // The deleted/filtered rows should not exist.
        assert(targetTableDF.filter(validationPredicate).count() === 0)
      }
    }
  }

  for {
    vectorizedReaderEnabled <- BOOLEAN_DOMAIN
    readColumnarBatchAsRows <- if (vectorizedReaderEnabled) BOOLEAN_DOMAIN else Seq(false)
  } test("PredicatePushdown: Single deletion at the first row group. " +
    s"vectorizedReaderEnabled: $vectorizedReaderEnabled " +
    s"readColumnarBatchAsRows: $readColumnarBatchAsRows") {
    testPredicatePushDown(
      deletePredicates = Seq("id == 100"),
      selectPredicate = None,
      expectedNumRows = multiRowgroupTableRowsNum - 1,
      validationPredicate = "id == 100",
      vectorizedReaderEnabled = vectorizedReaderEnabled,
      readColumnarBatchAsRows = readColumnarBatchAsRows)
  }

  for {
    vectorizedReaderEnabled <- BOOLEAN_DOMAIN
    readColumnarBatchAsRows <- if (vectorizedReaderEnabled) BOOLEAN_DOMAIN else Seq(false)
  } test("PredicatePushdown: Single deletion at the second row group. " +
    s"vectorizedReaderEnabled: $vectorizedReaderEnabled " +
    s"readColumnarBatchAsRows: $readColumnarBatchAsRows") {
    testPredicatePushDown(
      deletePredicates = Seq("id == 900000"),
      selectPredicate = None,
      expectedNumRows = multiRowgroupTableRowsNum - 1,
      // (rowId, Expected value).
      validationPredicate = "id == 900000",
      vectorizedReaderEnabled = vectorizedReaderEnabled,
      readColumnarBatchAsRows = readColumnarBatchAsRows)
  }

  for {
    vectorizedReaderEnabled <- BOOLEAN_DOMAIN
    readColumnarBatchAsRows <- if (vectorizedReaderEnabled) BOOLEAN_DOMAIN else Seq(false)
  } test("PredicatePushdown: Single delete statement with multiple ids. " +
    s"vectorizedReaderEnabled: $vectorizedReaderEnabled " +
    s"readColumnarBatchAsRows: $readColumnarBatchAsRows") {
    testPredicatePushDown(
      deletePredicates = Seq("id in (20, 200, 2000, 900000)"),
      selectPredicate = None,
      expectedNumRows = multiRowgroupTableRowsNum - 4,
      validationPredicate = "id in (20, 200, 2000, 900000)",
      vectorizedReaderEnabled = vectorizedReaderEnabled,
      readColumnarBatchAsRows = readColumnarBatchAsRows)
  }

  for {
    vectorizedReaderEnabled <- BOOLEAN_DOMAIN
    readColumnarBatchAsRows <- if (vectorizedReaderEnabled) BOOLEAN_DOMAIN else Seq(false)
  } test("PredicatePushdown: Multiple delete statements. " +
    s"vectorizedReaderEnabled: $vectorizedReaderEnabled " +
    s"readColumnarBatchAsRows: $readColumnarBatchAsRows") {
    testPredicatePushDown(
      deletePredicates = Seq("id = 20", "id = 200", "id = 2000", "id = 900000"),
      selectPredicate = None,
      expectedNumRows = multiRowgroupTableRowsNum - 4,
      validationPredicate = "id in (20, 200, 2000, 900000)",
      vectorizedReaderEnabled = vectorizedReaderEnabled,
      readColumnarBatchAsRows = readColumnarBatchAsRows)
  }

  for {
    vectorizedReaderEnabled <- BOOLEAN_DOMAIN
    readColumnarBatchAsRows <- if (vectorizedReaderEnabled) BOOLEAN_DOMAIN else Seq(false)
  } test("PredicatePushdown: Scan with predicates. " +
    s"vectorizedReaderEnabled: $vectorizedReaderEnabled " +
    s"readColumnarBatchAsRows: $readColumnarBatchAsRows") {
    testPredicatePushDown(
      deletePredicates = Seq("id = 20", "id = 2000"),
      selectPredicate = Some("id not in (200, 900000)"),
      expectedNumRows = multiRowgroupTableRowsNum - 4,
      validationPredicate = "id in (20, 200, 2000, 900000)",
      vectorizedReaderEnabled = vectorizedReaderEnabled,
      readColumnarBatchAsRows = readColumnarBatchAsRows)
  }

  for {
    vectorizedReaderEnabled <- BOOLEAN_DOMAIN
    readColumnarBatchAsRows <- if (vectorizedReaderEnabled) BOOLEAN_DOMAIN else Seq(false)
  } test("PredicatePushdown: Scan with predicates - no deletes. " +
    s"vectorizedReaderEnabled: $vectorizedReaderEnabled " +
    s"readColumnarBatchAsRows: $readColumnarBatchAsRows") {
    testPredicatePushDown(
      deletePredicates = Seq.empty,
      selectPredicate = Some("id not in (20, 200, 2000, 900000)"),
      expectedNumRows = multiRowgroupTableRowsNum - 4,
      validationPredicate = "id in (20, 200, 2000, 900000)",
      vectorizedReaderEnabled = vectorizedReaderEnabled,
      readColumnarBatchAsRows = readColumnarBatchAsRows)
  }

  test("Predicate pushdown works on queries that select metadata fields") {
    withTempDir { dir =>
      withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> true.toString) {
        sql(s"CREATE TABLE delta.`${dir.getCanonicalPath}` SHALLOW CLONE $multiRowgroupTable")

        val targetTable = io.delta.tables.DeltaTable.forPath(dir.getCanonicalPath)
        targetTable.delete("id == 900000")

        val r1 = targetTable.toDF.select("id", "_metadata.row_index").count()
        assert(r1 === multiRowgroupTableRowsNum - 1)

        val r2 = targetTable.toDF.select("id", "_metadata.row_index", "_metadata.file_path").count()
        assert(r2 === multiRowgroupTableRowsNum - 1)

        val r3 = targetTable
          .toDF
          .select("id", "_metadata.file_block_start", "_metadata.file_path").count()
        assert(r3 === multiRowgroupTableRowsNum - 1)
      }
    }
  }

  private def assertPredicatesArePushedDown(df: DataFrame): Unit = {
    val scan = df.queryExecution.executedPlan.collectFirst {
      case scan: FileSourceScanExec => scan
    }
    assert(scan.map(_.dataFilters.nonEmpty).getOrElse(true))
  }
}
