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

package org.apache.spark.sql.delta

import java.io.File
import java.net.URI

// scalastyle:off import.ordering.noEmptyLine
import org.apache.spark.sql.delta.DeltaOperations.Delete
import org.apache.spark.sql.delta.commands.DeltaGenerateCommand
import org.apache.spark.sql.delta.hooks.GenerateSymlinkManifest
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.util.DeltaFileOperations
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.util.Progressable

import org.apache.spark.SparkThrowable
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SharedSparkSession
// scalastyle:on import.ordering.noEmptyLine

class DeltaGenerateSymlinkManifestSuite
  extends DeltaGenerateSymlinkManifestSuiteBase
  with DeltaSQLCommandTest

trait DeltaGenerateSymlinkManifestSuiteBase extends QueryTest
  with SharedSparkSession  with DeletionVectorsTestUtils
  with DeltaTestUtilsForTempViews {

  import testImplicits._

  test("basic case: SQL command - path-based table") {
    withTempDir { tablePath =>
      tablePath.delete()

      spark.createDataset(spark.sparkContext.parallelize(1 to 100, 7))
        .write.format("delta").mode("overwrite").save(tablePath.toString)

      assertManifest(tablePath, expectSameFiles = false, expectedNumFiles = 0)

      // Create a Delta table and call the scala api for generating manifest files
      spark.sql(s"GENERATE symlink_ForMat_Manifest FOR TABLE delta.`${tablePath.getAbsolutePath}`")
      assertManifest(tablePath, expectSameFiles = true, expectedNumFiles = 7)
    }
  }

  test("basic case: SQL command - name-based table") {
    withTable("deltaTable") {
      spark.createDataset(spark.sparkContext.parallelize(1 to 100, 7))
        .write.format("delta").saveAsTable("deltaTable")

      val tableId = TableIdentifier("deltaTable")
      val tablePath = new File(spark.sessionState.catalog.getTableMetadata(tableId).location)
      assertManifest(tablePath, expectSameFiles = false, expectedNumFiles = 0)

      spark.sql(s"GENERATE symlink_ForMat_Manifest FOR TABLE deltaTable")
      assertManifest(tablePath, expectSameFiles = true, expectedNumFiles = 7)
    }
  }

  test("basic case: SQL command - throw error on bad tables") {
    var e: Exception = intercept[AnalysisException] {
      spark.sql("GENERATE symlink_format_manifest FOR TABLE nonExistentTable")
    }
    assert(e.getMessage.contains("not found") || e.getMessage.contains("cannot be found"))

    withTable("nonDeltaTable") {
      spark.range(2).write.format("parquet").saveAsTable("nonDeltaTable")
      e = intercept[AnalysisException] {
        spark.sql("GENERATE symlink_format_manifest FOR TABLE nonDeltaTable")
      }
      assert(e.getMessage.contains("only supported for Delta"))
    }
  }

  test("basic case: SQL command - throw error on non delta table paths") {
    withTempDir { dir =>
      var e = intercept[AnalysisException] {
        spark.sql(s"GENERATE symlink_format_manifest FOR TABLE delta.`$dir`")
      }

      assert(e.getMessage.contains("not found") ||
        e.getMessage.contains("only supported for Delta"))

      spark.range(2).write.format("parquet").mode("overwrite").save(dir.toString)

      e = intercept[AnalysisException] {
        spark.sql(s"GENERATE symlink_format_manifest FOR TABLE delta.`$dir`")
      }
      assert(e.getMessage.contains("table not found") ||
        e.getMessage.contains("only supported for Delta"))

      e = intercept[AnalysisException] {
        spark.sql(s"GENERATE symlink_format_manifest FOR TABLE parquet.`$dir`")
      }
      assert(e.getMessage.contains("not found") || e.getMessage.contains("cannot be found"))
    }
  }

  testWithTempView("basic case: SQL command - throw error on temp views") { isSQLTempView =>
    withTable("t1") {
      spark.range(2).write.format("delta").saveAsTable("t1")
      createTempViewFromTable("t1", isSQLTempView)
      val e = intercept[AnalysisException] {
        spark.sql(s"GENERATE symlink_format_manifest FOR TABLE v")
      }
      assert(e.getMessage.contains("not found") || e.getMessage.contains("cannot be found"))
    }
  }

  test("basic case: SQL command - throw error on unsupported mode") {
    withTempDir { tablePath =>
      spark.range(2).write.format("delta").save(tablePath.getAbsolutePath)
      val e = intercept[IllegalArgumentException] {
        spark.sql(s"GENERATE xyz FOR TABLE delta.`${tablePath.getAbsolutePath}`")
      }
      assert(e.toString.contains("not supported"))
      DeltaGenerateCommand.modeNameToGenerationFunc.keys.foreach { modeName =>
        assert(e.toString.contains(modeName))
      }
    }
  }

  test("basic case: Scala API - path-based table") {
    withTempDir { tablePath =>
      tablePath.delete()

      spark.createDataset(spark.sparkContext.parallelize(1 to 100, 7))
        .write.format("delta").mode("overwrite").save(tablePath.toString)

      assertManifest(tablePath, expectSameFiles = false, expectedNumFiles = 0)

      // Create a Delta table and call the scala api for generating manifest files
      val deltaTable = io.delta.tables.DeltaTable.forPath(tablePath.getAbsolutePath)
      deltaTable.generate("symlink_format_manifest")
      assertManifest(tablePath, expectSameFiles = true, expectedNumFiles = 7)
    }
  }

  test("basic case: Scala API - name-based table") {
    withTable("deltaTable") {
      spark.createDataset(spark.sparkContext.parallelize(1 to 100, 7))
        .write.format("delta").saveAsTable("deltaTable")

      val tableId = TableIdentifier("deltaTable")
      val tablePath = new File(spark.sessionState.catalog.getTableMetadata(tableId).location)
      assertManifest(tablePath, expectSameFiles = false, expectedNumFiles = 0)

      val deltaTable = io.delta.tables.DeltaTable.forName("deltaTable")
      deltaTable.generate("symlink_format_manifest")
      assertManifest(tablePath, expectSameFiles = true, expectedNumFiles = 7)
    }
  }


  test ("full manifest: non-partitioned table") {
    withTempDir { tablePath =>
      tablePath.delete()

      def write(parallelism: Int): Unit = {
        spark.createDataset(spark.sparkContext.parallelize(1 to 100, parallelism))
          .write.format("delta").mode("overwrite").save(tablePath.toString)
      }

      write(7)
      assertManifest(tablePath, expectSameFiles = false, expectedNumFiles = 0)
      generateSymlinkManifest(tablePath.toString)
      assertManifest(tablePath, expectSameFiles = true, expectedNumFiles = 7)

      // Reduce files
      write(5)
      assertManifest(tablePath, expectSameFiles = false, expectedNumFiles = 7)
      generateSymlinkManifest(tablePath.toString)
      assertManifest(tablePath, expectSameFiles = true, expectedNumFiles = 5)

      // Remove all data
      spark.emptyDataset[Int].write.format("delta").mode("overwrite").save(tablePath.toString)
      assertManifest(tablePath, expectSameFiles = false, expectedNumFiles = 5)
      generateSymlinkManifest(tablePath.toString)
      assertManifest(
        tablePath, expectSameFiles = true, expectedNumFiles = 0)
      assert(spark.read.format("delta").load(tablePath.toString).count() == 0)

      // delete all data
      write(5)
      assertManifest(
        tablePath, expectSameFiles = false, expectedNumFiles = 0)
      val deltaTable = io.delta.tables.DeltaTable.forPath(spark, tablePath.toString)
      deltaTable.delete()
      generateSymlinkManifest(tablePath.toString)
      assertManifest(tablePath, expectSameFiles = true, expectedNumFiles = 0)
      assert(spark.read.format("delta").load(tablePath.toString).count() == 0)
    }
  }

  test("full manifest: partitioned table") {
    withTempDir { tablePath =>
      tablePath.delete()

      def write(parallelism: Int, partitions1: Int, partitions2: Int): Unit = {
        spark.createDataset(spark.sparkContext.parallelize(1 to 100, parallelism)).toDF("value")
          .withColumn("part1", $"value" % partitions1)
          .withColumn("part2", $"value" % partitions2)
          .write.format("delta").partitionBy("part1", "part2")
          .mode("overwrite").save(tablePath.toString)
      }

      write(10, 10, 10)
      assertManifest(tablePath, expectSameFiles = false, expectedNumFiles = 0)
      generateSymlinkManifest(tablePath.toString)
      // 10 files each in ../part1=X/part2=X/ for X = 0 to 9
      assertManifest(tablePath, expectSameFiles = true, expectedNumFiles = 100)

      // Reduce # partitions on both dimensions
      write(1, 1, 1)
      assertManifest(tablePath, expectSameFiles = false, expectedNumFiles = 100)
      generateSymlinkManifest(tablePath.toString)
      assertManifest(tablePath, expectSameFiles = true, expectedNumFiles = 1)

      // Increase # partitions on both dimensions
      write(5, 5, 5)
      assertManifest(tablePath, expectSameFiles = false, expectedNumFiles = 1)
      generateSymlinkManifest(tablePath.toString)
      assertManifest(tablePath, expectSameFiles = true, expectedNumFiles = 25)

      // Increase # partitions on only one dimension
      write(5, 10, 5)
      assertManifest(tablePath, expectSameFiles = false, expectedNumFiles = 25)
      generateSymlinkManifest(tablePath.toString)
      assertManifest(tablePath, expectSameFiles = true, expectedNumFiles = 50)

      // Remove all data
      spark.emptyDataset[Int].toDF("value")
        .withColumn("part1", $"value" % 10)
        .withColumn("part2", $"value" % 10)
        .write.format("delta").mode("overwrite").save(tablePath.toString)
      assertManifest(tablePath, expectSameFiles = false, expectedNumFiles = 50)
      generateSymlinkManifest(tablePath.toString)
      assertManifest(tablePath, expectSameFiles = true, expectedNumFiles = 0)
      assert(spark.read.format("delta").load(tablePath.toString).count() == 0)

      // delete all data
      write(5, 5, 5)
      generateSymlinkManifest(tablePath.toString)
      assertManifest(tablePath, expectSameFiles = true, expectedNumFiles = 25)
      val deltaTable = io.delta.tables.DeltaTable.forPath(spark, tablePath.toString)
      deltaTable.delete()
      generateSymlinkManifest(tablePath.toString)
      assertManifest(tablePath, expectSameFiles = true, expectedNumFiles = 0)
      assert(spark.read.format("delta").load(tablePath.toString).count() == 0)
    }
  }

  test("incremental manifest: table property controls post commit manifest generation") {
    withTempDir { tablePath =>
      tablePath.delete()

      def writeWithIncrementalManifest(enabled: Boolean, numFiles: Int): Unit = {
        withIncrementalManifest(tablePath, enabled) {
          spark.createDataset(spark.sparkContext.parallelize(1 to 100, numFiles))
            .write.format("delta").mode("overwrite").save(tablePath.toString)
        }
      }

      writeWithIncrementalManifest(enabled = false, numFiles = 1)
      assertManifest(tablePath, expectSameFiles = false, expectedNumFiles = 0)

      // Enabling it should automatically generate manifest files
      writeWithIncrementalManifest(enabled = true, numFiles = 2)
      assertManifest(tablePath, expectSameFiles = true, expectedNumFiles = 2)

      // Disabling it should stop updating existing manifest files
      writeWithIncrementalManifest(enabled = false, numFiles = 3)
      assertManifest(tablePath, expectSameFiles = false, expectedNumFiles = 2)
    }
  }

  test("incremental manifest: unpartitioned table") {
    withTempDir { tablePath =>
      tablePath.delete()

      def write(numFiles: Int): Unit = withIncrementalManifest(tablePath, enabled = true) {
        spark.createDataset(spark.sparkContext.parallelize(1 to 100, numFiles))
          .write.format("delta").mode("overwrite").save(tablePath.toString)
      }

      write(1)
      // first write won't generate automatic manifest as mode enable after first write
      assertManifest(tablePath, expectSameFiles = false, expectedNumFiles = 0)

      // Increase files
      write(7)
      assertManifest(tablePath, expectSameFiles = true, expectedNumFiles = 7)

      // Reduce files
      write(5)
      assertManifest(tablePath, expectSameFiles = true, expectedNumFiles = 5)

      // Remove all data
      spark.emptyDataset[Int].write.format("delta").mode("overwrite").save(tablePath.toString)
      assert(spark.read.format("delta").load(tablePath.toString).count() == 0)
      assertManifest(
        tablePath, expectSameFiles = true, expectedNumFiles = 0)
    }
  }



  test("incremental manifest: partitioned table") {
    withTempDir { tablePath =>
      tablePath.delete()

      def writePartitioned(parallelism: Int, numPartitions1: Int, numPartitions2: Int): Unit = {
        withIncrementalManifest(tablePath, enabled = true) {
          val input =
            if (parallelism == 0) spark.emptyDataset[Int]
            else spark.createDataset(spark.sparkContext.parallelize(1 to 100, parallelism))
          input.toDF("value")
            .withColumn("part1", $"value" % numPartitions1)
            .withColumn("part2", $"value" % numPartitions2)
            .write.format("delta").partitionBy("part1", "part2")
            .mode("overwrite").save(tablePath.toString)
        }
      }

      writePartitioned(1, 1, 1)
      // Manifests wont be generated in the first write because `withIncrementalManifest` will
      // enable manifest generation only after the first write defines the table log.
      assertManifest(tablePath, expectSameFiles = false, expectedNumFiles = 0)

      writePartitioned(10, 10, 10)
      // 10 files each in ../part1=X/part2=X/ for X = 0 to 9 (so only 10 subdirectories)
      assertManifest(tablePath, expectSameFiles = true, expectedNumFiles = 100)

      // Update such that 1 file is removed and 1 file is added in another partition
      val deltaTable = io.delta.tables.DeltaTable.forPath(spark, tablePath.toString)
      deltaTable.updateExpr("value = 1", Map("part1" -> "0", "value" -> "-1"))
      assertManifest(tablePath, expectSameFiles = true, expectedNumFiles = 100)

      // Delete such that 1 file is removed
      deltaTable.delete("value = -1")
      assertManifest(tablePath, expectSameFiles = true, expectedNumFiles = 99)

      // Reduce # partitions on both dimensions
      writePartitioned(1, 1, 1)
      assertManifest(tablePath, expectSameFiles = true, expectedNumFiles = 1)

      // Increase # partitions on both dimensions
      writePartitioned(5, 5, 5)
      assertManifest(tablePath, expectSameFiles = true, expectedNumFiles = 25)

      // Increase # partitions on only one dimension
      writePartitioned(5, 10, 5)
      assertManifest(tablePath, expectSameFiles = true, expectedNumFiles = 50)

      // Remove all data
      writePartitioned(0, 1, 1)
      assert(spark.read.format("delta").load(tablePath.toString).count() == 0)
      assertManifest(tablePath, expectSameFiles = true, expectedNumFiles = 0)
    }
  }

  test("incremental manifest: generate full manifest if manifest did not exist") {
    withTempDir { tablePath =>

      def write(numPartitions: Int): Unit = {
        spark.range(0, 100, 1, 1).toDF("value").withColumn("part", $"value" % numPartitions)
          .write.format("delta").partitionBy("part").mode("append").save(tablePath.toString)
      }

      write(10)
      assertManifest(tablePath, expectSameFiles = false, expectedNumFiles = 0)

      withIncrementalManifest(tablePath, enabled = true) {
        write(1)  // update only one partition
      }
      // Manifests should be generated for all partitions
      assertManifest(tablePath, expectSameFiles = true, expectedNumFiles = 11)
    }
  }

  test("incremental manifest: failure to generate manifest throws exception") {
    withTempDir { tablePath =>
      tablePath.delete()

      import SymlinkManifestFailureTestFileSystem._

      withSQLConf(
          s"fs.$SCHEME.impl" -> classOf[SymlinkManifestFailureTestFileSystem].getName,
          s"fs.$SCHEME.impl.disable.cache" -> "true",
          s"fs.AbstractFileSystem.$SCHEME.impl" ->
            classOf[SymlinkManifestFailureTestAbstractFileSystem].getName,
          s"fs.AbstractFileSystem.$SCHEME.impl.disable.cache" -> "true") {
        def write(numFiles: Int): Unit = withIncrementalManifest(tablePath, enabled = true) {
          spark.createDataset(spark.sparkContext.parallelize(1 to 100, numFiles))
            .write.format("delta").mode("overwrite").save(s"$SCHEME://$tablePath")
        }

        val manifestPath = new File(tablePath, GenerateSymlinkManifest.MANIFEST_LOCATION)
        require(!manifestPath.exists())
        write(1) // first write enables the property does not write any file
        require(!manifestPath.exists())

        val ex = catalyst.util.quietly {
          intercept[RuntimeException] { write(2) }
        }

        assert(ex.getMessage().contains(GenerateSymlinkManifest.name))
        assert(ex.getCause().toString.contains("Test exception"))
      }
    }
  }

  test("special partition column names") {

    def assertColNames(inputStr: String): Unit = withClue(s"input: $inputStr") {
      withTempDir { tablePath =>
        tablePath.delete()
        val inputLines = inputStr.trim.stripMargin.trim.split("\n").toSeq
        require(inputLines.size > 0)
        val input = spark.read.json(inputLines.toDS)
        val partitionCols = input.schema.fieldNames
        val inputWithValue = input.withColumn("value", lit(1))

        inputWithValue.write.format("delta").partitionBy(partitionCols: _*).save(tablePath.toString)
        generateSymlinkManifest(tablePath.toString)
        assertManifest(tablePath, expectSameFiles = true, expectedNumFiles = inputLines.size)
      }
    }

    intercept[AnalysisException] {
      assertColNames("""{ " " : 0 }""")
    }
    assertColNames("""{ "%" : 0 }""")
    assertColNames("""{ "a.b." : 0 }""")
    assertColNames("""{ "a/b." : 0 }""")
    assertColNames("""{ "a_b" : 0 }""")
    intercept[AnalysisException] {
      assertColNames("""{ "a b" : 0 }""")
    }
  }

  test("special partition column values") {
    withTempDir { tablePath =>
      tablePath.delete()
      val inputStr = """
          |{ "part1" : 1,    "part2": "$0$", "value" : 1 }
          |{ "part1" : null, "part2": "_1_", "value" : 1 }
          |{ "part1" : 1,    "part2": "",    "value" : 1 }
          |{ "part1" : null, "part2": " ",   "value" : 1 }
          |{ "part1" : 1,    "part2": "  ",  "value" : 1 }
          |{ "part1" : null, "part2": "/",   "value" : 1 }
          |{ "part1" : 1,    "part2": null,  "value" : 1 }
          |"""
      val input = spark.read.json(inputStr.trim.stripMargin.trim.split("\n").toSeq.toDS)
      input.write.format("delta").partitionBy("part1", "part2").save(tablePath.toString)
      generateSymlinkManifest(tablePath.toString)
      assertManifest(tablePath, expectSameFiles = true, expectedNumFiles = 7)
    }
  }

  test("root table path with escapable chars like space") {
    withTempDir { p =>
      val tablePath = new File(p.toString, "path with space")
      spark.createDataset(spark.sparkContext.parallelize(1 to 100, 1)).toDF("value")
        .withColumn("part", $"value" % 2)
        .write.format("delta").partitionBy("part").save(tablePath.toString)

      generateSymlinkManifest(tablePath.toString)
      assertManifest(tablePath, expectSameFiles = true, expectedNumFiles = 2)
    }
  }

  test("block manifest generation with persistent DVs") {
    withDeletionVectorsEnabled() {
      val rowsToBeRemoved = Seq(1L, 42L, 43L)

      withTempDir { dir =>
        val tablePath = dir.getAbsolutePath
        // Write in 2 files.
        spark.range(end = 50L).toDF("id").coalesce(1)
          .write.format("delta").mode("overwrite").save(tablePath)
        spark.range(start = 50L, end = 100L).toDF("id").coalesce(1)
          .write.format("delta").mode("append").save(tablePath)
        val deltaLog = DeltaLog.forTable(spark, tablePath)
        assert(deltaLog.snapshot.allFiles.count() === 2L)

        // Step 1: Make sure generation works on DV enabled tables without a DV in the snapshot.
        // Delete an entire file, which can't produce DVs.
        spark.sql(s"""DELETE FROM delta.`$tablePath` WHERE id BETWEEN 0 and 49""")
        val remainingFiles = deltaLog.snapshot.allFiles.collect()
        assert(remainingFiles.size === 1L)
        assert(remainingFiles(0).deletionVector === null)
        // Should work fine, since the snapshot doesn't contain DVs.
        spark.sql(s"""GENERATE symlink_format_manifest FOR TABLE delta.`$tablePath`""")

        // Step 2: Make sure generation fails if there are DVs in the snapshot.

        // This is needed to make the manual commit work correctly, since we are not actually
        // running a command that produces metrics.
        withSQLConf(DeltaSQLConf.DELTA_HISTORY_METRICS_ENABLED.key -> "false") {
          val txn = deltaLog.startTransaction()
          assert(txn.snapshot.allFiles.count() === 1)
          val file = txn.snapshot.allFiles.collect().head
          val actions = removeRowsFromFileUsingDV(deltaLog, file, rowIds = rowsToBeRemoved)
          txn.commit(actions, Delete(predicate = Seq.empty))
        }
        val e = intercept[DeltaCommandUnsupportedWithDeletionVectorsException] {
          spark.sql(s"""GENERATE symlink_format_manifest FOR TABLE delta.`$tablePath`""")
        }
        checkErrorHelper(
          exception = e,
          errorClass = "DELTA_UNSUPPORTED_GENERATE_WITH_DELETION_VECTORS")
      }
    }
  }

  private def setEnabledIncrementalManifest(tablePath: String, enabled: Boolean): Unit = {
    spark.sql(s"ALTER TABLE delta.`$tablePath` " +
      s"SET TBLPROPERTIES('${DeltaConfigs.SYMLINK_FORMAT_MANIFEST_ENABLED.key}'='$enabled')")
  }

  test("block incremental manifest generation with persistent DVs") {
    import DeltaTablePropertyValidationFailedSubClass._

    def expectConstraintViolation(subClass: DeltaTablePropertyValidationFailedSubClass)
        (thunk: => Unit): Unit = {
      val e = intercept[DeltaTablePropertyValidationFailedException] {
        thunk
      }
      checkErrorHelper(
        exception = e,
        errorClass = "DELTA_VIOLATE_TABLE_PROPERTY_VALIDATION_FAILED." + subClass.tag
      )
    }

    withDeletionVectorsEnabled() {
      val rowsToBeRemoved = Seq(1L, 42L, 43L)

      withTempDir { dir =>
        val tablePath = dir.getAbsolutePath
        spark.range(end = 100L).toDF("id").coalesce(1)
          .write.format("delta").mode("overwrite").save(tablePath)
        val deltaLog = DeltaLog.forTable(spark, tablePath)

        // Make sure both properties can't be enabled together.
        enableDeletionVectorsInTable(new Path(tablePath), enable = true)
        expectConstraintViolation(
            subClass = PersistentDeletionVectorsWithIncrementalManifestGeneration) {
          setEnabledIncrementalManifest(tablePath, enabled = true)
        }
        // Or in the other order.
        enableDeletionVectorsInTable(new Path(tablePath), enable = false)
        setEnabledIncrementalManifest(tablePath, enabled = true)
        expectConstraintViolation(
            subClass = PersistentDeletionVectorsWithIncrementalManifestGeneration)  {
          enableDeletionVectorsInTable(new Path(tablePath), enable = true)
        }
        setEnabledIncrementalManifest(tablePath, enabled = false)
        // Or both at once.
        expectConstraintViolation(
            subClass = PersistentDeletionVectorsWithIncrementalManifestGeneration)  {
          spark.sql(s"ALTER TABLE delta.`$tablePath` " +
            s"SET TBLPROPERTIES('${DeltaConfigs.SYMLINK_FORMAT_MANIFEST_ENABLED.key}'='true'," +
            s" '${DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.key}' = 'true')")
        }

        // If DVs were allowed at some point and are still present in the table,
        // enabling incremental manifest generation must still fail.
        enableDeletionVectorsInTable(new Path(tablePath), enable = true)
        withSQLConf(DeltaSQLConf.DELTA_HISTORY_METRICS_ENABLED.key -> "false") {
          val txn = deltaLog.startTransaction()
          assert(txn.snapshot.allFiles.count() === 1)
          val file = txn.snapshot.allFiles.collect().head
          val actions = removeRowsFromFileUsingDV(deltaLog, file, rowIds = rowsToBeRemoved)
          txn.commit(actions, Delete(predicate = Seq.empty))
        }
        assert(getFilesWithDeletionVectors(deltaLog).nonEmpty)
        enableDeletionVectorsInTable(new Path(tablePath), enable = false)
        expectConstraintViolation(
            subClass = ExistingDeletionVectorsWithIncrementalManifestGeneration)  {
          setEnabledIncrementalManifest(tablePath, enabled = true)
        }
        // Purge
        spark.sql(s"REORG TABLE delta.`$tablePath` APPLY (PURGE)")
        assert(getFilesWithDeletionVectors(deltaLog).isEmpty)
        // Now it should work.
        setEnabledIncrementalManifest(tablePath, enabled = true)

        // As a last fallback, in case some other writer put the table into an illegal state,
        // we still need to fail the manifest generation if there are DVs.
        // Reset table.
        setEnabledIncrementalManifest(tablePath, enabled = false)
        enableDeletionVectorsInTable(new Path(tablePath), enable = false)
        spark.range(end = 100L).toDF("id").coalesce(1)
          .write.format("delta").mode("overwrite").save(tablePath)
        // Add DVs
        enableDeletionVectorsInTable(new Path(tablePath), enable = true)
        withSQLConf(DeltaSQLConf.DELTA_HISTORY_METRICS_ENABLED.key -> "false") {
          val txn = deltaLog.startTransaction()
          assert(txn.snapshot.allFiles.count() === 1)
          val file = txn.snapshot.allFiles.collect().head
          val actions = removeRowsFromFileUsingDV(deltaLog, file, rowIds = rowsToBeRemoved)
          txn.commit(actions, Delete(predicate = Seq.empty))
        }
        // Force enable manifest generation.
        withSQLConf(DeltaSQLConf.DELTA_TABLE_PROPERTY_CONSTRAINTS_CHECK_ENABLED.key -> "false") {
          setEnabledIncrementalManifest(tablePath, enabled = true)
        }
        val e2 = intercept[DeltaCommandUnsupportedWithDeletionVectorsException] {
          spark.range(10).write.format("delta").mode("append").save(tablePath)
        }
        checkErrorHelper(
          exception = e2,
          errorClass = "DELTA_UNSUPPORTED_GENERATE_WITH_DELETION_VECTORS")
        // This is fine, since the new snapshot won't contain DVs.
        spark.range(10).write.format("delta").mode("overwrite").save(tablePath)

        // Make sure we can get the table back into a consistent state, as well
        setEnabledIncrementalManifest(tablePath, enabled = false)
        // No more exception.
        spark.range(10).write.format("delta").mode("append").save(tablePath)
      }
    }
  }

  private def checkErrorHelper(
      exception: SparkThrowable,
      errorClass: String
  ): Unit = {
    assert(exception.getErrorClass === errorClass,
      s"Expected errorClass $errorClass, but got $exception")
  }

  Seq(true, false).foreach { useIncremental =>
    test(s"delete partition column with special char - incremental=$useIncremental") {

      def writePartition(dir: File, partName: String): Unit = {
        spark.range(10)
          .withColumn("part", lit(partName))
          .repartition(1)
          .write
          .format("delta")
          .mode("append")
          .partitionBy("part")
          .save(dir.toString)
      }

      withTempDir { dir =>
        // create table and write first manifest
        writePartition(dir, "noSpace")
        generateSymlinkManifest(dir.toString)

        withIncrementalManifest(dir, useIncremental) {
          // 1. test paths with spaces
          writePartition(dir, "yes space")

          if (!useIncremental) { generateSymlinkManifest(dir.toString) }
          assertManifest(dir, expectSameFiles = true, expectedNumFiles = 2)

          // delete partition
          sql(s"""DELETE FROM delta.`${dir.toString}` WHERE part="yes space";""")

          if (!useIncremental) { generateSymlinkManifest(dir.toString) }
          assertManifest(dir, expectSameFiles = true, expectedNumFiles = 1)

          // 2. test special characters
          // scalastyle:off nonascii
          writePartition(dir, "库尔 勒")
          if (!useIncremental) { generateSymlinkManifest(dir.toString) }
          assertManifest(dir, expectSameFiles = true, expectedNumFiles = 2)

          // delete partition
          sql(s"""DELETE FROM delta.`${dir.toString}` WHERE part="库尔 勒";""")
          // scalastyle:on nonascii

          if (!useIncremental) { generateSymlinkManifest(dir.toString) }
          assertManifest(dir, expectSameFiles = true, expectedNumFiles = 1)
        }
      }
    }
  }

  /**
   * Assert that the manifest files in the table meet the expectations.
   * @param tablePath Path of the Delta table
   * @param expectSameFiles Expect that the manifest files contain the same data files
   *                        as the latest version of the table
   * @param expectedNumFiles Expected number of manifest files
   */
  def assertManifest(
      tablePath: File,
      expectSameFiles: Boolean,
      expectedNumFiles: Int): Unit = {
    val deltaSnapshot = DeltaLog.forTable(spark, tablePath.toString).update()
    val manifestPath = new File(tablePath, GenerateSymlinkManifest.MANIFEST_LOCATION)

    if (!manifestPath.exists) {
      assert(expectedNumFiles == 0 && !expectSameFiles)
      return
    }

    // Validate the expected number of files are present in the manifest
    val filesInManifest = spark.read.text(manifestPath.toString).select("value").as[String]
      .map { _.stripPrefix("file:") }.toDF("file")
    assert(filesInManifest.count() == expectedNumFiles)

    // Validate that files in the latest version of DeltaLog is same as those in the manifest
    val filesInLog = deltaSnapshot.allFiles.map { addFile =>
      // Note: this unescapes the relative path in `addFile`
      DeltaFileOperations.absolutePath(tablePath.toString, addFile.path).toString
    }.toDF("file")
    if (expectSameFiles) {
      checkAnswer(filesInManifest, filesInLog.toDF())

      // Validate that each file in the manifest is actually present in table. This mainly checks
      // whether the file names in manifest are not escaped and therefore are readable directly
      // by Hadoop APIs.
      val fs = new Path(manifestPath.toString)
        .getFileSystem(deltaSnapshot.deltaLog.newDeltaHadoopConf())
      spark.read.text(manifestPath.toString).select("value").as[String].collect().foreach { p =>
        assert(fs.exists(new Path(p)), s"path $p in manifest not found in file system")
      }
    } else {
      assert(filesInManifest.as[String].collect().toSet != filesInLog.as[String].collect().toSet)
    }

    // If there are partitioned files, make sure the partitions values read from them are the
    // same as those in the table.
    val partitionCols = deltaSnapshot.metadata.partitionColumns.map(x => s"`$x`")
    if (partitionCols.nonEmpty && expectSameFiles && expectedNumFiles > 0) {
      val partitionsInManifest = spark.read.text(manifestPath.toString)
        .selectExpr(partitionCols: _*).distinct()
      val partitionsInData = spark.read.format("delta").load(tablePath.toString)
        .selectExpr(partitionCols: _*).distinct()
      checkAnswer(partitionsInManifest, partitionsInData)
    }
  }

  protected def withIncrementalManifest(tablePath: File, enabled: Boolean)(func: => Unit): Unit = {
    if (tablePath.exists()) {
      val latestMetadata = DeltaLog.forTable(spark, tablePath).update().metadata
      if (DeltaConfigs.SYMLINK_FORMAT_MANIFEST_ENABLED.fromMetaData(latestMetadata) != enabled) {
        spark.sql(s"ALTER TABLE delta.`$tablePath` " +
          s"SET TBLPROPERTIES(${DeltaConfigs.SYMLINK_FORMAT_MANIFEST_ENABLED.key}=$enabled)")
      }
    }
    func
  }

  protected def generateSymlinkManifest(tablePath: String): Unit = {
    val deltaLog = DeltaLog.forTable(spark, tablePath)
    GenerateSymlinkManifest.generateFullManifest(spark, deltaLog)
  }
}

class SymlinkManifestFailureTestAbstractFileSystem(
    uri: URI,
    conf: org.apache.hadoop.conf.Configuration)
  extends org.apache.hadoop.fs.DelegateToFileSystem(
    uri,
    new SymlinkManifestFailureTestFileSystem,
    conf,
    SymlinkManifestFailureTestFileSystem.SCHEME,
    false) {

  // Implementation copied from RawLocalFs
  import org.apache.hadoop.fs.local.LocalConfigKeys
  import org.apache.hadoop.fs._

  override def getUriDefaultPort(): Int = -1
  override def getServerDefaults(): FsServerDefaults = LocalConfigKeys.getServerDefaults()
  override def isValidName(src: String): Boolean = true
}


class SymlinkManifestFailureTestFileSystem extends RawLocalFileSystem {

  private var uri: URI = _
  override def getScheme: String = SymlinkManifestFailureTestFileSystem.SCHEME

  override def initialize(name: URI, conf: Configuration): Unit = {
    uri = URI.create(name.getScheme + ":///")
    super.initialize(name, conf)
  }

  override def getUri(): URI = if (uri == null) {
    // RawLocalFileSystem's constructor will call this one before `initialize` is called.
    // Just return the super's URI to avoid NPE.
    super.getUri
  } else {
    uri
  }

  // Override both create() method defined in RawLocalFileSystem such that any file creation
  // throws error.

  override def create(
      path: Path,
      overwrite: Boolean,
      bufferSize: Int,
      replication: Short,
      blockSize: Long,
      progress: Progressable): FSDataOutputStream = {
    if (path.toString.contains(GenerateSymlinkManifest.MANIFEST_LOCATION)) {
      throw new RuntimeException("Test exception")
    }
    super.create(path, overwrite, bufferSize, replication, blockSize, null)
  }

  override def create(
      path: Path,
      permission: FsPermission,
      overwrite: Boolean,
      bufferSize: Int,
      replication: Short,
      blockSize: Long,
      progress: Progressable): FSDataOutputStream = {
    if (path.toString.contains(GenerateSymlinkManifest.MANIFEST_LOCATION)) {
      throw new RuntimeException("Test exception")
    }
    super.create(path, permission, overwrite, bufferSize, replication, blockSize, progress)
  }
}

object SymlinkManifestFailureTestFileSystem {
  val SCHEME = "testScheme"
}

