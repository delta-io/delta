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

package io.delta.tables

import java.io.File
import java.util.Locale

import scala.language.postfixOps

// scalastyle:off import.ordering.noEmptyLine
import org.apache.spark.sql.delta.{AppendOnlyTableFeature, DeltaIllegalArgumentException, DeltaLog, DeltaTableFeatureException, FakeFileSystem, InvariantsTableFeature, TestReaderWriterFeature, TestRemovableReaderWriterFeature, TestRemovableWriterFeature, TestWriterFeature}
import org.apache.spark.sql.delta.actions.{ Metadata, Protocol }
import org.apache.spark.sql.delta.storage.LocalLogStore
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.test.DeltaTestImplicits._
import org.apache.spark.sql.delta.util.FileNames
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.{Path, UnsupportedFileSystemException}

import org.apache.spark.SparkException
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.sql.{functions, AnalysisException, DataFrame, Dataset, QueryTest, Row}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.Utils

class DeltaTableSuite extends QueryTest
  with SharedSparkSession
  with DeltaSQLCommandTest {

  test("forPath") {
    withTempDir { dir =>
      testData.write.format("delta").save(dir.getAbsolutePath)
      checkAnswer(
        DeltaTable.forPath(spark, dir.getAbsolutePath).toDF,
        testData.collect().toSeq)
      checkAnswer(
        DeltaTable.forPath(dir.getAbsolutePath).toDF,
        testData.collect().toSeq)
    }
  }

  test("forPath - with non-Delta table path") {
    val msg = "not a delta table"
    withTempDir { dir =>
      testData.write.format("parquet").mode("overwrite").save(dir.getAbsolutePath)
      testError(msg) { DeltaTable.forPath(spark, dir.getAbsolutePath) }
      testError(msg) { DeltaTable.forPath(dir.getAbsolutePath) }
    }
  }

  test("forName") {
    withTempDir { dir =>
      withTable("deltaTable") {
        testData.write.format("delta").saveAsTable("deltaTable")

        checkAnswer(
          DeltaTable.forName(spark, "deltaTable").toDF,
          testData.collect().toSeq)
        checkAnswer(
          DeltaTable.forName("deltaTable").toDF,
          testData.collect().toSeq)

      }
    }
  }

  def testForNameOnNonDeltaName(tableName: String): Unit = {
    val msg = "not a Delta table"
    testError(msg) { DeltaTable.forName(spark, tableName) }
    testError(msg) { DeltaTable.forName(tableName) }
  }

  test("forName - with non-Delta table name") {
    withTempDir { dir =>
      withTable("notADeltaTable") {
        testData.write.format("parquet").mode("overwrite").saveAsTable("notADeltaTable")
        testForNameOnNonDeltaName("notADeltaTable")
      }
    }
  }

  test("forName - with temp view name") {
    withTempDir { dir =>
      withTempView("viewOnDeltaTable") {
        testData.write.format("delta").save(dir.getAbsolutePath)
        spark.read.format("delta").load(dir.getAbsolutePath)
          .createOrReplaceTempView("viewOnDeltaTable")
        testForNameOnNonDeltaName("viewOnDeltaTable")
      }
    }
  }

  test("forName - with delta.`path`") {
    // for name should work on Delta table paths
    withTempDir { dir =>
      testData.write.format("delta").save(dir.getAbsolutePath)
      checkAnswer(
        DeltaTable.forName(spark, s"delta.`$dir`").toDF,
        testData.collect().toSeq)
      checkAnswer(
        DeltaTable.forName(s"delta.`$dir`").toDF,
        testData.collect().toSeq)
    }

    // using forName on non Delta Table paths should fail
    withTempDir { dir =>
      testForNameOnNonDeltaName(s"delta.`$dir`")

      testData.write.format("parquet").mode("overwrite").save(dir.getAbsolutePath)
      testForNameOnNonDeltaName(s"delta.`$dir`")
    }
  }

  test("as") {
    withTempDir { dir =>
      testData.write.format("delta").save(dir.getAbsolutePath)
      checkAnswer(
        DeltaTable.forPath(dir.getAbsolutePath).as("tbl").toDF.select("tbl.value"),
        testData.select("value").collect().toSeq)
    }
  }

  test("isDeltaTable - path - with _delta_log dir") {
    withTempDir { dir =>
      testData.write.format("delta").save(dir.getAbsolutePath)
      assert(DeltaTable.isDeltaTable(dir.getAbsolutePath))
    }
  }

  test("isDeltaTable - path - with empty _delta_log dir") {
    withTempDir { dir =>
      new File(dir, "_delta_log").mkdirs()
      assert(!DeltaTable.isDeltaTable(dir.getAbsolutePath))
    }
  }

  test("isDeltaTable - path - with no _delta_log dir") {
    withTempDir { dir =>
      assert(!DeltaTable.isDeltaTable(dir.getAbsolutePath))
    }
  }

  test("isDeltaTable - path - with non-existent dir") {
    withTempDir { dir =>
      JavaUtils.deleteRecursively(dir)
      assert(!DeltaTable.isDeltaTable(dir.getAbsolutePath))
    }
  }

  test("isDeltaTable - with non-Delta table path") {
    withTempDir { dir =>
      testData.write.format("parquet").mode("overwrite").save(dir.getAbsolutePath)
      assert(!DeltaTable.isDeltaTable(dir.getAbsolutePath))
    }
  }

  def testError(expectedMsg: String)(thunk: => Unit): Unit = {
    val e = intercept[AnalysisException] { thunk }
    assert(e.getMessage.toLowerCase(Locale.ROOT).contains(expectedMsg.toLowerCase(Locale.ROOT)))
  }

  test("DeltaTable is Java Serializable but cannot be used in executors") {
    import testImplicits._

    // DeltaTable can be passed to executor without method calls.
    withTempDir { dir =>
      testData.write.format("delta").mode("append").save(dir.getAbsolutePath)
      val dt: DeltaTable = DeltaTable.forPath(dir.getAbsolutePath)
      spark.range(5).as[Long].map{ row: Long =>
        val foo = dt
        row + 3
      }.count()
    }

    // DeltaTable can be passed to executor but method call causes exception.
    val e = intercept[Exception] {
      withTempDir { dir =>
        testData.write.format("delta").mode("append").save(dir.getAbsolutePath)
        val dt: DeltaTable = DeltaTable.forPath(dir.getAbsolutePath)
        spark.range(5).as[Long].map{ row: Long =>
          dt.toDF
          row + 3
        }.count()
      }
    }.getMessage
    assert(e.contains("DeltaTable cannot be used in executors"))
  }
}

class DeltaTableHadoopOptionsSuite extends QueryTest
  with SharedSparkSession
  with DeltaSQLCommandTest {

  import testImplicits._

  protected override def sparkConf =
    super.sparkConf.set("spark.delta.logStore.fake.impl", classOf[LocalLogStore].getName)

  /**
   * Create Hadoop file system options for `FakeFileSystem`. If Delta doesn't pick up them,
   * it won't be able to read/write any files using `fake://`.
   */
  private def fakeFileSystemOptions: Map[String, String] = {
    Map(
      "fs.fake.impl" -> classOf[FakeFileSystem].getName,
      "fs.fake.impl.disable.cache" -> "true"
    )
  }

  /** Create a fake file system path to test from the dir path. */
  private def fakeFileSystemPath(dir: File): String = s"fake://${dir.getCanonicalPath}"

  private def readDeltaTableByPath(path: String): DataFrame = {
    spark.read.options(fakeFileSystemOptions).format("delta").load(path)
  }

  // Ensure any new API from [[DeltaTable]] has to verify it can work with custom file system
  // options.
  private val publicMethods =
  scala.reflect.runtime.universe.typeTag[io.delta.tables.DeltaTable].tpe.decls
    .filter(_.isPublic)
    .map(_.name.toString).toSet

  private val ignoreMethods = Seq()

  private val testedMethods = Seq(
    "addFeatureSupport",
    "as",
    "alias",
    "clone",
    "cloneAtTimestamp",
    "cloneAtVersion",
    "delete",
    "detail",
    "dropFeatureSupport",
    "generate",
    "history",
    "merge",
    "optimize",
    "restoreToVersion",
    "restoreToTimestamp",
    "toDF",
    "update",
    "updateExpr",
    "upgradeTableProtocol",
    "vacuum"
  )

  val untestedMethods = publicMethods -- ignoreMethods -- testedMethods
  assert(
    untestedMethods.isEmpty,
    s"Found new methods added to DeltaTable: $untestedMethods. " +
      "Please make sure you add a new test to verify it works with file system " +
      "options in this file, and update the `testedMethods` list. " +
      "If this new method doesn't need to support file system options, " +
      "you can add it to the `ignoredMethods` list")

  test("forPath: as/alias/toDF with filesystem options.") {
    withTempDir { dir =>
      val path = fakeFileSystemPath(dir)
      val fsOptions = fakeFileSystemOptions
      testData.write.options(fsOptions).format("delta").save(path)

      checkAnswer(
        DeltaTable.forPath(spark, path, fsOptions).as("tbl").toDF.select("tbl.value"),
        testData.select("value").collect().toSeq)

      checkAnswer(
        DeltaTable.forPath(spark, path, fsOptions).alias("tbl").toDF.select("tbl.value"),
        testData.select("value").collect().toSeq)
    }
  }

  test("forPath with unsupported options") {
    withTempDir { dir =>
      val path = fakeFileSystemPath(dir)
      val fsOptions = fakeFileSystemOptions
      testData.write.options(fsOptions).format("delta").save(path)

      val finalOptions = fsOptions + ("otherKey" -> "otherVal")
      assertThrows[DeltaIllegalArgumentException] {
        io.delta.tables.DeltaTable.forPath(spark, path, finalOptions)
      }
    }
  }

  test("forPath error out without filesystem options passed in.") {
    withTempDir { dir =>
      val path = fakeFileSystemPath(dir)
      val fsOptions = fakeFileSystemOptions
      testData.write.options(fsOptions).format("delta").save(path)

      val e = intercept[UnsupportedFileSystemException] {
        io.delta.tables.DeltaTable.forPath(spark, path)
      }.getMessage

      assert(e.contains("""No FileSystem for scheme "fake""""))
    }
  }

  test("forPath - with filesystem options") {
    withTempDir { dir =>
      val path = fakeFileSystemPath(dir)
      val fsOptions = fakeFileSystemOptions
      testData.write.options(fsOptions).format("delta").save(path)

      val deltaTable =
        io.delta.tables.DeltaTable.forPath(spark, path, fsOptions)

      val testDataSeq = testData.collect().toSeq

      // verify table can be read
      checkAnswer(deltaTable.toDF, testDataSeq)

      // verify java friendly API.
      import scala.collection.JavaConverters._
      val deltaTable2 = io.delta.tables.DeltaTable.forPath(
        spark, path, new java.util.HashMap[String, String](fsOptions.asJava))
      checkAnswer(deltaTable2.toDF, testDataSeq)
    }
  }

  test("updateExpr - with filesystem options") {
    withTempDir { dir =>
      val path = fakeFileSystemPath(dir)
      val fsOptions = fakeFileSystemOptions
      val df = Seq((1, 10), (2, 20), (3, 30), (4, 40)).toDF("key", "value")
      df.write.options(fsOptions).format("delta").save(path)

      val table = io.delta.tables.DeltaTable.forPath(spark, path, fsOptions)

      table.updateExpr(Map("key" -> "100"))

      checkAnswer(readDeltaTableByPath(path),
        Row(100, 10) :: Row(100, 20) :: Row(100, 30) :: Row(100, 40) :: Nil)
    }
  }

  test("update - with filesystem options") {
    withTempDir { dir =>
      val path = fakeFileSystemPath(dir)
      val df = Seq((1, 10), (2, 20), (3, 30), (4, 40)).toDF("key", "value")
      df.write.options(fakeFileSystemOptions).format("delta").save(path)

      val table = io.delta.tables.DeltaTable.forPath(spark, path, fakeFileSystemOptions)

      table.update(Map("key" -> functions.expr("100")))

      checkAnswer(readDeltaTableByPath(path),
        Row(100, 10) :: Row(100, 20) :: Row(100, 30) :: Row(100, 40) :: Nil)
    }
  }

  test("delete - with filesystem options") {
    withTempDir { dir =>
      val path = fakeFileSystemPath(dir)
      val df = Seq((1, 10), (2, 20), (3, 30), (4, 40)).toDF("key", "value")
      df.write.options(fakeFileSystemOptions).format("delta").save(path)

      val table = io.delta.tables.DeltaTable.forPath(spark, path, fakeFileSystemOptions)

      table.delete(functions.expr("key = 1 or key = 2"))

      checkAnswer(readDeltaTableByPath(path), Row(3, 30) :: Row(4, 40) :: Nil)
    }
  }

  test("merge - with filesystem options") {
    withTempDir { dir =>
      val path = fakeFileSystemPath(dir)
      val target = Seq((1, 10), (2, 20)).toDF("key1", "value1")
      target.write.options(fakeFileSystemOptions).format("delta").save(path)
      val source = Seq((1, 100), (3, 30)).toDF("key2", "value2")

      val table = io.delta.tables.DeltaTable.forPath(spark, path, fakeFileSystemOptions)

      table.merge(source, "key1 = key2")
        .whenMatched().updateExpr(Map("key1" -> "key2", "value1" -> "value2"))
        .whenNotMatched().insertExpr(Map("key1" -> "key2", "value1" -> "value2"))
        .execute()

      checkAnswer(readDeltaTableByPath(path), Row(1, 100) :: Row(2, 20) :: Row(3, 30) :: Nil)
    }
  }

  test("vacuum - with filesystem options") {
    // Note: verify that [DeltaTableUtils.findDeltaTableRoot] works when either
    // DELTA_FORMAT_CHECK_CACHE_ENABLED is on or off.
    Seq("true", "false").foreach{ deltaFormatCheckEnabled =>
      withSQLConf(
        "spark.databricks.delta.formatCheck.cache.enabled" -> deltaFormatCheckEnabled) {
        withTempDir { dir =>
          val path = fakeFileSystemPath(dir)
          testData.write.options(fakeFileSystemOptions).format("delta").save(path)
          val table = io.delta.tables.DeltaTable.forPath(spark, path, fakeFileSystemOptions)

          // create a uncommitted file.
          val notCommittedFile = "notCommittedFile.json"
          val file = new File(dir, notCommittedFile)
          FileUtils.write(file, "gibberish")
          // set to ancient time so that the file is eligible to be vacuumed.
          file.setLastModified(0)
          assert(file.exists())

          table.vacuum()

          val file2 = new File(dir, notCommittedFile)
          assert(!file2.exists())
        }
      }
    }
  }

  test("clone - with filesystem options") {
    withTempDir { dir =>
      val baseDir = fakeFileSystemPath(dir)

      val srcDir = new File(baseDir, "source").getCanonicalPath
      val dstDir = new File(baseDir, "destination").getCanonicalPath

      spark.range(10).write.options(fakeFileSystemOptions).format("delta").save(srcDir)

      val srcTable =
        io.delta.tables.DeltaTable.forPath(spark, srcDir, fakeFileSystemOptions)
      srcTable.clone(dstDir)

      val srcLog = DeltaLog.forTable(spark, new Path(srcDir), fakeFileSystemOptions)
      val dstLog = DeltaLog.forTable(spark, new Path(dstDir), fakeFileSystemOptions)

      checkAnswer(
        spark.baseRelationToDataFrame(srcLog.createRelation()),
        spark.baseRelationToDataFrame(dstLog.createRelation())
      )
    }
  }

  test("cloneAtVersion/timestamp - with filesystem options") {
    Seq(true, false).foreach { cloneWithVersion =>
      withTempDir { dir =>
        val baseDir = fakeFileSystemPath(dir)
        val fsOptions = fakeFileSystemOptions

        val srcDir = new File(baseDir, "source").getCanonicalPath
        val dstDir = new File(baseDir, "destination").getCanonicalPath

        val df1 = Seq(1, 2, 3).toDF("id")
        val df2 = Seq(4, 5).toDF("id")
        val df3 = Seq(6, 7).toDF("id")

        // version 0.
        df1.write.format("delta").options(fsOptions).save(srcDir)

        // version 1.
        df2.write.format("delta").options(fsOptions).mode("append").save(srcDir)

        // version 2.
        df3.write.format("delta").options(fsOptions).mode("append").save(srcDir)

        val srcTable =
          io.delta.tables.DeltaTable.forPath(spark, srcDir, fakeFileSystemOptions)

        if (cloneWithVersion) {
          srcTable.cloneAtVersion(0, dstDir)
        } else {
          // clone with timestamp.
          //
          // set the time to first file with a early time and verify the delta table can be
          // restored to it.
          val desiredTime = "1983-01-01"
          val format = new java.text.SimpleDateFormat("yyyy-MM-dd")
          val time = format.parse(desiredTime).getTime

          val logPath = new Path(srcDir, "_delta_log")
          val file = new File(FileNames.unsafeDeltaFile(logPath, 0).toString)
          assert(file.setLastModified(time))
          srcTable.cloneAtTimestamp(desiredTime, dstDir)
        }

        val dstLog = DeltaLog.forTable(spark, new Path(dstDir), fakeFileSystemOptions)

        checkAnswer(
          df1,
          spark.baseRelationToDataFrame(dstLog.createRelation())
        )
      }
    }
  }

  test("optimize - with filesystem options") {
    withTempDir { dir =>
      val path = fakeFileSystemPath(dir)
      val fsOptions = fakeFileSystemOptions

      Seq(1, 2, 3).toDF().write.options(fsOptions).format("delta").save(path)
      Seq(4, 5, 6)
        .toDF().write.options(fsOptions).format("delta").mode("append").save(path)

      val origData: DataFrame = spark.read.options(fsOptions).format("delta").load(path)

      val deltaLog = DeltaLog.forTable(spark, new Path(path), fsOptions)
      val table = io.delta.tables.DeltaTable.forPath(spark, path, fsOptions)
      val versionBeforeOptimize = deltaLog.snapshot.version

      table.optimize().executeCompaction()
      deltaLog.update()
      assert(deltaLog.snapshot.version == versionBeforeOptimize + 1)
      checkDatasetUnorderly(origData.as[Int], 1, 2, 3, 4, 5, 6)
    }
  }

  test("history - with filesystem options") {
    withTempDir { dir =>
      val path = fakeFileSystemPath(dir)
      val fsOptions = fakeFileSystemOptions

      Seq(1, 2, 3).toDF().write.options(fsOptions).format("delta").save(path)

      val table = io.delta.tables.DeltaTable.forPath(spark, path, fsOptions)
      table.history().collect()
    }
  }

  test("generate - with filesystem options") {
    withSQLConf("spark.databricks.delta.symlinkFormatManifest.fileSystemCheck.enabled" -> "false") {
      withTempDir { dir =>
        val path = fakeFileSystemPath(dir)
        val fsOptions = fakeFileSystemOptions

        Seq(1, 2, 3).toDF().write.options(fsOptions).format("delta").save(path)

        val table = io.delta.tables.DeltaTable.forPath(spark, path, fsOptions)
        table.generate("symlink_format_manifest")
      }
    }
  }

  test("restoreTable - with filesystem options") {
    withSQLConf("spark.databricks.service.checkSerialization" -> "false") {
      withTempDir { dir =>
        val path = fakeFileSystemPath(dir)
        val fsOptions = fakeFileSystemOptions

        val df1 = Seq(1, 2, 3).toDF("id")
        val df2 = Seq(4, 5).toDF("id")
        val df3 = Seq(6, 7).toDF("id")

        // version 0.
        df1.write.format("delta").options(fsOptions).save(path)
        val deltaLog = DeltaLog.forTable(spark, new Path(path), fsOptions)
        assert(deltaLog.snapshot.version == 0)

        // version 1.
        df2.write.format("delta").options(fsOptions).mode("append").save(path)
        deltaLog.update()
        assert(deltaLog.snapshot.version == 1)

        // version 2.
        df3.write.format("delta").options(fsOptions).mode("append").save(path)
        deltaLog.update()
        assert(deltaLog.snapshot.version == 2)

        checkAnswer(
          spark.read.format("delta").options(fsOptions).load(path),
          df1.union(df2).union(df3))

        val deltaTable = io.delta.tables.DeltaTable.forPath(spark, path, fsOptions)
        deltaTable.restoreToVersion(1)

        checkAnswer(
          spark.read.format("delta").options(fsOptions).load(path),
          df1.union(df2)
        )

        // set the time to first file with a early time and verify the delta table can be restored
        // to it.
        val desiredTime = "1996-01-12"
        val format = new java.text.SimpleDateFormat("yyyy-MM-dd")
        val time = format.parse(desiredTime).getTime

        val logPath = new Path(dir.getCanonicalPath, "_delta_log")
        val file = new File(FileNames.unsafeDeltaFile(logPath, 0).toString)
        assert(file.setLastModified(time))

        val deltaTable2 = io.delta.tables.DeltaTable.forPath(spark, path, fsOptions)
        deltaTable2.restoreToTimestamp(desiredTime)

        checkAnswer(
          spark.read.format("delta").options(fsOptions).load(path),
          df1
        )
      }
    }
  }

  test("upgradeTableProtocol - with filesystem options.") {
    withTempDir { dir =>
      val path = fakeFileSystemPath(dir)
      val fsOptions = fakeFileSystemOptions

      // create a table with a default Protocol.
      val testSchema = spark.range(1).schema
      val log = DeltaLog.forTable(spark, new Path(path), fsOptions)
      log.createLogDirectoriesIfNotExists()
      log.store.write(
        FileNames.unsafeDeltaFile(log.logPath, 0),
        Iterator(Metadata(schemaString = testSchema.json).json, Protocol(0, 0).json),
        overwrite = false,
        log.newDeltaHadoopConf())
      log.update()

      // update the protocol.
      val table = DeltaTable.forPath(spark, path, fsOptions)
      table.upgradeTableProtocol(1, 2)

      val expectedProtocol = Protocol(1, 2)
      assert(log.snapshot.protocol === expectedProtocol)
    }
  }

  test("addFeatureSupport - with filesystem options.") {
    withTempDir { dir =>
      val path = fakeFileSystemPath(dir)
      val fsOptions = fakeFileSystemOptions

      // create a table with a default Protocol.
      val testSchema = spark.range(1).schema
      val log = DeltaLog.forTable(spark, new Path(path), fsOptions)
      log.createLogDirectoriesIfNotExists()
      log.store.write(
        FileNames.unsafeDeltaFile(log.logPath, 0),
        Iterator(Metadata(schemaString = testSchema.json).json, Protocol(1, 2).json),
        overwrite = false,
        log.newDeltaHadoopConf())
      log.update()

      // update the protocol to support a writer feature.
      val table = DeltaTable.forPath(spark, path, fsOptions)
      table.addFeatureSupport(TestWriterFeature.name)
      assert(log.update().protocol === Protocol(1, 7).withFeatures(Seq(
        AppendOnlyTableFeature,
        InvariantsTableFeature,
        TestWriterFeature)))
      table.addFeatureSupport(TestReaderWriterFeature.name)
      assert(
        log.update().protocol === Protocol(3, 7).withFeatures(Seq(
          AppendOnlyTableFeature,
          InvariantsTableFeature,
          TestWriterFeature,
          TestReaderWriterFeature)))

      // update the protocol again with invalid feature name.
      assert(intercept[DeltaTableFeatureException] {
        table.addFeatureSupport("__invalid_feature__")
      }.getErrorClass === "DELTA_UNSUPPORTED_FEATURES_IN_CONFIG")
    }
  }

  test("dropFeatureSupport - with filesystem options.") {
    withTempDir { dir =>
      val path = fakeFileSystemPath(dir)
      val fsOptions = fakeFileSystemOptions

      // create a table with a default Protocol.
      val testSchema = spark.range(1).schema
      val log = DeltaLog.forTable(spark, new Path(path), fsOptions)
      log.createLogDirectoriesIfNotExists()
      log.store.write(
        FileNames.unsafeDeltaFile(log.logPath, 0),
        Iterator(Metadata(schemaString = testSchema.json).json, Protocol(1, 2).json),
        overwrite = false,
        log.newDeltaHadoopConf())
      log.update()

      // update the protocol to support a writer feature.
      val table = DeltaTable.forPath(spark, path, fsOptions)
      table.addFeatureSupport(TestRemovableWriterFeature.name)
      assert(log.update().protocol === Protocol(1, 7).withFeatures(Seq(
        AppendOnlyTableFeature,
        InvariantsTableFeature,
        TestRemovableWriterFeature)))

      // Attempt truncating the history when dropping a feature that is not required.
      // This verifies the truncateHistory option was correctly passed.
      assert(intercept[DeltaTableFeatureException] {
        table.dropFeatureSupport("testRemovableWriter", truncateHistory = true)
      }.getErrorClass === "DELTA_FEATURE_DROP_HISTORY_TRUNCATION_NOT_ALLOWED")

      // Drop feature.
      table.dropFeatureSupport(TestRemovableWriterFeature.name)
      // After dropping the feature we should return back to the original protocol.
      assert(log.update().protocol === Protocol(1, 2))

      table.addFeatureSupport(TestRemovableReaderWriterFeature.name)
      assert(
        log.update().protocol === Protocol(3, 7).withFeatures(Seq(
          AppendOnlyTableFeature,
          InvariantsTableFeature,
          TestRemovableReaderWriterFeature)))

      // Drop feature.
      table.dropFeatureSupport(TestRemovableReaderWriterFeature.name)
      // After dropping the feature we should return back to the original protocol.
      assert(log.update().protocol === Protocol(1, 2))

      // Try to drop an unsupported feature.
      assert(intercept[DeltaTableFeatureException] {
        table.dropFeatureSupport("__invalid_feature__")
      }.getErrorClass === "DELTA_FEATURE_DROP_UNSUPPORTED_CLIENT_FEATURE")

      // Try to drop a feature that is not present in the protocol.
      assert(intercept[DeltaTableFeatureException] {
        table.dropFeatureSupport(TestRemovableReaderWriterFeature.name)
      }.getErrorClass === "DELTA_FEATURE_DROP_FEATURE_NOT_PRESENT")

      // Try to drop a non-removable feature.
      assert(intercept[DeltaTableFeatureException] {
        table.dropFeatureSupport(TestReaderWriterFeature.name)
      }.getErrorClass === "DELTA_FEATURE_DROP_NONREMOVABLE_FEATURE")
    }
  }

  test("details - with filesystem options.") {
    withTempDir{ dir =>
      val path = fakeFileSystemPath(dir)
      val fsOptions = fakeFileSystemOptions
      Seq(1, 2, 3).toDF().write.format("delta").options(fsOptions).save(path)

      val deltaTable = DeltaTable.forPath(spark, path, fsOptions)
      checkAnswer(
        deltaTable.detail().select("format"),
        Seq(Row("delta"))
      )
    }
  }
}
