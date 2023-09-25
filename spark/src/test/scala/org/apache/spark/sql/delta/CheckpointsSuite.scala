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
import org.apache.spark.sql.delta.actions._
import org.apache.spark.sql.delta.deletionvectors.DeletionVectorsSuite
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.storage.LocalLogStore
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.test.DeltaTestImplicits._
import org.apache.spark.sql.delta.util.FileNames
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FSDataOutputStream, Path, RawLocalFileSystem}
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.util.Progressable

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType

trait CheckpointsSuiteBase extends QueryTest with SharedSparkSession {
  def testDifferentV2Checkpoints(testName: String)(f: => Unit): Unit = {
    for (checkpointFormat <- Seq(V2Checkpoint.Format.JSON.name, V2Checkpoint.Format.PARQUET.name)) {
      test(s"$testName [v2CheckpointFormat: $checkpointFormat]") {
        withSQLConf(
          DeltaConfigs.CHECKPOINT_POLICY.defaultTablePropertyKey -> CheckpointPolicy.V2.name,
          DeltaSQLConf.CHECKPOINT_V2_TOP_LEVEL_FILE_FORMAT.key -> checkpointFormat
        ) {
          f
        }
      }
    }
  }

  /** Get V2 [[CheckpointProvider]] from the underlying deltalog snapshot */
  def getV2CheckpointProvider(
      deltaLog: DeltaLog,
      update: Boolean = true): V2CheckpointProvider = {
    val snapshot = if (update) deltaLog.update() else deltaLog.unsafeVolatileSnapshot
    snapshot.checkpointProvider match {
      case v2CheckpointProvider: V2CheckpointProvider =>
        v2CheckpointProvider
      case provider : LazyCompleteCheckpointProvider
          if provider.underlyingCheckpointProvider.isInstanceOf[V2CheckpointProvider] =>
        provider.underlyingCheckpointProvider.asInstanceOf[V2CheckpointProvider]
      case EmptyCheckpointProvider =>
        throw new IllegalStateException("underlying snapshot doesn't have a checkpoint")
      case other =>
        throw new IllegalStateException(s"The underlying checkpoint is not a v2 checkpoint. " +
          s"It is: ${other.getClass.getName}")
    }
  }
}

class CheckpointsSuite
  extends CheckpointsSuiteBase
  with DeltaCheckpointTestUtils
  with DeltaSQLCommandTest {

  protected override def sparkConf = {
    // Set the gs LogStore impl to `LocalLogStore` so that it will work with `FakeGCSFileSystem`.
    // The default one is `HDFSLogStore` which requires a `FileContext` but we don't have one.
    super.sparkConf.set("spark.delta.logStore.gs.impl", classOf[LocalLogStore].getName)
  }

  test("checkpoint metadata - checkpoint schema above the configured threshold are not" +
    " written to LAST_CHECKPOINT") {
    withTempDir { tempDir =>
      spark.range(10).write.format("delta").save(tempDir.getAbsolutePath)
      val deltaLog = DeltaLog.forTable(spark, tempDir.getAbsolutePath)
      deltaLog.checkpoint()
      val lastCheckpointOpt = deltaLog.readLastCheckpointFile()
      assert(lastCheckpointOpt.nonEmpty)
      assert(lastCheckpointOpt.get.checkpointSchema.nonEmpty)
      val expectedCheckpointSchema =
        Seq("txn", "add", "remove", "metaData", "protocol", "domainMetadata")
      assert(lastCheckpointOpt.get.checkpointSchema.get.fieldNames.toSeq ===
        expectedCheckpointSchema)

      spark.range(10).write.mode("append").format("delta").save(tempDir.getAbsolutePath)
      withSQLConf(DeltaSQLConf.CHECKPOINT_SCHEMA_WRITE_THRESHOLD_LENGTH.key-> "10") {
        deltaLog.checkpoint()
        val lastCheckpointOpt = deltaLog.readLastCheckpointFile()
        assert(lastCheckpointOpt.nonEmpty)
        assert(lastCheckpointOpt.get.checkpointSchema.isEmpty)
      }
    }
  }

  testDifferentV2Checkpoints("checkpoint metadata - checkpoint schema not persisted in" +
      " json v2 checkpoints but persisted in parquet v2 checkpoints") {
    withTempDir { tempDir =>
      spark.range(10).write.format("delta").save(tempDir.getAbsolutePath)
      val deltaLog = DeltaLog.forTable(spark, tempDir.getAbsolutePath)
      deltaLog.checkpoint()
      val lastCheckpointOpt = deltaLog.readLastCheckpointFile()
      assert(lastCheckpointOpt.nonEmpty)
      val expectedFormat =
        spark.conf.getOption(DeltaSQLConf.CHECKPOINT_V2_TOP_LEVEL_FILE_FORMAT.key)
      assert(lastCheckpointOpt.get.checkpointSchema.isEmpty ===
        (expectedFormat.contains(V2Checkpoint.Format.JSON.name)))
    }
  }

  testDifferentV2Checkpoints(s"V2 Checkpoint write test" +
      s" - metadata, protocol, sidecar, checkpoint metadata actions") {
    withTempDir { tempDir =>
      spark.range(10).write.format("delta").save(tempDir.getAbsolutePath)
      val deltaLog = DeltaLog.forTable(spark, tempDir.getAbsolutePath)
      deltaLog.checkpoint()
      val checkpointFiles = deltaLog.listFrom(0).filter(FileNames.isCheckpointFile).toList
      assert(checkpointFiles.length == 1)
      val checkpoint = checkpointFiles.head
      val fileNameParts = checkpoint.getPath.getName.split("\\.")
      // The file name should be <version>.checkpoint.<uniqueStr>.parquet.
      assert(fileNameParts.length == 4)
      fileNameParts match {
        case Array(version, checkpointLiteral, _, format) =>
          val expectedFormat =
            spark.conf.getOption(DeltaSQLConf.CHECKPOINT_V2_TOP_LEVEL_FILE_FORMAT.key).get
          assert(format == expectedFormat)
          assert(version.toLong == 0)
          assert(checkpointLiteral == "checkpoint")
      }

      def getCheckpointFileActions(checkpoint: FileStatus) : Seq[Action] = {
        if (checkpoint.getPath.toString.endsWith("json")) {
          deltaLog.store.read(checkpoint.getPath).map(Action.fromJson)
        } else {
          val fileIndex =
            DeltaLogFileIndex(DeltaLogFileIndex.CHECKPOINT_FILE_FORMAT_PARQUET, Seq(checkpoint)).get
          deltaLog.loadIndex(fileIndex, Action.logSchema)
            .as[SingleAction].collect().map(_.unwrap).toSeq
        }
      }
      val actions = getCheckpointFileActions(checkpoint)
      // V2 Checkpoints should contain exactly one action each of types
      // Metadata, CheckpointMetadata, and Protocol
      // In this particular case, we should only have one sidecar file
      val sidecarActions = actions.collect{ case s: SidecarFile => s}
      assert(sidecarActions.length == 1)
      val sidecarPath = sidecarActions.head.path
      assert(sidecarPath.endsWith("parquet"))

      val metadataActions = actions.collect { case m: Metadata => m }
      assert(metadataActions.length == 1)

      val checkpointMetadataActions = actions.collect { case cm: CheckpointMetadata => cm }
      assert(checkpointMetadataActions.length == 1)

      assert(
        DeltaConfigs.CHECKPOINT_POLICY.fromMetaData(metadataActions.head)
        .needsV2CheckpointSupport
      )

      val protocolActions = actions.collect { case p: Protocol => p }
      assert(protocolActions.length == 1)
      assert(CheckpointProvider.isV2CheckpointEnabled(protocolActions.head))
    }
  }

  test("SC-86940: isGCSPath") {
    val conf = new Configuration()
    assert(Checkpoints.isGCSPath(conf, new Path("gs://foo/bar")))
    // Scheme is case insensitive
    assert(Checkpoints.isGCSPath(conf, new Path("Gs://foo/bar")))
    assert(Checkpoints.isGCSPath(conf, new Path("GS://foo/bar")))
    assert(Checkpoints.isGCSPath(conf, new Path("gS://foo/bar")))
    assert(!Checkpoints.isGCSPath(conf, new Path("non-gs://foo/bar")))
    assert(!Checkpoints.isGCSPath(conf, new Path("/foo")))
    // Set the default file system and verify we can detect it
    conf.set("fs.defaultFS", "gs://foo/")
    conf.set("fs.gs.impl", classOf[FakeGCSFileSystem].getName)
    conf.set("fs.gs.impl.disable.cache", "true")
    assert(Checkpoints.isGCSPath(conf, new Path("/foo")))
  }

  test("SC-86940: writing a GCS checkpoint should happen in a new thread") {
    withTempDir { tempDir =>
      val path = tempDir.getCanonicalPath
      spark.range(1).write.format("delta").save(path)

      // Use `FakeGCSFileSystem` which will verify we write in a separate gcs thread.
      withSQLConf(
          "fs.gs.impl" -> classOf[FakeGCSFileSystem].getName,
          "fs.gs.impl.disable.cache" -> "true") {
        DeltaLog.clearCache()
        val gsPath = new Path(s"gs://${tempDir.getCanonicalPath}")
        val deltaLog = DeltaLog.forTable(spark, gsPath)
        deltaLog.checkpoint()
      }
    }
  }

  private def verifyCheckpoint(
      checkpoint: Option[LastCheckpointInfo],
      version: Int,
      parts: Option[Int]): Unit = {
    assert(checkpoint.isDefined)
    checkpoint.foreach { lastCheckpointInfo =>
      assert(lastCheckpointInfo.version == version)
      assert(lastCheckpointInfo.parts == parts)
    }
  }

  test("multipart checkpoints") {
     withTempDir { tempDir =>
      val path = tempDir.getCanonicalPath

      withSQLConf(
        DeltaSQLConf.DELTA_CHECKPOINT_PART_SIZE.key -> "10",
        DeltaConfigs.CHECKPOINT_INTERVAL.defaultTablePropertyKey -> "1") {
        // 1 file actions
        spark.range(1).repartition(1).write.format("delta").save(path)
        val deltaLog = DeltaLog.forTable(spark, path)

        // 2 file actions, 1 new file
        spark.range(1).repartition(1).write.format("delta").mode("append").save(path)

        verifyCheckpoint(deltaLog.readLastCheckpointFile(), 1, None)

        val checkpointPath =
          FileNames.checkpointFileSingular(deltaLog.logPath, deltaLog.snapshot.version).toUri
        assert(new File(checkpointPath).exists())

        // 11 total file actions, 9 new files
        spark.range(30).repartition(9).write.format("delta").mode("append").save(path)
        verifyCheckpoint(deltaLog.readLastCheckpointFile(), 2, Some(2))

        var checkpointPaths =
          FileNames.checkpointFileWithParts(deltaLog.logPath, deltaLog.snapshot.version, 2)
        checkpointPaths.foreach(p => assert(new File(p.toUri).exists()))

        // 20 total actions, 9 new files
        spark.range(100).repartition(9).write.format("delta").mode("append").save(path)
        verifyCheckpoint(deltaLog.readLastCheckpointFile(), 3, Some(2))

        assert(deltaLog.snapshot.version == 3)
        checkpointPaths =
          FileNames.checkpointFileWithParts(deltaLog.logPath, deltaLog.snapshot.version, 2)
        checkpointPaths.foreach(p => assert(new File(p.toUri).exists()))

        // 31 total actions, 11 new files
        spark.range(100).repartition(11).write.format("delta").mode("append").save(path)
        verifyCheckpoint(deltaLog.readLastCheckpointFile(), 4, Some(4))

        assert(deltaLog.snapshot.version == 4)
        checkpointPaths =
          FileNames.checkpointFileWithParts(deltaLog.logPath, deltaLog.snapshot.version, 4)
        checkpointPaths.foreach(p => assert(new File(p.toUri).exists()))
      }

      // Increase max actions
      withSQLConf(DeltaSQLConf.DELTA_CHECKPOINT_PART_SIZE.key -> "100") {
        val deltaLog = DeltaLog.forTable(spark, path)
        // 100 total actions, 69 new files
        spark.range(1000).repartition(69).write.format("delta").mode("append").save(path)
        verifyCheckpoint(deltaLog.readLastCheckpointFile(), 5, None)
        val checkpointPath =
          FileNames.checkpointFileSingular(deltaLog.logPath, deltaLog.snapshot.version).toUri
        assert(new File(checkpointPath).exists())

        // 101 total actions, 1 new file
        spark.range(1).repartition(1).write.format("delta").mode("append").save(path)
        verifyCheckpoint(deltaLog.readLastCheckpointFile(), 6, Some(2))
         var checkpointPaths =
          FileNames.checkpointFileWithParts(deltaLog.logPath, deltaLog.snapshot.version, 2)
        checkpointPaths.foreach(p => assert(new File(p.toUri).exists()))
      }
    }
  }

  testDifferentV2Checkpoints("multipart v2 checkpoint") {
    withTempDir { tempDir =>
      val path = tempDir.getCanonicalPath

      withSQLConf(
        DeltaSQLConf.DELTA_CHECKPOINT_PART_SIZE.key -> "10",
        DeltaConfigs.CHECKPOINT_POLICY.defaultTablePropertyKey -> CheckpointPolicy.V2.name,
        DeltaConfigs.CHECKPOINT_INTERVAL.defaultTablePropertyKey -> "1") {
        // 1 file actions
        spark.range(1).repartition(1).write.format("delta").save(path)
        val deltaLog = DeltaLog.forTable(spark, path)

        def getNumFilesInSidecarDirectory(): Int = {
          val fs = deltaLog.sidecarDirPath.getFileSystem(deltaLog.newDeltaHadoopConf())
          fs.listStatus(deltaLog.sidecarDirPath).size
        }

        // 2 file actions, 1 new file
        spark.range(1).repartition(1).write.format("delta").mode("append").save(path)
        assert(getV2CheckpointProvider(deltaLog).version == 1)
        assert(getV2CheckpointProvider(deltaLog).sidecarFileStatuses.size == 1)
        assert(getNumFilesInSidecarDirectory() == 1)

        // 11 total file actions, 9 new files
        spark.range(30).repartition(9).write.format("delta").mode("append").save(path)
        assert(getV2CheckpointProvider(deltaLog).version == 2)
        assert(getV2CheckpointProvider(deltaLog).sidecarFileStatuses.size == 2)
        assert(getNumFilesInSidecarDirectory() == 3)

        // 20 total actions, 9 new files
        spark.range(100).repartition(9).write.format("delta").mode("append").save(path)
        assert(getV2CheckpointProvider(deltaLog).version == 3)
        assert(getV2CheckpointProvider(deltaLog).sidecarFileStatuses.size == 2)
        assert(getNumFilesInSidecarDirectory() == 5)

        // 31 total actions, 11 new files
        spark.range(100).repartition(11).write.format("delta").mode("append").save(path)
        assert(getV2CheckpointProvider(deltaLog).version == 4)
        assert(getV2CheckpointProvider(deltaLog).sidecarFileStatuses.size == 4)
        assert(getNumFilesInSidecarDirectory() == 9)

        // Increase max actions
        withSQLConf(DeltaSQLConf.DELTA_CHECKPOINT_PART_SIZE.key -> "100") {
          // 100 total actions, 69 new files
          spark.range(1000).repartition(69).write.format("delta").mode("append").save(path)
          assert(getV2CheckpointProvider(deltaLog).version == 5)
          assert(getV2CheckpointProvider(deltaLog).sidecarFileStatuses.size == 1)
          assert(getNumFilesInSidecarDirectory() == 10)

          // 101 total actions, 1 new file
          spark.range(1).repartition(1).write.format("delta").mode("append").save(path)
          assert(getV2CheckpointProvider(deltaLog).version == 6)
          assert(getV2CheckpointProvider(deltaLog).sidecarFileStatuses.size == 2)
          assert(getNumFilesInSidecarDirectory() == 12)
        }
      }
    }
  }

  test("checkpoint does not contain CDC field") {
    withSQLConf(
        DeltaConfigs.CHANGE_DATA_FEED.defaultTablePropertyKey -> "true"
    ) {
      withTempDir { tempDir =>
        withTempView("src") {
          spark.range(10).write.format("delta").save(tempDir.getAbsolutePath)
          spark.range(5, 15).createOrReplaceTempView("src")
          sql(
            s"""
               |MERGE INTO delta.`$tempDir` t USING src s ON t.id = s.id
               |WHEN MATCHED THEN DELETE
               |WHEN NOT MATCHED THEN INSERT *
               |""".stripMargin)
          checkAnswer(
            spark.read.format("delta").load(tempDir.getAbsolutePath),
            Seq(0, 1, 2, 3, 4, 10, 11, 12, 13, 14).map { i => Row(i) })

          // CDC should exist in the log as seen through getChanges, but it shouldn't be in the
          // snapshots and the checkpoint file shouldn't have a CDC column.
          val deltaLog = DeltaLog.forTable(spark, tempDir.getAbsolutePath)
          assert(deltaLog.getChanges(1).next()._2.exists(_.isInstanceOf[AddCDCFile]))
          assert(deltaLog.snapshot.stateDS.collect().forall { sa => sa.cdc == null })
          deltaLog.checkpoint()
          val checkpointFile = FileNames.checkpointFileSingular(deltaLog.logPath, 1)
          val checkpointSchema = spark.read.format("parquet").load(checkpointFile.toString).schema
          val expectedCheckpointSchema =
            Seq(
              "txn",
              "add",
              "remove",
              "metaData",
              "protocol",
              "domainMetadata")
          assert(checkpointSchema.fieldNames.toSeq == expectedCheckpointSchema)
        }
      }
    }
  }

  testDifferentV2Checkpoints("v2 checkpoint contains only addfile and removefile and" +
      " remove file does not contain remove.tags and remove.numRecords") {
    withSQLConf(
      DeltaConfigs.CHANGE_DATA_FEED.defaultTablePropertyKey -> "true"
    ) {
      val expectedCheckpointSchema = Seq("add", "remove")
      val expectedRemoveFileSchema = Seq(
        "path",
        "deletionTimestamp",
        "dataChange",
        "extendedFileMetadata",
        "partitionValues",
        "size",
        "deletionVector",
        "baseRowId",
        "defaultRowCommitVersion")
      withTempDir { tempDir =>
        withTempView("src") {
          val tablePath = tempDir.getAbsolutePath
          // Append rows [0, 9] to table and merge tablePath.
          spark.range(end = 10).write.format("delta").mode("overwrite").save(tablePath)
          spark.range(5, 15).createOrReplaceTempView("src")
          sql(
            s"""
               |MERGE INTO delta.`$tempDir` t USING src s ON t.id = s.id
               |WHEN MATCHED THEN DELETE
               |WHEN NOT MATCHED THEN INSERT *
               |""".stripMargin)
          checkAnswer(
            spark.read.format("delta").load(tempDir.getAbsolutePath),
            Seq(0, 1, 2, 3, 4, 10, 11, 12, 13, 14).map { i => Row(i) })

          // CDC should exist in the log as seen through getChanges, but it shouldn't be in the
          // snapshots and the checkpoint file shouldn't have a CDC column.
          val deltaLog = DeltaLog.forTable(spark, tempDir.getAbsolutePath)
          assert(deltaLog.getChanges(1).next()._2.exists(_.isInstanceOf[AddCDCFile]))
          assert(deltaLog.snapshot.stateDS.collect().forall { sa => sa.cdc == null })
          deltaLog.checkpoint()
          var sidecarCheckpointFiles = getV2CheckpointProvider(deltaLog).sidecarFileStatuses
          assert(sidecarCheckpointFiles.size == 1)
          var sidecarFile = sidecarCheckpointFiles.head.getPath.toString
          var checkpointSchema = spark.read.format("parquet").load(sidecarFile).schema
          var removeSchemaName =
            checkpointSchema("remove").dataType.asInstanceOf[StructType].fieldNames
          assert(checkpointSchema.fieldNames.toSeq == expectedCheckpointSchema)
          assert(removeSchemaName.toSeq === expectedRemoveFileSchema)

          // Append rows [0, 9] to table and merge one more time.
          spark.range(end = 10).write.format("delta").mode("append").save(tablePath)
          sql(
            s"""
               |MERGE INTO delta.`$tempDir` t USING src s ON t.id = s.id
               |WHEN MATCHED THEN DELETE
               |WHEN NOT MATCHED THEN INSERT *
               |""".stripMargin)
          deltaLog.checkpoint()
          sidecarCheckpointFiles = getV2CheckpointProvider(deltaLog).sidecarFileStatuses
          sidecarFile = sidecarCheckpointFiles.head.getPath.toString
          checkpointSchema = spark.read.format(source = "parquet").load(sidecarFile).schema
          removeSchemaName = checkpointSchema("remove").dataType.asInstanceOf[StructType].fieldNames
          assert(removeSchemaName.toSeq === expectedRemoveFileSchema)
          checkAnswer(
            spark.sql(s"select * from delta.`$tablePath`"),
            Seq(0, 0, 1, 1, 2, 2, 3, 3, 4, 4).map { i => Row(i) })
        }
      }
    }
  }

  test("checkpoint does not contain remove.tags and remove.numRecords") {
    withTempDir { tempDir =>
      val expectedRemoveFileSchema = Seq(
        "path",
        "deletionTimestamp",
        "dataChange",
        "extendedFileMetadata",
        "partitionValues",
        "size",
        "deletionVector",
        "baseRowId",
        "defaultRowCommitVersion")

      val tablePath = tempDir.getAbsolutePath
      // Append rows [0, 9] to table and merge tablePath.
      spark.range(end = 10).write.format("delta").mode("overwrite").save(tablePath)
      spark.range(5, 15).createOrReplaceTempView("src")
      sql(
        s"""
           |MERGE INTO delta.`$tempDir` t USING src s ON t.id = s.id
           |WHEN MATCHED THEN DELETE
           |WHEN NOT MATCHED THEN INSERT *
           |""".stripMargin)
      val deltaLog = DeltaLog.forTable(spark, tablePath)
      deltaLog.checkpoint()
      var checkpointFile = FileNames.checkpointFileSingular(deltaLog.logPath, 1).toString
      var checkpointSchema = spark.read.format(source = "parquet").load(checkpointFile).schema
      var removeSchemaName = checkpointSchema("remove").dataType.asInstanceOf[StructType].fieldNames
      assert(removeSchemaName.toSeq === expectedRemoveFileSchema)
      checkAnswer(
        spark.sql(s"select * from delta.`$tablePath`"),
        Seq(0, 1, 2, 3, 4, 10, 11, 12, 13, 14).map { i => Row(i) })
      // Append rows [0, 9] to table and merge one more time.
      spark.range(end = 10).write.format("delta").mode("append").save(tablePath)
      sql(
        s"""
           |MERGE INTO delta.`$tempDir` t USING src s ON t.id = s.id
           |WHEN MATCHED THEN DELETE
           |WHEN NOT MATCHED THEN INSERT *
           |""".stripMargin)
      deltaLog.checkpoint()
      checkpointFile = FileNames.checkpointFileSingular(deltaLog.logPath, 1).toString
      checkpointSchema = spark.read.format(source = "parquet").load(checkpointFile).schema
      removeSchemaName = checkpointSchema("remove").dataType.asInstanceOf[StructType].fieldNames
      assert(removeSchemaName.toSeq === expectedRemoveFileSchema)
      checkAnswer(
        spark.sql(s"select * from delta.`$tablePath`"),
        Seq(0, 0, 1, 1, 2, 2, 3, 3, 4, 4).map { i => Row(i) })
    }
  }

  test("checkpoint with DVs") {
    for (v2Checkpoint <- Seq(true, false))
    withTempDir { tempDir =>
      val source = new File(DeletionVectorsSuite.table1Path) // this table has DVs in two versions
      val target = new File(tempDir, "insertTest")

      // Copy the source2 DV table to a temporary directory, so that we do updates to it
      FileUtils.copyDirectory(source, target)

      if (v2Checkpoint) {
        spark.sql(s"ALTER TABLE delta.`${target.getAbsolutePath}` SET TBLPROPERTIES " +
          s"('${DeltaConfigs.CHECKPOINT_POLICY.key}' = 'v2')")
      }

      sql(s"ALTER TABLE delta.`${target.getAbsolutePath}` " +
        s"SET TBLPROPERTIES (${DeltaConfigs.CHECKPOINT_INTERVAL.key} = 10)")
      def insertData(data: String): Unit = {
        spark.sql(s"INSERT INTO TABLE delta.`${target.getAbsolutePath}` $data")
      }
      val newData = Seq.range(3000, 3010)
      newData.foreach { i => insertData(s"VALUES($i)") }

      // Check the target file has checkpoint generated
      val deltaLog = DeltaLog.forTable(spark, target.getAbsolutePath)
      verifyCheckpoint(deltaLog.readLastCheckpointFile(), version = 10, parts = None)

      // Delete the commit files 0-9, so that we are forced to read the checkpoint file
      val logPath = new Path(new File(target, "_delta_log").getAbsolutePath)
      for (i <- 0 to 10) {
        val file = new File(FileNames.deltaFile(logPath, version = i).toString)
        file.delete()
      }

      // Make sure the contents are the same
      import testImplicits._
      checkAnswer(
        spark.sql(s"SELECT * FROM delta.`${target.getAbsolutePath}`"),
        (DeletionVectorsSuite.expectedTable1DataV4 ++ newData).toSeq.toDF())
    }
  }

  testDifferentCheckpoints("last checkpoint contains correct schema for v1/v2" +
      " Checkpoints") { (checkpointPolicy, v2CheckpointFormatOpt) =>
    withTempDir { tempDir =>
      spark.range(10).write.format("delta").save(tempDir.getAbsolutePath)
      val deltaLog = DeltaLog.forTable(spark, tempDir.getAbsolutePath)
      deltaLog.checkpoint()
      val lastCheckpointOpt = deltaLog.readLastCheckpointFile()
      assert(lastCheckpointOpt.nonEmpty)
      if (checkpointPolicy.needsV2CheckpointSupport) {
        if (v2CheckpointFormatOpt.contains(V2Checkpoint.Format.JSON)) {
          assert(lastCheckpointOpt.get.checkpointSchema.isEmpty)
        } else {
          assert(lastCheckpointOpt.get.checkpointSchema.nonEmpty)
          assert(lastCheckpointOpt.get.checkpointSchema.get.fieldNames.toSeq ===
            Seq("txn", "add", "remove", "metaData", "protocol",
              "domainMetadata", "checkpointMetadata", "sidecar"))
        }
      } else {
        assert(lastCheckpointOpt.get.checkpointSchema.nonEmpty)
        assert(lastCheckpointOpt.get.checkpointSchema.get.fieldNames.toSeq ===
          Seq("txn", "add", "remove", "metaData", "protocol", "domainMetadata"))
      }
    }
  }

  test("last checkpoint - v2 checkpoint fields threshold") {
    withTempDir { tempDir =>
      val tablePath = tempDir.getAbsolutePath
      spark.range(1).write.format("delta").save(tablePath)
      val deltaLog = DeltaLog.forTable(spark, tablePath)
      // Enable v2Checkpoint table feature.
      spark.sql(s"ALTER TABLE delta.`$tablePath` SET TBLPROPERTIES " +
        s"('${DeltaConfigs.CHECKPOINT_POLICY.key}' = 'v2')")

      def writeCheckpoint(
        adds: Int,
        nonFileActionThreshold: Int,
        sidecarActionThreshold: Int): LastCheckpointInfo = {
        withSQLConf(
          DeltaSQLConf.LAST_CHECKPOINT_NON_FILE_ACTIONS_THRESHOLD.key -> s"$nonFileActionThreshold",
          DeltaSQLConf.LAST_CHECKPOINT_SIDECARS_THRESHOLD.key -> s"$sidecarActionThreshold"
        ) {
          val addFiles = (1 to adds).map(_ =>
            AddFile(
              path = java.util.UUID.randomUUID.toString,
              partitionValues = Map(),
              size = 128L,
              modificationTime = 1L,
              dataChange = true
            ))
          deltaLog.startTransaction().commit(addFiles, DeltaOperations.ManualUpdate)
          deltaLog.checkpoint()
        }
        val lastCheckpointInfoOpt = deltaLog.readLastCheckpointFile()
        assert(lastCheckpointInfoOpt.nonEmpty)
        lastCheckpointInfoOpt.get
      }

      // Append 1 AddFile [AddFile-2]
      val lc1 = writeCheckpoint(adds = 1, nonFileActionThreshold = 10, sidecarActionThreshold = 10)
      assert(lc1.v2Checkpoint.nonEmpty)
      // 3 non file actions - protocol/metadata/checkpointMetadata, 1 sidecar
      assert(lc1.v2Checkpoint.get.nonFileActions.get.size === 3)
      assert(lc1.v2Checkpoint.get.sidecarFiles.get.size === 1)

      // Append 1 SetTxn, 8 more AddFiles [SetTxn-1, AddFile-10]
      deltaLog.startTransaction()
        .commit(Seq(SetTransaction("app-1", 2, None)), DeltaOperations.ManualUpdate)
      val lc2 = writeCheckpoint(adds = 8, nonFileActionThreshold = 4, sidecarActionThreshold = 10)
      assert(lc2.v2Checkpoint.nonEmpty)
      // 4 non file actions - protocol/metadata/checkpointMetadata/setTxn, 1 sidecar
      assert(lc2.v2Checkpoint.get.nonFileActions.get.size === 4)
      assert(lc2.v2Checkpoint.get.sidecarFiles.get.size === 1)

      // Append 10 more AddFiles [SetTxn-1, AddFile-20]
      val lc3 = writeCheckpoint(adds = 10, nonFileActionThreshold = 3, sidecarActionThreshold = 10)
      assert(lc3.v2Checkpoint.nonEmpty)
      // non-file actions exceeded threshold, 1 sidecar
      assert(lc3.v2Checkpoint.get.nonFileActions.isEmpty)
      assert(lc3.v2Checkpoint.get.sidecarFiles.get.size === 1)

      withSQLConf(DeltaSQLConf.DELTA_CHECKPOINT_PART_SIZE.key -> "5") {
        // Append 10 more AddFiles [SetTxn-1, AddFile-30]
        val lc4 =
          writeCheckpoint(adds = 10, nonFileActionThreshold = 3, sidecarActionThreshold = 10)
        assert(lc4.v2Checkpoint.nonEmpty)
        // non-file actions exceeded threshold
        // total 30 file actions, across 6 sidecar files (5 actions per file)
        assert(lc4.v2Checkpoint.get.nonFileActions.isEmpty)
        assert(lc4.v2Checkpoint.get.sidecarFiles.get.size === 6)
      }

      withSQLConf(DeltaSQLConf.DELTA_CHECKPOINT_PART_SIZE.key -> "2") {
        // Append 0 AddFiles [SetTxn-1, AddFile-30]
        val lc5 =
          writeCheckpoint(adds = 0, nonFileActionThreshold = 10, sidecarActionThreshold = 10)
        assert(lc5.v2Checkpoint.nonEmpty)
        // 4 non file actions - protocol/metadata/checkpointMetadata/setTxn
        // total 30 file actions, across 15 sidecar files (2 actions per file)
        assert(lc5.v2Checkpoint.get.nonFileActions.get.size === 4)
        assert(lc5.v2Checkpoint.get.sidecarFiles.isEmpty)
      }
    }
  }
}

/**
 * A fake GCS file system to verify delta commits are written in a separate gcs thread.
 */
class FakeGCSFileSystem extends RawLocalFileSystem {
  override def getScheme: String = "gs"
  override def getUri: URI = URI.create("gs:/")

  private def assertGCSThread(f: Path): Unit = {
    if (f.getName.contains(".json") || f.getName.contains(".checkpoint")) {
      assert(
        Thread.currentThread().getName.contains("delta-gcs-"),
        s"writing $f was happening in non gcs thread: ${Thread.currentThread()}")
    }
  }

  override def create(
      f: Path,
      permission: FsPermission,
      overwrite: Boolean,
      bufferSize: Int,
      replication: Short,
      blockSize: Long,
      progress: Progressable): FSDataOutputStream = {
    assertGCSThread(f)
    super.create(f, permission, overwrite, bufferSize, replication, blockSize, progress)
  }

  override def create(
      f: Path,
      overwrite: Boolean,
      bufferSize: Int,
      replication: Short,
      blockSize: Long,
      progress: Progressable): FSDataOutputStream = {
    assertGCSThread(f)
    super.create(f, overwrite, bufferSize, replication, blockSize, progress)
  }
}

