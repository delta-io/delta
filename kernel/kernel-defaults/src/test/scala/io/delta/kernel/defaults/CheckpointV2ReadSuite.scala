/*
 * Copyright (2023) The Delta Lake Project Authors.
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
package io.delta.kernel.defaults

import java.io.File

import scala.collection.JavaConverters._

import io.delta.kernel.defaults.engine.DefaultEngine
import io.delta.kernel.defaults.utils.{ExpressionTestUtils, TestRow, TestUtils}
import io.delta.kernel.expressions.Literal
import io.delta.kernel.internal.checkpoints.CheckpointInstance
import io.delta.kernel.internal.fs.{Path => DeltaPath}
import io.delta.kernel.internal.{InternalScanFileUtils, SnapshotImpl}
import io.delta.tables.DeltaTable
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.Row
import org.apache.spark.sql.delta.{DeltaLog, Snapshot}
import org.apache.spark.sql.delta.actions.{AddFile, Metadata, Protocol}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaTestImplicits.OptimisticTxnTestHelper
import org.apache.spark.sql.delta.util.FileNames
import org.apache.spark.sql.types.{BooleanType, IntegerType, LongType, MapType, StringType, StructType}

class CheckpointV2ReadSuite extends AnyFunSuite with TestUtils with ExpressionTestUtils {
  private final val supportedFileFormats = Seq("json", "parquet")

  override lazy val defaultEngine = DefaultEngine.create(new Configuration() {
    {
      // Set the batch sizes to small so that we get to test the multiple batch scenarios.
      set("delta.kernel.default.parquet.reader.batch-size", "2");
      set("delta.kernel.default.json.reader.batch-size", "2");
    }
  })

  def createSourceTable(
      tbl: String,
      path: String,
      partitionOrClusteringSpec: String): Unit = {
    spark.sql(s"CREATE TABLE $tbl (a INT, b STRING) USING delta " +
      s"$partitionOrClusteringSpec BY (a) LOCATION '$path' " +
      s"TBLPROPERTIES ('delta.checkpointInterval' = '2', 'delta.checkpointPolicy'='v2')")
    spark.sql(s"INSERT INTO $tbl VALUES (1, 'a'), (2, 'b')")
    spark.sql(s"INSERT INTO $tbl VALUES (3, 'c'), (4, 'd')")
    spark.sql(s"INSERT INTO $tbl VALUES (5, 'e'), (6, 'f')")
  }

  def validateSnapshot(
      path: String,
      snapshotFromSpark: Snapshot,
      strictFileValidation: Boolean = true,
      ckptVersionExpected: Option[Int] = None,
      expectV2CheckpointFormat: Boolean = true): Unit = {
    // Create a snapshot from Spark connector and from kernel.
    val snapshot = latestSnapshot(path)
    val snapshotImpl = snapshot.asInstanceOf[SnapshotImpl]

    // Validate metadata/protocol loaded correctly from top-level v2 checkpoint file.
    val expectedMetadataId =
      DeltaTable.forPath(path).detail().select("id").collect().head.getString(0)
    assert(snapshotImpl.getMetadata.getId == expectedMetadataId)
    assert(snapshotImpl.getProtocol.getMinReaderVersion ==
      snapshotFromSpark.protocol.minReaderVersion)
    assert(snapshotImpl.getProtocol.getMinWriterVersion ==
      snapshotFromSpark.protocol.minWriterVersion)
    assert(snapshotImpl.getProtocol.getReaderFeatures.asScala.toSet ==
      snapshotFromSpark.protocol.readerFeatureNames)
    assert(snapshotImpl.getProtocol.getWriterFeatures.asScala.toSet ==
      snapshotFromSpark.protocol.writerFeatureNames)
    assert(snapshot.getVersion(defaultEngine) == snapshotFromSpark.version)

    // Validate that snapshot read from most recent checkpoint. For most cases, given a checkpoint
    // interval of 2, this will be the most recent even version.
    val expectedV2CkptToRead =
      ckptVersionExpected.getOrElse(snapshotFromSpark.version - (snapshotFromSpark.version % 2))
    assert(snapshotImpl.getLogSegment.checkpoints.asScala.map(
      f => FileNames.checkpointVersion(new Path(f.getPath)))
      .contains(expectedV2CkptToRead))
    assert(snapshotImpl.getLogSegment.checkpoints.asScala.map(
      f => new CheckpointInstance(new DeltaPath(f.getPath)).format
        == CheckpointInstance.CheckpointFormat.V2)
      .contains(expectV2CheckpointFormat))


    // Validate AddFiles from sidecars found against Spark connector.
    val scan = snapshot.getScanBuilder(defaultEngine).build()
    val foundFiles =
      collectScanFileRows(scan).map(InternalScanFileUtils.getAddFileStatus).map(
        _.getPath.split('/').last).toSet
    val expectedFiles = snapshotFromSpark.allFiles.collect().map(_.toPath.toString).toSet
    if (strictFileValidation) {
      assert(foundFiles == expectedFiles)
    } else {
      assert(foundFiles.subsetOf(expectedFiles))
    }
  }

  test("v2 checkpoint support") {
    supportedFileFormats.foreach { format =>
      withTempDir { path =>
        val tbl = "tbl"
        withTable(tbl) {
          // Create table.
          withSQLConf(DeltaSQLConf.CHECKPOINT_V2_TOP_LEVEL_FILE_FORMAT.key -> format,
            "spark.databricks.delta.clusteredTable.enableClusteringTablePreview" -> "true") {
            createSourceTable(tbl, path.toString, "CLUSTER")

            // Insert more data to ensure multiple ColumnarBatches created.
            spark.createDataFrame(
              spark.sparkContext.parallelize(10 to 110).map(i => Row(i, i.toString)),
              new StructType().add("a", IntegerType).add("b", StringType))
              .repartition(10)
              .write.format("delta").mode("append").saveAsTable(tbl)
          }

          // Validate snapshot and data.
          validateSnapshot(path.toString, DeltaLog.forTable(spark, path.toString).update())
          checkTable(
            path = path.toString,
            expectedAnswer = spark.sql(s"SELECT * FROM $tbl").collect().map(TestRow(_)))

          // Remove some files from the table, then add a new one.
          spark.sql(s"DELETE FROM $tbl WHERE a=1 OR a=2")
          spark.sql(s"INSERT INTO $tbl VALUES (7, 'g'), (8, 'h')")

          // Validate snapshot and data.
          validateSnapshot(path.toString, DeltaLog.forTable(spark, path.toString).update())
          checkTable(
            path = path.toString,
            expectedAnswer = spark.sql(s"SELECT * FROM $tbl").collect().map(TestRow(_)))
        }
      }
    }
  }

  test("v2 checkpoint support with multiple sidecars") {
    supportedFileFormats.foreach { format =>
      withTempDir { path =>
        val tbl = "tbl"
        withTable(tbl) {
          // Create table.
          withSQLConf(DeltaSQLConf.CHECKPOINT_V2_TOP_LEVEL_FILE_FORMAT.key -> format,
            DeltaSQLConf.DELTA_CHECKPOINT_PART_SIZE.key -> "1", // Ensure 1 action per checkpoint.
            "spark.databricks.delta.clusteredTable.enableClusteringTablePreview" -> "true") {
            createSourceTable(tbl, path.toString, "CLUSTER")
          }

          // Validate snapshot and data.
          validateSnapshot(path.toString, DeltaLog.forTable(spark, path.toString).update())
          checkTable(
            path = path.toString,
            expectedAnswer = (1 to 6).map(i => TestRow(i, (i - 1 + 'a').toChar.toString))
          )

          // Remove some files from the table, then add a new one.
          spark.sql(s"DELETE FROM $tbl WHERE a=1 OR a=2")
          spark.sql(s"INSERT INTO $tbl VALUES (7, 'g'), (8, 'h')")

          // Validate snapshot and data.
          validateSnapshot(path.toString, DeltaLog.forTable(spark, path.toString).update())
          checkTable(
            path = path.toString,
            expectedAnswer = (3 to 8).map(i => TestRow(i, (i - 1 + 'a').toChar.toString))
          )
        }
      }
    }
  }

  test("v2 checkpoint support with an empty sidecar") {
    supportedFileFormats.foreach { format =>
      withTempDir { path =>
        val tbl = "tbl"
        withTable(tbl) {
          // Create table.
          withSQLConf(DeltaSQLConf.CHECKPOINT_V2_TOP_LEVEL_FILE_FORMAT.key -> format,
            DeltaSQLConf.DELTA_CHECKPOINT_PART_SIZE.key -> "1", // Ensure 1 action per checkpoint.
            "spark.databricks.delta.clusteredTable.enableClusteringTablePreview" -> "true") {
            createSourceTable(tbl, path.toString, "CLUSTER")
          }

          // Evalute Spark result before updating sidecar.
          val snapshotFromSpark = DeltaLog.forTable(spark, path.toString).update()
          snapshotFromSpark.allFiles.collect()

          // Remove all data from one of the sidecar files.
          val sidecarFolderPath =
            new Path(DeltaLog.forTable(spark, path.toString).logPath, "_sidecars")
          val tmpPath = new Path(new Path(path.toString), "_tmp")
          val sidecarCkptPath = new Path(new File(sidecarFolderPath.toUri).listFiles()
            .filter(f => !f.getName.endsWith(".crc")).head.toURI).toUri
          // Create new empty sidecar file, then move it to the sidecar filepath. Delete the sidecar
          // checksum file to prevent corruption check.
          spark.createDataFrame(spark.sparkContext.parallelize(Seq.empty[Row]),
              new StructType()
                .add("add", new StructType()
                  .add("path", StringType)
                  .add("partitionValues", MapType(StringType, StringType))
                  .add("size", LongType)
                  .add("modificationTime", LongType)
                  .add("dataChange", BooleanType))
                .add("remove", new StructType()
                  .add("path", StringType)))
            .coalesce(1)
            .write.mode("append").parquet(tmpPath.toString)
          val oldPath =
            new File(tmpPath.toString).listFiles().filter(_.getName.endsWith(".parquet")).head
          oldPath.renameTo(new File(sidecarCkptPath))
          val parent = new Path(sidecarCkptPath).getParent
          val name = "." + new Path(sidecarCkptPath).getName + ".crc"
          new File(new Path(parent, name).toUri).delete()

          // Validate snapshot and data.
          validateSnapshot(
            path.toString,
            DeltaLog.forTable(spark, path.toString).update(),
            strictFileValidation = false)
        }
      }
    }
  }

  test("UUID named checkpoint with actions") {
    withTempDir { path =>
      // Create Delta log and a checkpoint file with actions in it.
      val log = DeltaLog.forTable(spark, new Path(path.toString))
      new File(log.logPath.toUri).mkdirs()

      val metadata = Metadata("testId", schemaString = "{\"type\":\"struct\",\"fields\":[" +
        "{\"name\":\"a\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}")
      val supportedFeatures = Set("v2Checkpoint", "appendOnly", "invariants")
      val protocol = Protocol(3, 7, Some(Set("v2Checkpoint")), Some(supportedFeatures))
      val add = AddFile(new Path("addfile").toUri.toString, Map.empty, 100L,
        10L, dataChange = true)

      log.startTransaction().commitManually(Seq(metadata, add): _*)
      log.upgradeProtocol(None, log.update(), protocol)
      log.checkpoint(log.update())

      // Spark snapshot and files must be evaluated before renaming the checkpoint file.
      // This is because this checkpoint file (technically) becomes invalid, as there is no
      // CheckpointManifest action in it. However, because the Spark connector will place all
      // Add and Remove actions in the sidecar files, we must use this hack to test this
      // scenario.
      val snapshotFromSpark = DeltaLog.forTable(spark, path.toString).update()
      snapshotFromSpark.allFiles.collect()

      // Rename to UUID.
      val ckptPath = new Path(new File(log.logPath.toUri).listFiles().filter(f =>
        FileNames.isCheckpointFile(new Path(f.getPath))).head.toURI)
      new File(ckptPath.toUri).renameTo(new File(new Path(ckptPath.getParent, ckptPath.getName
        .replace("checkpoint.parquet", "checkpoint.abc-def.parquet")).toUri))

      // Validate snapshot.
      validateSnapshot(path.toString, snapshotFromSpark, ckptVersionExpected = Some(1))
    }
  }

  test("compatibility checkpoint with sidecar files") {
    withTempDir { path =>
      val tbl = "tbl"
      withTable(tbl) {
        // Create checkpoint with sidecars.
        withSQLConf(DeltaSQLConf.CHECKPOINT_V2_TOP_LEVEL_FILE_FORMAT.key -> "parquet",
          "spark.databricks.delta.clusteredTable.enableClusteringTablePreview" -> "true") {
          spark.conf.set(DeltaSQLConf.CHECKPOINT_V2_TOP_LEVEL_FILE_FORMAT.key, "parquet")
          createSourceTable(tbl, path.toString, "CLUSTER")
        }

        // Spark snapshot and files must be evaluated before renaming the checkpoint file.
        val snapshotFromSpark = DeltaLog.forTable(spark, path.toString).update()
        snapshotFromSpark.allFiles.collect()

        // Rename from UUID.
        val ckptPath = new Path(
          new File(DeltaLog.forTable(spark, path.toString).logPath.toUri).listFiles()
            .filter(f => FileNames.isCheckpointFile(new Path(f.getPath))).head.toURI)
        new File(ckptPath.toUri).renameTo(new File(
          FileNames.checkpointFileSingular(ckptPath.getParent, 2).toUri))

        // Validate snapshot and data.
        validateSnapshot(path.toString, snapshotFromSpark, expectV2CheckpointFormat = false)
        checkTable(
          path = path.toString,
          expectedAnswer = (1 to 6).map(i => TestRow(i, (i - 1 + 'a').toChar.toString))
        )
      }
    }
  }

  test("read from table with partition predicates") {
    withTempDir { path =>
      val tbl = "tbl"
      withTable(tbl) {
        // Create source table with schema (a INT, b STRING) partitioned by a.
        createSourceTable(tbl, path.toString, "PARTITIONED")

        // Read from the source table with a partition predicate and validate the results.
        val result = readSnapshot(
          latestSnapshot(path.toString),
          filter = greaterThan(col("a"), Literal.ofInt(3)))
        checkAnswer(result, Seq(TestRow(4, "d"), TestRow(5, "e"), TestRow(6, "f")))
      }
    }
  }
}
