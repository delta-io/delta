/*
 * Copyright (2020-present) The Delta Lake Project Authors.
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

import io.delta.kernel.defaults.utils.{TestRow, TestUtils}
import io.delta.kernel.internal.{InternalScanFileUtils, SnapshotImpl}
import io.delta.tables.DeltaTable
import org.apache.hadoop.fs.Path
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.delta.{DeltaLog, Snapshot}
import org.apache.spark.sql.delta.actions.{AddFile, Metadata, Protocol}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaTestImplicits.OptimisticTxnTestHelper
import org.apache.spark.sql.delta.util.FileNames

class CheckpointV2ReadSuite extends AnyFunSuite with TestUtils {
  private final val supportedFileFormats = Seq("json", "parquet")

  def validateSnapshot(path: String, snapshotFromSpark: Snapshot): Unit = {
    // Create a snapshot from Spark connector and from kernel.
    val snapshot = latestSnapshot(path)
    val snapshotImpl = snapshot.asInstanceOf[SnapshotImpl]

    // Validate metadata/protocol loaded correctly from checkpoint manifest.
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
    assert(snapshot.getVersion(defaultTableClient) == snapshotFromSpark.version)

    // Validate AddFiles from sidecars found against Spark connector.
    val scan = snapshot.getScanBuilder(defaultTableClient).build()
    val foundFiles =
      collectScanFileRows(scan).map(InternalScanFileUtils.getAddFileStatus).map(
        _.getPath.split('/').last).toSet
    val expectedFiles = snapshotFromSpark.allFiles.collect().map(_.path).toSet
    assert(foundFiles == expectedFiles)
  }

  test("v2 checkpoint support") {
    supportedFileFormats.foreach { format =>
      withTempDir { path =>
        val tbl = "tbl"
        withTable(tbl) {
          // Create table.
          withSQLConf(DeltaSQLConf.CHECKPOINT_V2_TOP_LEVEL_FILE_FORMAT.key -> format,
            "spark.databricks.delta.clusteredTable.enableClusteringTablePreview" -> "true") {
            spark.sql(s"CREATE TABLE $tbl (a INT, b STRING) USING delta CLUSTER BY (a) " +
              s"LOCATION '$path' " +
              s"TBLPROPERTIES ('delta.checkpointInterval' = '2', 'delta.checkpointPolicy'='v2')")
            spark.sql(s"INSERT INTO $tbl VALUES (1, 'a'), (2, 'b')")
            spark.sql(s"INSERT INTO $tbl VALUES (3, 'c'), (4, 'd')")
            spark.sql(s"INSERT INTO $tbl VALUES (5, 'e'), (6, 'f')")
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
      // scneario.
      val snapshotFromSpark = DeltaLog.forTable(spark, path.toString).update()
      snapshotFromSpark.allFiles.collect()

      // Rename to UUID.
      val ckptPath = new Path(new File(log.logPath.toUri).listFiles().filter(f =>
        FileNames.isCheckpointFile(new Path(f.getPath))).head.toURI)
      new File(ckptPath.toUri).renameTo(new File(new Path(ckptPath.getParent, ckptPath.getName
        .replace("checkpoint.parquet", "checkpoint.abc-def.parquet")).toUri))

      // Validate snapshot.
      validateSnapshot(path.toString, snapshotFromSpark)
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
          spark.sql(s"CREATE TABLE $tbl (a INT, b STRING) USING delta LOCATION '$path'" +
            " TBLPROPERTIES ('delta.checkpointInterval' = '2', 'delta.checkpointPolicy'='v2')")
          spark.sql("INSERT INTO tbl VALUES (1, 'a'), (2, 'b')")
          spark.sql("INSERT INTO tbl VALUES (3, 'c'), (4, 'd')")
          spark.sql("INSERT INTO tbl VALUES (5, 'e'), (6, 'f')")
        }

        // Rename from UUID.
        val ckptPath = new Path(
          new File(DeltaLog.forTable(spark, path.toString).logPath.toUri).listFiles()
            .filter(f => FileNames.isCheckpointFile(new Path(f.getPath))).head.toURI)
        new File(ckptPath.toUri).renameTo(new File(
          FileNames.checkpointFileSingular(ckptPath.getParent, 2).toUri))

        // Validate snapshot and data.
        validateSnapshot(path.toString, DeltaLog.forTable(spark, path.toString).update())
        checkTable(
          path = path.toString,
          expectedAnswer = (1 to 6).map(i => TestRow(i, (i - 1 + 'a').toChar.toString))
        )
      }
    }
  }
}
