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
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files
import java.sql.Timestamp

import org.apache.spark.sql.delta.actions.Protocol
import org.apache.spark.sql.delta.sources.{DeltaLogStreamingBackend, DeltaSQLConf}
import org.apache.spark.sql.delta.sources.DeltaSourceMetadataEvolutionSupport.{collectMetadataActions, collectMetadataAtVersion, collectProtocolActions, collectProtocolAtVersion}
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.util.FileNames
import com.fasterxml.jackson.core.JsonParseException

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession

class DeltaLogStreamingBackendSuite
  extends QueryTest
    with SharedSparkSession
    with DeltaSQLCommandTest
    with DeltaTableProvider {

  private def defaultOptions: DeltaOptions =
    new DeltaOptions(Map.empty[String, String], spark.sessionState.conf)

  test("protocolRead rejects a protocol that this client cannot read") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      spark.range(1).write.format(writeFormat).save(path)
      val backend = new DeltaLogStreamingBackend(
        DeltaLog.forTable(spark, path), () => None, spark, defaultOptions)
      val snapshot = backend.update()

      backend.protocolRead(Protocol(1, 2), snapshot)
      val versionError = intercept[InvalidProtocolVersionException] {
        backend.protocolRead(Protocol(Integer.MAX_VALUE, Integer.MAX_VALUE), snapshot)
      }
      assert(versionError.tableNameOrPath === snapshot.dataPath.toString)
      val unknownReaderFeature = Protocol(3, 7).withReaderFeatures(Set("unknownFeature"))
        .withWriterFeatures(Set("unknownFeature"))
      val featureError = intercept[DeltaUnsupportedTableFeatureException] {
        backend.protocolRead(unknownReaderFeature, snapshot)
      }
      assert(featureError.getErrorClass === "DELTA_UNSUPPORTED_FEATURES_FOR_READ")
    }
  }

  test("each read gets the catalog table again") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      spark.range(1).write.format(writeFormat).save(path)
      spark.range(1).write.format(writeFormat).mode("append").save(path)
      var catalogTableReads = 0
      val backend = new DeltaLogStreamingBackend(
        DeltaLog.forTable(spark, path),
        () => {
          catalogTableReads += 1
          None
        },
        spark,
        options = defaultOptions)

      def assertCatalogTableReads(expected: Int)(read: => Any): Unit = {
        val before = catalogTableReads
        read
        assert(catalogTableReads - before == expected)
      }

      // A source can refresh its catalog table while it runs, so a read must not reuse the value
      // that an earlier read got.
      assertCatalogTableReads(1)(backend.update())
      assertCatalogTableReads(1)(backend.update())
      assertCatalogTableReads(1)(backend.getSnapshotAt(0))
      assertCatalogTableReads(1)(backend.getSnapshotToValidateProtocol(0))
      assertCatalogTableReads(1)(backend.getActiveCommitAtTime(
        new Timestamp(System.currentTimeMillis()),
        canReturnLastCommit = true,
        mustBeRecreatable = true,
        canReturnEarliestCommit = false))
      assertCatalogTableReads(1)(backend.getSingleCommitIterator(0, failOnDataLoss = true).toList)
      assertCatalogTableReads(1)(collectMetadataActions(backend, 0, Some(1)))
      assertCatalogTableReads(1)(collectProtocolActions(backend, 0, Some(1)))
      // These reads have never used the catalog table.
      assertCatalogTableReads(0)(backend.unsafeVolatileSnapshot)
      assertCatalogTableReads(0)(backend.unsafeVolatileTableId)
      assertCatalogTableReads(0)(backend.checkVersionExists(
        version = 1, mustBeRecreatable = false, allowOutOfRange = false))
    }
  }

  test("unsafeVolatileTableId returns the table id of the Delta log") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      spark.range(1).write.format(writeFormat).save(path)
      val deltaLog = DeltaLog.forTable(spark, path)
      val backend = new DeltaLogStreamingBackend(deltaLog, () => None, spark, defaultOptions)

      // The source writes this id into the metadata tracking log, so it must be the same string
      // that DeltaLog.unsafeVolatileTableId returns.
      assert(backend.unsafeVolatileTableId eq deltaLog.unsafeVolatileTableId)
      assert(backend.unsafeVolatileTableId == deltaLog.update().metadata.id)
    }
  }

  test("getSingleCommitIterator returns the commits from the start version") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      // Versions 0, 1 and 2.
      (0 until 3).foreach { _ =>
        spark.range(1).write.format(writeFormat).mode("append").save(path)
      }
      val backend = new DeltaLogStreamingBackend(
        DeltaLog.forTable(spark, path), () => None, spark, options = defaultOptions)

      def versions(startVersion: Long): Seq[Long] =
        backend.getSingleCommitIterator(startVersion, failOnDataLoss = true).map(_.version).toList

      assert(versions(0) === Seq(0, 1, 2))
      assert(versions(1) === Seq(1, 2))
      // A start after the latest version returns no commits.
      assert(versions(3).isEmpty)
    }
  }

  test("getSingleCommitIterator with a missing commit file") {
    withSQLConf(DeltaSQLConf.DELTA_GET_CHANGE_LOG_FILES_FAIL_ON_GAPS_IN_TESTS.key -> "false") {
      withTempDir { dir =>
        val path = dir.getCanonicalPath
        (0 until 5).foreach { _ =>
          spark.range(1).write.format(writeFormat).mode("append").save(path)
        }
        val deltaLog = DeltaLog.forTable(spark, path)
        deltaLog.checkpoint()
        assert(new File(FileNames.unsafeDeltaFile(deltaLog.logPath, 2).toUri).delete())
        val backend = new DeltaLogStreamingBackend(deltaLog, () => None, spark, defaultOptions)

        def versions(failOnDataLoss: Boolean): Seq[Long] =
          backend.getSingleCommitIterator(startVersion = 0, failOnDataLoss).map(_.version).toList

        val e = intercept[DeltaIllegalStateException] {
          versions(failOnDataLoss = true)
        }
        checkError(e, "DELTA_MISSING_FILES_UNEXPECTED_VERSION", "42K03",
          Map("startVersion" -> "2", "earliestVersion" -> "3", "option" -> "failOnDataLoss"))
        assert(versions(failOnDataLoss = false) === Seq(0, 1, 3, 4))
      }
    }
  }

  test("collectMetadataActions preserves version order and inclusive bounds") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      spark.range(1).write.format(writeFormat).save(path)
      spark.range(1).write.format(writeFormat).mode("append").save(path)
      sql(s"ALTER TABLE delta.`$path` ADD COLUMNS (value STRING)")
      val deltaLog = DeltaLog.forTable(spark, path)
      val backend = new DeltaLogStreamingBackend(deltaLog, () => None, spark, defaultOptions)

      val changes = collectMetadataActions(backend, 0, Some(2))
      assert(changes.map(_._1) === Seq(0, 2))
      changes.foreach { case (version, metadata) =>
        assert(metadata === deltaLog.getSnapshotAt(version).metadata)
      }
      assert(collectMetadataActions(backend, 1, Some(2)) === changes.tail)
      assert(collectMetadataActions(backend, 0, Some(1)) === changes.take(1))
      assert(collectMetadataActions(backend, 2, Some(2)) === changes.tail)
      assert(collectMetadataActions(backend, 0, None) === changes)
      assert(collectMetadataActions(backend, 1, Some(1)).isEmpty)
    }
  }

  test("collectProtocolActions omits commits without a protocol change") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      spark.range(1).write.format(writeFormat).save(path)
      spark.range(1).write.format(writeFormat).mode("append").save(path)
      val deltaLog = DeltaLog.forTable(spark, path)
      val backend = new DeltaLogStreamingBackend(deltaLog, () => None, spark, defaultOptions)

      val changes = collectProtocolActions(backend, 0, Some(1))
      assert(changes === Seq(0L -> deltaLog.getSnapshotAt(0).protocol))
      assert(collectProtocolActions(backend, 0, None) === changes)
      assert(collectProtocolActions(backend, 1, Some(1)).isEmpty)
    }
  }

  test("action collectors honor failOnDataLoss when a commit is missing") {
    withSQLConf(DeltaSQLConf.DELTA_GET_CHANGE_LOG_FILES_FAIL_ON_GAPS_IN_TESTS.key -> "false") {
      withTempDir { dir =>
        val path = dir.getCanonicalPath
        (0 until 3).foreach { _ =>
          spark.range(1).write.format(writeFormat).mode("append").save(path)
        }
        val deltaLog = DeltaLog.forTable(spark, path)
        deltaLog.checkpoint()
        assert(new File(FileNames.unsafeDeltaFile(deltaLog.logPath, 1).toUri).delete())
        val backend = new DeltaLogStreamingBackend(deltaLog, () => None, spark, defaultOptions)

        intercept[DeltaIllegalStateException] {
          collectMetadataActions(backend, 0, Some(2))
        }
        intercept[DeltaIllegalStateException] {
          collectProtocolActions(backend, 0, Some(2))
        }
        val tolerantBackend = new DeltaLogStreamingBackend(
          deltaLog, () => None, spark,
          new DeltaOptions(Map("failOnDataLoss" -> "false"), spark.sessionState.conf))
        assert(collectMetadataActions(tolerantBackend, 0, Some(2))
          .map(_._1) === Seq(0))
        assert(collectProtocolActions(tolerantBackend, 0, Some(2))
          .map(_._1) === Seq(0))
      }
    }
  }

  test("action collectors preserve V1 gap handling after the end version") {
    withSQLConf(DeltaSQLConf.DELTA_GET_CHANGE_LOG_FILES_FAIL_ON_GAPS_IN_TESTS.key -> "false") {
      withTempDir { dir =>
        val path = dir.getCanonicalPath
        (0 until 4).foreach { _ =>
          spark.range(1).write.format(writeFormat).mode("append").save(path)
        }
        val deltaLog = DeltaLog.forTable(spark, path)
        deltaLog.checkpoint()
        assert(new File(FileNames.unsafeDeltaFile(deltaLog.logPath, 2).toUri).delete())
        val backend = new DeltaLogStreamingBackend(deltaLog, () => None, spark, defaultOptions)

        // Preserve the existing lookahead, which can fail on a gap beyond the requested range.
        intercept[DeltaIllegalStateException] {
          collectMetadataActions(backend, 0, Some(1))
        }
        intercept[DeltaIllegalStateException] {
          collectProtocolActions(backend, 0, Some(1))
        }
        intercept[DeltaIllegalStateException] {
          collectMetadataAtVersion(backend, 1)
        }
        intercept[DeltaIllegalStateException] {
          collectProtocolAtVersion(backend, 1)
        }
      }
    }
  }

  test("action collectors preserve duplicates and return the first action at a version") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      spark.range(1).write.format(writeFormat).save(path)
      val deltaLog = DeltaLog.forTable(spark, path)
      val metadata = deltaLog.snapshot.metadata
      val secondMetadata = metadata.copy(description = "second metadata")
      val protocols = Seq(Protocol(1, 1), Protocol(1, 2))
      val actions = Seq(metadata, secondMetadata) ++ protocols
      Files.write(new File(FileNames.unsafeDeltaFile(deltaLog.logPath, 1).toUri).toPath,
        actions.map(_.json).mkString("\n").getBytes(UTF_8))
      val backend = new DeltaLogStreamingBackend(deltaLog, () => None, spark, defaultOptions)

      assert(collectMetadataActions(backend, 1, Some(1)).map(_._2) ===
        Seq(metadata, secondMetadata))
      assert(collectProtocolActions(backend, 1, Some(1)).map(_._2) === protocols)
      assert(collectMetadataAtVersion(backend, 1).contains(metadata))
      assert(collectProtocolAtVersion(backend, 1).contains(protocols.head))
    }
  }

  test("single-version collectors stop early while range collectors read all actions") {
    withSQLConf(DeltaSQLConf.LOG_SIZE_IN_MEMORY_THRESHOLD.key -> "0") {
      withTempDir { dir =>
        val path = dir.getCanonicalPath
        spark.range(1).write.format(writeFormat).save(path)
        val deltaLog = DeltaLog.forTable(spark, path)
        val snapshot = deltaLog.snapshot
        val actions = Seq(snapshot.metadata.json, snapshot.protocol.json, "invalid JSON")
        Files.write(new File(FileNames.unsafeDeltaFile(deltaLog.logPath, 1).toUri).toPath,
          actions.mkString("\n").getBytes(UTF_8))
        val backend = new DeltaLogStreamingBackend(deltaLog, () => None, spark, defaultOptions)

        assert(collectMetadataAtVersion(backend, 1).contains(snapshot.metadata))
        assert(collectProtocolAtVersion(backend, 1).contains(snapshot.protocol))
        intercept[JsonParseException] {
          collectMetadataActions(backend, 1, Some(1))
        }
        intercept[JsonParseException] {
          collectProtocolActions(backend, 1, Some(1))
        }
      }
    }
  }
}
