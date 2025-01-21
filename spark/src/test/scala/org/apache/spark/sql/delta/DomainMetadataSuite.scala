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

import java.nio.charset.StandardCharsets.UTF_8
import java.util.concurrent.ExecutionException

import scala.util.{Failure, Success, Try}

import org.apache.spark.sql.delta.DeltaOperations.{ManualUpdate, Truncate}
import org.apache.spark.sql.delta.actions.{DomainMetadata, TableFeatureProtocolUtils}
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.test.DeltaTestImplicits._
import org.apache.spark.sql.delta.util.{FileNames, JsonUtils}
import org.junit.Assert._

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.streaming.CheckpointFileManager
import org.apache.spark.sql.test.SharedSparkSession

class DomainMetadataSuite
    extends QueryTest
    with SharedSparkSession
    with DeltaSQLCommandTest {
  import testImplicits._

  private def sortByDomain(domainMetadata: Seq[DomainMetadata]): Seq[DomainMetadata] =
    domainMetadata.sortBy(_.domain)

  /**
   * A helper to validate the [[DomainMetadata]] actions can be retained during the delta state
   * reconstruction.
   *
   * @param doCheckpoint: Explicitly create a delta log checkpoint if marked as true.
   * @param doChecksum: Disable writting checksum file if marked as false.
  */
  private def validateStateReconstructionHelper(
      doCheckpoint: Boolean,
      doChecksum: Boolean): Unit = {
    val table = "testTable"
    withTable(table) {
      withSQLConf(
        DeltaSQLConf.DELTA_WRITE_CHECKSUM_ENABLED.key -> doChecksum.toString) {
        sql(
          s"""
             | CREATE TABLE $table(id int) USING delta
             | tblproperties
             | ('${TableFeatureProtocolUtils.propertyKey(DomainMetadataTableFeature)}' = 'enabled')
             |""".stripMargin)
        (1 to 100).toDF("id").write.format("delta").mode("append").saveAsTable(table)

        var deltaTable = DeltaTableV2(spark, TableIdentifier(table))
        def deltaLog = deltaTable.deltaLog
        assert(deltaTable.snapshot.domainMetadata.isEmpty)

        val domainMetadata = DomainMetadata("testDomain1", "", false) ::
          DomainMetadata("testDomain2", "{\"key1\":\"value1\"", false) :: Nil
        deltaTable.startTransactionWithInitialSnapshot().commit(domainMetadata, Truncate())
        assertEquals(sortByDomain(domainMetadata), sortByDomain(deltaLog.update().domainMetadata))
        assert(deltaLog.update().logSegment.checkpointProvider.version === -1)

        if (doCheckpoint) {
          deltaLog.checkpoint(deltaLog.unsafeVolatileSnapshot)
          // Clear the DeltaLog cache to force creating a new DeltaLog instance which will build
          // the Snapshot from the checkpoint file.
          DeltaLog.clearCache()
          deltaTable = DeltaTableV2(spark, TableIdentifier(table))
          assert(!deltaTable.snapshot.logSegment.checkpointProvider.isEmpty)

          assertEquals(
            sortByDomain(domainMetadata),
            sortByDomain(deltaTable.snapshot.domainMetadata))
        }

        DeltaLog.clearCache()
        deltaTable = DeltaTableV2(spark, TableIdentifier(table))
        val checksumOpt = deltaTable.snapshot.checksumOpt
        if (doChecksum) {
          assertEquals(
            sortByDomain(checksumOpt.get.domainMetadata.get), sortByDomain(domainMetadata))
        } else {
          assert(checksumOpt.isEmpty)
        }
        assert(deltaLog.update().validateChecksum())
      }
    }
  }

  // A helper to validate [[DomainMetadata]] actions can be deleted.
  private def validateDeletionHelper(doCheckpoint: Boolean, doChecksum: Boolean): Unit = {
    val table = "testTable"
    withTable(table) {
      withSQLConf(
        DeltaSQLConf.DELTA_WRITE_CHECKSUM_ENABLED.key -> doChecksum.toString
      ) {
        sql(
          s"""
             | CREATE TABLE $table(id int) USING delta
             | tblproperties
             | ('${TableFeatureProtocolUtils.propertyKey(DomainMetadataTableFeature)}' = 'enabled')
             |""".stripMargin)
        (1 to 100).toDF("id").write.format("delta").mode("append").saveAsTable(table)

        DeltaLog.clearCache()
        val deltaTable = DeltaTableV2(spark, TableIdentifier(table))
        val deltaLog = deltaTable.deltaLog
        assert(deltaTable.snapshot.domainMetadata.isEmpty)

        val domainMetadata = DomainMetadata("testDomain1", "", false) ::
          DomainMetadata("testDomain2", "{\"key1\":\"value1\"}", false) :: Nil

        deltaTable.startTransactionWithInitialSnapshot().commit(domainMetadata, Truncate())
        assertEquals(sortByDomain(domainMetadata), sortByDomain(deltaLog.update().domainMetadata))
        assert(deltaLog.update().logSegment.checkpointProvider.version === -1)

        // Delete testDomain1.
        deltaTable.startTransaction().commit(
          DomainMetadata("testDomain1", "", true) :: Nil, Truncate())
        val domainMetadatasAfterDeletion = DomainMetadata(
          "testDomain2",
          "{\"key1\":\"value1\"}", false) :: Nil
        assertEquals(
          sortByDomain(domainMetadatasAfterDeletion),
          sortByDomain(deltaLog.update().domainMetadata))

        // Create a new commit and validate the incrementally built snapshot state respects the
        // DomainMetadata deletion.
        deltaTable.startTransaction().commit(Nil, ManualUpdate)
        var snapshot = deltaLog.update()
        assertEquals(sortByDomain(domainMetadatasAfterDeletion), snapshot.domainMetadata)
        if (doCheckpoint) {
          deltaLog.checkpoint(snapshot)
          assertEquals(
            sortByDomain(domainMetadatasAfterDeletion),
            deltaLog.update().domainMetadata)
        }

        // force state reconstruction and validate it respects the DomainMetadata retention.
        DeltaLog.clearCache()
        snapshot = DeltaLog.forTableWithSnapshot(spark, TableIdentifier(table))._2
        assertEquals(sortByDomain(domainMetadatasAfterDeletion), snapshot.domainMetadata)
      }
    }
  }

  test("DomainMetadata actions tracking in CRC should stop once threshold is crossed") {
    def assertDomainMetadatas(
        deltaLog: DeltaLog,
        expectedDomainMetadatas: Seq[DomainMetadata],
        expectedInCrc: Boolean): Unit = {
      val snapshot = deltaLog.update()
      assert(snapshot.validateChecksum())
      assertEquals(sortByDomain(expectedDomainMetadatas), sortByDomain(snapshot.domainMetadata))
      assert(snapshot.checksumOpt.nonEmpty)
      if (expectedInCrc) {
        assert(snapshot.checksumOpt.get.domainMetadata.nonEmpty)
        assertEquals(
          sortByDomain(expectedDomainMetadatas),
          sortByDomain(snapshot.checksumOpt.get.domainMetadata.get))
      } else {
        assert(snapshot.checksumOpt.get.domainMetadata.isEmpty)
      }
    }

    val table = "testTable"
    withSQLConf(
      DeltaSQLConf.DELTA_MAX_DOMAIN_METADATAS_IN_CRC.key -> "2") {
      withTable(table) {
        sql(
          s"""
             | CREATE TABLE $table(id int) USING delta
             | tblproperties
             | ('${TableFeatureProtocolUtils.propertyKey(DomainMetadataTableFeature)}' = 'enabled')
             |""".stripMargin)
        val deltaLog = DeltaLog.forTable(spark, TableIdentifier(table))
        assertDomainMetadatas(deltaLog, Seq.empty, true)

        deltaLog
          .startTransaction()
          .commit(DomainMetadata("testDomain1", "", false) :: Nil, Truncate())
        assertDomainMetadatas(
          deltaLog, DomainMetadata("testDomain1", "", false) :: Nil, true)

        deltaLog
          .startTransaction()
          .commit(DomainMetadata("testDomain2", "", false) :: Nil, Truncate())
        assertDomainMetadatas(
          deltaLog,
          DomainMetadata("testDomain1", "", false) ::
            DomainMetadata("testDomain2", "", false) :: Nil,
          true)

        deltaLog
          .startTransaction()
          .commit(DomainMetadata("testDomain3", "", false) :: Nil, Truncate())
        assertDomainMetadatas(
          deltaLog,
          DomainMetadata("testDomain1", "", false) ::
            DomainMetadata("testDomain2", "", false) ::
            DomainMetadata("testDomain3", "", false) :: Nil,
          false)
      }
    }
  }

  test("Validate crc can be read when domainMetadata is missing") {
    val table = "testTable"
    withTable(table) {
      sql(
        s"""
           | CREATE TABLE $table(id int) USING delta
           | tblproperties
           | ('${TableFeatureProtocolUtils.propertyKey(DomainMetadataTableFeature)}' = 'enabled')
           |""".stripMargin)
      val deltaTable = DeltaTableV2(spark, TableIdentifier(table))
      val deltaLog = deltaTable.deltaLog
      val version =
        deltaTable
          .startTransactionWithInitialSnapshot()
          .commit(DomainMetadata("testDomain1", "", false) :: Nil, Truncate())
      val snapshot = deltaLog.update()
      assert(snapshot.checksumOpt.nonEmpty)
      assert(snapshot.checksumOpt.get.domainMetadata.nonEmpty)
      val originalChecksum = snapshot.checksumOpt.get

      // Write out a checksum without domainMetadata.
      val checksumWithoutDomainMetadata = originalChecksum.copy(domainMetadata = None)
      val writer = CheckpointFileManager.create(deltaLog.logPath, deltaLog.newDeltaHadoopConf())
      val toWrite = JsonUtils.toJson(checksumWithoutDomainMetadata) + "\n"
      val stream = writer.createAtomic(
        FileNames.checksumFile(deltaLog.logPath, version + 1),
        overwriteIfPossible = false)
      stream.write(toWrite.getBytes(UTF_8))
      stream.close()

      // Make sure the read is not broken.
      val content =
        deltaLog
          .store
          .read(
            FileNames.checksumFile(deltaLog.logPath, version + 1),
            deltaLog.newDeltaHadoopConf())
      val checksumFromFile = JsonUtils.mapper.readValue[VersionChecksum](content.head)
      assert(checksumWithoutDomainMetadata == checksumFromFile)
    }
  }


  test("DomainMetadata action survives state reconstruction [w/o checkpoint, w/o checksum]") {
    validateStateReconstructionHelper(doCheckpoint = false, doChecksum = false)
  }

  test("DomainMetadata action survives state reconstruction [w/ checkpoint, w/ checksum]") {
    validateStateReconstructionHelper(doCheckpoint = true, doChecksum = true)
  }

  test("DomainMetadata action survives state reconstruction [w/ checkpoint, w/o checksum]") {
    validateStateReconstructionHelper(doCheckpoint = true, doChecksum = false)
  }

  test("DomainMetadata action survives state reconstruction [w/o checkpoint, w/ checksum]") {
    validateStateReconstructionHelper(doCheckpoint = false, doChecksum = true)
  }

  test("DomainMetadata deletion [w/o checkpoint, w/o checksum]") {
    validateDeletionHelper(doCheckpoint = false, doChecksum = false)
  }

  test("DomainMetadata deletion [w/ checkpoint, w/o checksum]") {
    validateDeletionHelper(doCheckpoint = true, doChecksum = false)
  }

  test("DomainMetadata deletion [w/o checkpoint, w/ checksum]") {
    validateDeletionHelper(doCheckpoint = false, doChecksum = true)
  }

  test("DomainMetadata deletion [w/ checkpoint, w/ checksum]") {
    validateDeletionHelper(doCheckpoint = true, doChecksum = true)
  }

  test("Multiple DomainMetadatas with the same domain should fail in single transaction") {
    val table = "testTable"
    withTable(table) {
      sql(
        s"""
           | CREATE TABLE $table(id int) USING delta
           | tblproperties
           | ('${TableFeatureProtocolUtils.propertyKey(DomainMetadataTableFeature)}' = 'enabled')
           |""".stripMargin)
      (1 to 100).toDF("id").write.format("delta").mode("append").saveAsTable(table)
      val deltaTable = DeltaTableV2(spark, TableIdentifier(table))
      val domainMetadata =
        DomainMetadata("testDomain1", "", false) ::
          DomainMetadata("testDomain1", "", false) :: Nil
      val e = intercept[DeltaIllegalArgumentException] {
        deltaTable.startTransactionWithInitialSnapshot().commit(domainMetadata, Truncate())
      }
      assertEquals(e.getMessage,
        "[DELTA_DUPLICATE_DOMAIN_METADATA_INTERNAL_ERROR] " +
          "Internal error: two DomainMetadata actions within the same transaction have " +
          "the same domain testDomain1")
    }
  }

  test("Validate the failure when table feature is not enabled") {
    withTempDir { dir =>
      (1 to 100).toDF().write.format("delta").save(dir.getAbsolutePath)
      val deltaLog = DeltaLog.forTable(spark, dir)
      val domainMetadata = DomainMetadata("testDomain1", "", false) :: Nil
      val e = intercept[DeltaIllegalArgumentException] {
        deltaLog.startTransaction().commit(domainMetadata, Truncate())
      }
      assertEquals(e.getMessage,
        "[DELTA_DOMAIN_METADATA_NOT_SUPPORTED] " +
          "Detected DomainMetadata action(s) for domains [testDomain1], " +
          "but DomainMetadataTableFeature is not enabled.")
    }
  }

  test("Validate the lifespan of metadata domains for the REPLACE TABLE operation") {
    val existingDomainMetadatas =
      DomainMetadata("testDomain1", "", false) ::
        DomainMetadata("testDomain2", "", false) ::
        Nil
    val newDomainMetadatas =
        DomainMetadata("testDomain2", "key=val", false) ::
        DomainMetadata("testDomain3", "", false) ::
        Nil

    val result = DomainMetadataUtils.handleDomainMetadataForReplaceTable(
      existingDomainMetadatas, newDomainMetadatas)

    // testDomain1: survives by default (not in the final list since it already
    //              exists in the snapshot).
    // testDomain2: overwritten by new domain metadata
    // testDomain3: added to the final list since it only appears in the new set.
    assert(result ===
        DomainMetadata("testDomain2", "key=val", false) :: // Overwritten
        DomainMetadata("testDomain3", "", false) :: // New metadata domain
        Nil)
  }
}
