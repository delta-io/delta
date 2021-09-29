/*
 * Copyright (2020) The Delta Lake Project Authors.
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

package io.delta.standalone.internal

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

import io.delta.standalone.{DeltaLog, Operation}
import io.delta.standalone.actions.{Action => ActionJ, AddFile => AddFileJ, CommitInfo => CommitInfoJ, Metadata => MetadataJ, Protocol => ProtocolJ, RemoveFile => RemoveFileJ}
import io.delta.standalone.internal.actions._
import io.delta.standalone.internal.exception.DeltaErrors
import io.delta.standalone.internal.util.ConversionUtils
import io.delta.standalone.types.{StringType, StructField, StructType}
import io.delta.standalone.internal.util.TestUtils._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

// scalastyle:off funsuite
import org.scalatest.FunSuite

class OptimisticTransactionSuite extends FunSuite {
  // scalastyle:on funsuite
  val writerId = "test-writer-id"
  val manualUpdate = new Operation(Operation.Name.MANUAL_UPDATE)

  val A_P1 = "part=1/a"
  val B_P1 = "part=1/b"
  val C_P1 = "part=1/c"
  val C_P2 = "part=2/c"
  val D_P2 = "part=2/d"
  val E_P3 = "part=3/e"
  val F_P3 = "part=3/f"
  val G_P4 = "part=4/g"

  private val addA_P1 = AddFile(A_P1, Map("part" -> "1"), 1, 1, dataChange = true)
  private val addB_P1 = AddFile(B_P1, Map("part" -> "1"), 1, 1, dataChange = true)
  private val addC_P1 = AddFile(C_P1, Map("part" -> "1"), 1, 1, dataChange = true)
  private val addC_P2 = AddFile(C_P2, Map("part" -> "2"), 1, 1, dataChange = true)
  private val addD_P2 = AddFile(D_P2, Map("part" -> "2"), 1, 1, dataChange = true)
  private val addE_P3 = AddFile(E_P3, Map("part" -> "3"), 1, 1, dataChange = true)
  private val addF_P3 = AddFile(F_P3, Map("part" -> "3"), 1, 1, dataChange = true)
  private val addG_P4 = AddFile(G_P4, Map("part" -> "4"), 1, 1, dataChange = true)

  def withLog(
      actions: Seq[Action],
      partitionCols: Seq[String] = "part" :: Nil)(
      test: DeltaLog => Unit): Unit = {
    val schemaFields = partitionCols.map { p => new StructField(p, new StringType()) }.toArray
    val schema = new StructType(schemaFields)
    //  TODO  val metadata = Metadata(partitionColumns = partitionCols, schemaString = schema.json)
    val metadata = Metadata(partitionColumns = partitionCols)
    withTempDir { dir =>
      val log = DeltaLog.forTable(new Configuration(), dir.getCanonicalPath)
      log.startTransaction().commit(metadata :: Nil, manualUpdate, writerId)
      log.startTransaction().commit(actions, manualUpdate, writerId)

      test(log)
    }
  }

  /**
   * @tparam T expected exception type
   */
  def testMetadata[T <: Throwable : ClassTag](
      metadata: Metadata,
      expectedExceptionMessageSubStr: String): Unit = {
    withTempDir { dir =>
      val log = DeltaLog.forTable(new Configuration(), dir.getCanonicalPath)
      val e1 = intercept[T] {
        log.startTransaction().commit(metadata :: Nil, manualUpdate, writerId)
      }
      assert(e1.getMessage.contains(expectedExceptionMessageSubStr))

      val e2 = intercept[T] {
        log.startTransaction().updateMetadata(ConversionUtils.convertMetadata(metadata))
      }
      assert(e2.getMessage.contains(expectedExceptionMessageSubStr))
    }
  }

  test("basic commit") {
    withLog(addA_P1 :: addB_P1 :: Nil) { log =>
      log.startTransaction().commit(addA_P1.remove :: Nil, manualUpdate, writerId)

      // [...] is what is automatically added by OptimisticTransaction
      // 0 -> metadata [CommitInfo, Protocol]
      // 1 -> addA_P1, addB_P1 [CommitInfo]
      // 2 -> removeA_P1 [CommitInfo]
      val versionLogs = log.getChanges(0, true).asScala.toList

      assert(versionLogs(0).getActions.asScala.count(_.isInstanceOf[MetadataJ]) == 1)
      assert(versionLogs(0).getActions.asScala.count(_.isInstanceOf[CommitInfoJ]) == 1)
      assert(versionLogs(0).getActions.asScala.count(_.isInstanceOf[ProtocolJ]) == 1)

      assert(versionLogs(1).getActions.asScala.count(_.isInstanceOf[AddFileJ]) == 2)
      assert(versionLogs(1).getActions.asScala.count(_.isInstanceOf[CommitInfoJ]) == 1)

      assert(versionLogs(2).getActions.asScala.count(_.isInstanceOf[RemoveFileJ]) == 1)
      assert(versionLogs(2).getActions.asScala.count(_.isInstanceOf[CommitInfoJ]) == 1)
    }
  }

  test("basic checkpoint") {
    withTempDir { dir =>
      val log = DeltaLog.forTable(new Configuration(), dir.getCanonicalPath)
      (1 to 15).foreach { i =>
        val meta = if (i == 1) Metadata() :: Nil else Nil
        val txn = log.startTransaction()
        val file = AddFile(i.toString, Map.empty, 1, 1, dataChange = true) :: Nil
        val delete: Seq[Action] = if (i > 1) {
          RemoveFile(i - 1 toString, Some(System.currentTimeMillis()), true) :: Nil
        } else {
          Nil
        }
        txn.commit(meta ++ delete ++ file, manualUpdate, writerId)
      }

      val log2 = DeltaLog.forTable(new Configuration(), dir.getCanonicalPath)
      assert(log2.snapshot.getVersion == 14)
      assert(log2.snapshot.getAllFiles.size == 1)
    }
  }

  ///////////////////////////////////////////////////////////////////////////
  // prepareCommit() tests
  ///////////////////////////////////////////////////////////////////////////

  test("committing twice in the same transaction should fail") {
    withTempDir { dir =>
      val log = DeltaLog.forTable(new Configuration(), dir.getCanonicalPath)
      val txn = log.startTransaction()
      txn.commit(Metadata() :: Nil, manualUpdate, writerId)
      val e = intercept[AssertionError] {
        txn.commit(Nil, manualUpdate, writerId)
      }
      assert(e.getMessage.contains("Transaction already committed."))
    }
  }

  test("user cannot commit their own CommitInfo") {
    withTempDir { dir =>
      val log = DeltaLog.forTable(new Configuration(), dir.getCanonicalPath)
      log.startTransaction().commit(Metadata() :: Nil, manualUpdate, writerId)
      val e = intercept[AssertionError] {
        log.startTransaction().commit(CommitInfo.empty() :: Nil, manualUpdate, writerId)
      }
      assert(e.getMessage.contains("Cannot commit a custom CommitInfo in a transaction."))
    }
  }

  test("commits shouldn't have more than one Metadata") {
    withTempDir { dir =>
      val log = DeltaLog.forTable(new Configuration(), dir.getCanonicalPath)
      val txn = log.startTransaction()
      val e = intercept[AssertionError] {
        txn.commit(Metadata() :: Metadata() :: Nil, manualUpdate, writerId)
      }
      assert(e.getMessage.contains("Cannot change the metadata more than once in a transaction."))
    }
  }

  // DeltaLog::ensureLogDirectoryExists
  test("transaction should throw if it cannot read log directory during first commit") {
    withTempDir { dir =>
      val log = DeltaLog.forTable(new Configuration(), dir.getCanonicalPath)
      dir.setReadOnly()

      val txn = log.startTransaction()
      val e = intercept[java.io.IOException] {
        txn.commit(Metadata() :: Nil, manualUpdate, writerId)
      }

      val logPath = new Path(log.getPath, "_delta_log")
      assert(e.getMessage == s"Cannot create ${logPath.toString}")
    }
  }

  test("initial commit without metadata should fail") {
    withTempDir { dir =>
      val log = DeltaLog.forTable(new Configuration(), dir.getCanonicalPath)
      val txn = log.startTransaction()
      val e = intercept[IllegalStateException] {
        txn.commit(Nil, manualUpdate, writerId)
      }
      assert(e.getMessage == DeltaErrors.metadataAbsentException().getMessage)
    }
  }

  test("AddFile with different partition schema compared to metadata should fail") {
    withTempDir { dir =>
      val log = DeltaLog.forTable(new Configuration(), dir.getCanonicalPath)

      // Note that Metadata() has no partition schema specified and addA_P1 does
      log.startTransaction().commit(Metadata() :: Nil, manualUpdate, writerId)
      val e = intercept[IllegalStateException] {
        log.startTransaction().commit(addA_P1 :: Nil, manualUpdate, writerId)
      }
      assert(e.getMessage.contains("The AddFile contains partitioning schema different from the " +
        "table's partitioning schema"))
    }
  }

  test("Can't create table with invalid protocol version") {
    withTempDir { dir =>
      val log = DeltaLog.forTable(new Configuration(), dir.getCanonicalPath)

      Seq(Protocol(1, 3), Protocol(1, 1), Protocol(2, 2)).foreach { protocol =>
        val e = intercept[AssertionError] {
          log.startTransaction().commit(Metadata() :: protocol :: Nil, manualUpdate, writerId)
        }
        assert(e.getMessage.contains("Invalid Protocol"))
      }
    }
  }

  test("can't change protocol to invalid version") {
    withTempDir { dir =>
      val log = DeltaLog.forTable(new Configuration(), dir.getCanonicalPath)
      log.startTransaction().commit(Metadata() :: Protocol() :: Nil, manualUpdate, writerId)

      Seq(Protocol(1, 3), Protocol(1, 1), Protocol(2, 2)).foreach { protocol =>
        val e = intercept[AssertionError] {
          log.startTransaction().commit(protocol :: Nil, manualUpdate, writerId)
        }
        assert(e.getMessage.contains("Invalid Protocol"))
      }
    }
  }

  test("can't remove from an append-only table") {
    withTempDir { dir =>
      val log = DeltaLog.forTable(new Configuration(), dir.getCanonicalPath)
      val metadata = Metadata(configuration = Map("appendOnly" -> "true"))
      log.startTransaction().commit(metadata :: Nil, manualUpdate, writerId)

      val e = intercept[UnsupportedOperationException] {
        log.startTransaction().commit(addA_P1.remove :: Nil, manualUpdate, writerId)
      }
      assert(e.getMessage.contains("This table is configured to only allow appends"))
    }
  }

  ///////////////////////////////////////////////////////////////////////////
  // updateMetadata() tests
  ///////////////////////////////////////////////////////////////////////////

  test("can't update metadata more than once in a transaction") {
    withTempDir { dir =>
      val log = DeltaLog.forTable(new Configuration(), dir.getCanonicalPath)
      val txn = log.startTransaction()
      txn.updateMetadata(ConversionUtils.convertMetadata(Metadata()))
      val e = intercept[AssertionError] {
        txn.updateMetadata(ConversionUtils.convertMetadata(Metadata()))
      }

      assert(e.getMessage.contains("Cannot change the metadata more than once in a transaction."))
    }
  }

  test("Protocol Action should be automatically added to transaction for new table") {
    withTempDir { dir =>
      val log = DeltaLog.forTable(new Configuration(), dir.getCanonicalPath)
      log.startTransaction().commit(Metadata() :: Nil, manualUpdate, writerId)
      assert(log.getChanges(0, true).asScala.next().getActions.contains(new ProtocolJ(1, 2)))
    }
  }

  test("updateMetadata removes Protocol properties from metadata config") {
    // Note: These Protocol properties are not currently exposed to the user. However, they
    //       might be in the future, and nothing is stopping the user now from seeing these
    //       properties in Delta OSS and adding them to the config map here.
    withTempDir { dir =>
      val log = DeltaLog.forTable(new Configuration(), dir.getCanonicalPath)
      val txn = log.startTransaction()
      val metadata = Metadata(configuration = Map(
        Protocol.MIN_READER_VERSION_PROP -> "1",
        Protocol.MIN_WRITER_VERSION_PROP -> "2"
      ))
      txn.updateMetadata(ConversionUtils.convertMetadata(metadata))
      txn.commit(Nil, manualUpdate, writerId)

      val writtenConfig = log.update().getMetadata.getConfiguration
      assert(!writtenConfig.containsKey(Protocol.MIN_READER_VERSION_PROP))
      assert(!writtenConfig.containsKey(Protocol.MIN_WRITER_VERSION_PROP))
    }
  }

  test("commit new metadata with Protocol properties should fail") {
    withTempDir { dir =>
      val log = DeltaLog.forTable(new Configuration(), dir.getCanonicalPath)
      log.startTransaction().commit(Metadata() :: Nil, manualUpdate, writerId)
      val newMetadata = Metadata(configuration = Map(
        Protocol.MIN_READER_VERSION_PROP -> "1",
        Protocol.MIN_WRITER_VERSION_PROP -> "2"
      ))

      val e = intercept[AssertionError] {
        log.startTransaction().commit(newMetadata:: Nil, manualUpdate, writerId)
      }
      assert(e.getMessage.contains(s"Should not have the protocol version " +
        s"(${Protocol.MIN_READER_VERSION_PROP}) as part of table properties"))
    }
  }

  ///////////////////////////////////////////////////////////////////////////
  // verifyNewMetadata() tests
  ///////////////////////////////////////////////////////////////////////////

  test("can't have duplicate column names") {
    // TODO: just call myStruct.getJson()
    // scalastyle:off
    val schemaStr = """{"type":"struct","fields":[{"name":"col1","type":"integer","nullable":true,"metadata":{}},{"name":"col1","type":"integer","nullable":true,"metadata":{}}]}"""
    // scalastyle:on
    testMetadata[RuntimeException](Metadata(schemaString = schemaStr), "Found duplicate column(s)")
  }

  test("column names (both data and partition) must be acceptable by parquet") {
    // TODO: just call myStruct.getJson()
    // test DATA columns
    // scalastyle:off
    val schemaStr1 = """{"type":"struct","fields":[{"name":"bad;column,name","type":"integer","nullable":true,"metadata":{}}]}"""
    // scalastyle:on
    testMetadata[RuntimeException](Metadata(schemaString = schemaStr1),
      """Attribute name "bad;column,name" contains invalid character(s)""")

    // test PARTITION columns
    testMetadata[RuntimeException](Metadata(partitionColumns = "bad;column,name" :: Nil),
      "Found partition columns having invalid character(s)")
  }

  // TODO: test updateMetadata > unenforceable not null constraints removed from metadata schemaStr

  // TODO: test updateMetadata > withGlobalConfigDefaults

  ///////////////////////////////////////////////////////////////////////////
  // commit() tests
  ///////////////////////////////////////////////////////////////////////////

  // - TODO commitInfo is actually added to final actions (with correct engineInfo)
  // - TODO isBlindAppend == true cases
  // - TODO isBlindAppend == false case
  // - TODO different operation names

  ///////////////////////////////////////////////////////////////////////////
  // checkForConflicts() tests
  ///////////////////////////////////////////////////////////////////////////

  // TODO multiple concurrent commits, not just one (i.e. 1st doesn't conflict, 2nd does)

  // TODO: test more ConcurrentAppendException

  // TODO: test more ConcurrentDeleteReadException (including readWholeTable)

  // TODO: test checkForAddedFilesThatShouldHaveBeenReadByCurrentTxn with SnapshotIsolation
  // i.e. datachange = false

  // TODO: test Checkpoint > partialWriteVisible (==> useRename)

  // TODO: test Checkpoint > !partialWriteVisible (==> !useRename)

  // TODO: test Checkpoint > correctly checkpoints all action types
}
