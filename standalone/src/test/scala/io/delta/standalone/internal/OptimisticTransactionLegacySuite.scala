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
import io.delta.standalone.actions.{AddFile => AddFileJ, CommitInfo => CommitInfoJ, Metadata => MetadataJ, Protocol => ProtocolJ, RemoveFile => RemoveFileJ}
import io.delta.standalone.exceptions.{ConcurrentAppendException, ConcurrentDeleteDeleteException, ConcurrentDeleteReadException, ConcurrentTransactionException, MetadataChangedException, ProtocolChangedException}
import io.delta.standalone.expressions.{EqualTo, Expression, Literal}
import io.delta.standalone.internal.actions._
import io.delta.standalone.internal.exception.DeltaErrors
import io.delta.standalone.internal.util.{ConversionUtils, SchemaUtils}
import io.delta.standalone.types._
import io.delta.standalone.internal.util.TestUtils._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

// scalastyle:off funsuite
import org.scalatest.FunSuite

class OptimisticTransactionLegacySuite extends FunSuite {
  // scalastyle:on funsuite

  val engineInfo = "test-engine-info"
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
    val metadata = Metadata(partitionColumns = partitionCols, schemaString = schema.toJson)
    withTempDir { dir =>
      val log = DeltaLog.forTable(new Configuration(), dir.getCanonicalPath)
      log.startTransaction().commit(metadata :: Nil, manualUpdate, engineInfo)
      log.startTransaction().commit(actions, manualUpdate, engineInfo)

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
        log.startTransaction().commit(metadata :: Nil, manualUpdate, engineInfo)
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
      log.startTransaction().commit(addA_P1.remove :: Nil, manualUpdate, engineInfo)

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
        txn.commit(meta ++ delete ++ file, manualUpdate, engineInfo)
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
      txn.commit(Metadata() :: Nil, manualUpdate, engineInfo)
      val e = intercept[AssertionError] {
        txn.commit(Iterable().asJava, manualUpdate, engineInfo)
      }
      assert(e.getMessage.contains("Transaction already committed."))
    }
  }

  test("user cannot commit their own CommitInfo") {
    withTempDir { dir =>
      val log = DeltaLog.forTable(new Configuration(), dir.getCanonicalPath)
      log.startTransaction().commit(Metadata() :: Nil, manualUpdate, engineInfo)
      val e = intercept[AssertionError] {
        log.startTransaction().commit(CommitInfo.empty() :: Nil, manualUpdate, engineInfo)
      }
      assert(e.getMessage.contains("Cannot commit a custom CommitInfo in a transaction."))
    }
  }

  test("commits shouldn't have more than one Metadata") {
    withTempDir { dir =>
      val log = DeltaLog.forTable(new Configuration(), dir.getCanonicalPath)
      val txn = log.startTransaction()
      val e = intercept[AssertionError] {
        txn.commit(Metadata() :: Metadata() :: Nil, manualUpdate, engineInfo)
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
        txn.commit(Metadata() :: Nil, manualUpdate, engineInfo)
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
        txn.commit(Iterable().asJava, manualUpdate, engineInfo)
      }
      assert(e.getMessage == DeltaErrors.metadataAbsentException().getMessage)
    }
  }

  test("AddFile with different partition schema compared to metadata should fail") {
    withTempDir { dir =>
      val log = DeltaLog.forTable(new Configuration(), dir.getCanonicalPath)

      // Note that Metadata() has no partition schema specified and addA_P1 does
      log.startTransaction().commit(Metadata() :: Nil, manualUpdate, engineInfo)
      val e = intercept[IllegalStateException] {
        log.startTransaction().commit(addA_P1 :: Nil, manualUpdate, engineInfo)
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
          log.startTransaction().commit(Metadata() :: protocol :: Nil, manualUpdate, engineInfo)
        }
        assert(e.getMessage.contains("Invalid Protocol"))
      }
    }
  }

  test("can't change protocol to invalid version") {
    withTempDir { dir =>
      val log = DeltaLog.forTable(new Configuration(), dir.getCanonicalPath)
      log.startTransaction().commit(Metadata() :: Protocol() :: Nil, manualUpdate, engineInfo)

      Seq(Protocol(1, 3), Protocol(1, 1), Protocol(2, 2)).foreach { protocol =>
        val e = intercept[AssertionError] {
          log.startTransaction().commit(protocol :: Nil, manualUpdate, engineInfo)
        }
        assert(e.getMessage.contains("Invalid Protocol"))
      }
    }
  }

  test("Removing from an append-only table") {
    withTempDir { dir =>
      val log = DeltaLog.forTable(new Configuration(), dir.getCanonicalPath)
      val metadata = Metadata(configuration = Map("appendOnly" -> "true"))
      log.startTransaction().commit(metadata :: Nil, manualUpdate, engineInfo)

      val removeWithDataChange = addA_P1.remove.copy(dataChange = true)
      val e = intercept[UnsupportedOperationException] {
        log.startTransaction().commit(removeWithDataChange :: Nil, manualUpdate, engineInfo)
      }
      assert(e.getMessage.contains("This table is configured to only allow appends"))

      val removeWithoutDataChange = addA_P1.remove.copy(dataChange = false)
      log.startTransaction().commit(removeWithoutDataChange :: Nil, manualUpdate, engineInfo)
    }
  }

  ///////////////////////////////////////////////////////////////////////////
  // updateMetadata() tests
  ///////////////////////////////////////////////////////////////////////////

  test("initial commit with multiple metadata actions should fail") {
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
      log.startTransaction().commit(Metadata() :: Nil, manualUpdate, engineInfo)
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
      txn.commit(Iterable().asJava, manualUpdate, engineInfo)

      val writtenConfig = log.update().getMetadata.getConfiguration
      assert(!writtenConfig.containsKey(Protocol.MIN_READER_VERSION_PROP))
      assert(!writtenConfig.containsKey(Protocol.MIN_WRITER_VERSION_PROP))
    }
  }

  test("commit new metadata with Protocol properties should fail") {
    withTempDir { dir =>
      val log = DeltaLog.forTable(new Configuration(), dir.getCanonicalPath)
      log.startTransaction().commit(Metadata() :: Nil, manualUpdate, engineInfo)
      val newMetadata = Metadata(configuration = Map(
        Protocol.MIN_READER_VERSION_PROP -> "1",
        Protocol.MIN_WRITER_VERSION_PROP -> "2"
      ))

      val e = intercept[AssertionError] {
        log.startTransaction().commit(newMetadata:: Nil, manualUpdate, engineInfo)
      }
      assert(e.getMessage.contains(s"Should not have the protocol version " +
        s"(${Protocol.MIN_READER_VERSION_PROP}) as part of table properties"))
    }
  }

  test("unenforceable not null constraints") {
    val validSchema = new StructType(Array(
      new StructField(
        "col1",
        new MapType(new ArrayType(new StringType(), true), new IntegerType(), true),
        true
      ),
      new StructField(
        "col2",
        new MapType(new IntegerType(), new ArrayType(new StringType(), true), true),
        true
      ),
      new StructField(
        "col3",
        new ArrayType(new MapType(new StringType(), new IntegerType(), true),
          true)
      )
    ))

    SchemaUtils.checkUnenforceableNotNullConstraints(validSchema) // should not throw

    // case 1: not null within array
    val inValidSchema1 = new StructType(
      Array(
        new StructField(
          "arr",
          new ArrayType(
            new StructType(
              Array(
                new StructField("name", new StringType(), true),
                new StructField("mailbox", new StringType(), false)
              )
            ),
            false // arr (ArrayType) containsNull
          )
        )
      )
    )

    val e1 = intercept[RuntimeException] {
      SchemaUtils.checkUnenforceableNotNullConstraints(inValidSchema1)
    }.getMessage

    assert(e1.contains("The element type of the field arr contains a NOT NULL constraint."))

    // case 2: null within map key
    val inValidSchema2 = new StructType(
      Array(
        new StructField(
          "m",
          new MapType(
            new StructType( // m.key
              Array(
                new StructField("name", new StringType(), true),
                new StructField("mailbox", new StringType(), false)
              )
            ),
            new IntegerType(), // m.value
            false // m (MapType) valueContainsNull
          )
        )
      )
    )

    val e2 = intercept[RuntimeException] {
      SchemaUtils.checkUnenforceableNotNullConstraints(inValidSchema2)
    }.getMessage

    assert(e2.contains("The key type of the field m contains a NOT NULL constraint."))

    // case 3: null within map key
    val inValidSchema3 = new StructType(
      Array(
        new StructField(
          "m",
          new MapType(
            new IntegerType(), // m.key
            new StructType( // m.value
              Array(
                new StructField("name", new StringType(), true),
                new StructField("mailbox", new StringType(), false)
              )
            ),
            false // m (MapType) valueContainsNull
          )
        )
      )
    )

    val e3 = intercept[RuntimeException] {
      SchemaUtils.checkUnenforceableNotNullConstraints(inValidSchema3)
    }.getMessage

    assert(e3.contains("The value type of the field m contains a NOT NULL constraint."))

    // case 4: not null within nested array
    val inValidSchema4 = new StructType(
      Array(
        new StructField(
          "s",
          new StructType(
            Array(
              new StructField("n", new IntegerType, false),
              new StructField(
                "arr",
                new ArrayType(
                  new StructType(
                    Array(
                      new StructField("name", new StringType(), true),
                      new StructField("mailbox", new StringType(), false)
                    )
                  ),
                  true // arr (ArrayType) containsNull
                ),
                false // arr (StructField) nullable
              )
            )
          ),
          true // s (StructField) nullable
        )
      )
    )

    val e4 = intercept[RuntimeException] {
      SchemaUtils.checkUnenforceableNotNullConstraints(inValidSchema4)
    }.getMessage

    assert(e4.contains("The element type of the field s.arr contains a NOT NULL constraint."))
  }

  test("updateMetadata withGlobalConfigDefaults") {
    // TODO: use DeltaConfigs...
    withTempDir { dir =>
      // note that the default for logRetentionDuration is 2592000000
      val hadoopConf = new Configuration()
      hadoopConf.set("logRetentionDuration", "1000")
      val metadata = Metadata(configuration = Map("logRetentionDuration" -> "2000"))

      val log = DeltaLogImpl.forTable(hadoopConf, dir.getCanonicalPath)
      log.startTransaction().commit(metadata :: Nil, manualUpdate, engineInfo)

      assert(log.deltaRetentionMillis == 2000)
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

  ///////////////////////////////////////////////////////////////////////////
  // commit() tests
  ///////////////////////////////////////////////////////////////////////////

  test("CommitInfo operation and engineInfo is persisted to the delta log") {
    withTempDir { dir =>
      val log = DeltaLog.forTable(new Configuration(), dir.getCanonicalPath)
      log.startTransaction().commit(Metadata() :: Nil, manualUpdate, engineInfo)

      val log2 = DeltaLog.forTable(new Configuration(), dir.getCanonicalPath)
      val commitInfo = log2.getCommitInfoAt(0)
      assert(commitInfo.getEngineInfo.isPresent)
      assert(commitInfo.getEngineInfo.get() == engineInfo)
      assert(commitInfo.getOperation == manualUpdate.getName.toString)

      // TODO: test commitInfo.operationParameters
    }
  }

  test("CommitInfo isBlindAppend is correctly set") {
    withTempDir { dir =>
      def verifyIsBlindAppend(version: Int, expected: Boolean): Unit = {
        val log = DeltaLog.forTable(new Configuration(), dir.getCanonicalPath)
        assert(log.getCommitInfoAt(version).getIsBlindAppend.get() == expected)
      }

      val add = AddFile("test", Map.empty, 1, 1, dataChange = false)

      val log0 = DeltaLog.forTable(new Configuration(), dir.getCanonicalPath)
      log0.startTransaction().commit(Metadata() :: add :: Nil, manualUpdate, engineInfo)
      verifyIsBlindAppend(0, expected = true)

      val log1 = DeltaLog.forTable(new Configuration(), dir.getCanonicalPath)
      log1.startTransaction().commit(add.remove :: Nil, manualUpdate, engineInfo)
      verifyIsBlindAppend(1, expected = false)

      val log2 = DeltaLog.forTable(new Configuration(), dir.getCanonicalPath)
      val txn2 = log2.startTransaction()
      txn2.markFilesAsRead(Literal.True)
      txn2.commit(add :: Nil, manualUpdate, engineInfo)
      verifyIsBlindAppend(2, expected = false)
    }
  }

  ///////////////////////////////////////////////////////////////////////////
  // checkForConflicts() tests
  ///////////////////////////////////////////////////////////////////////////

  private def setDataChangeFalse(fileActions: Seq[FileAction]): Seq[FileAction] = {
    fileActions.map {
      case a: AddFile => a.copy(dataChange = false)
      case r: RemoveFile => r.copy(dataChange = false)
      case cdc: AddCDCFile => cdc // change files are always dataChange = false
    }
  }

  //////////////////////////////////
  // protocolChangedException tests
  //////////////////////////////////

  test("concurrent protocol update should fail") {
    withLog(Nil) { log =>
      val tx1 = log.startTransaction()
      val tx2 = log.startTransaction()
      tx2.commit(Protocol(1, 2) :: Nil, manualUpdate, engineInfo)

      assertThrows[ProtocolChangedException] {
        tx1.commit(Protocol(1, 2) :: Nil, manualUpdate, engineInfo)
      }
    }
  }

  //////////////////////////////////
  // metadataChangedException tests
  //////////////////////////////////

  test("concurrent metadata update should fail") {
    withLog(Nil) { log =>
      val tx1 = log.startTransaction()
      val tx2 = log.startTransaction()
      tx2.updateMetadata(ConversionUtils.convertMetadata(Metadata(name = "foo")))
      tx2.commit(Iterable().asJava, manualUpdate, engineInfo)

      assertThrows[MetadataChangedException] {
        tx1.updateMetadata(ConversionUtils.convertMetadata(Metadata(name = "bar")))
        tx1.commit(Iterable().asJava, manualUpdate, engineInfo)
      }
    }
  }

  //////////////////////////////////
  // concurrentAppend tests
  //////////////////////////////////

  test("block concurrent commit when read partition was appended to by concurrent write") {
    withLog(addA_P1 :: addD_P2 :: addE_P3 :: Nil) { log =>
      val schema = log.update().getMetadata.getSchema
      val tx1 = log.startTransaction()
      // TX1 reads only P1
      val tx1Read = tx1.markFilesAsRead(new EqualTo(schema.column("part"), Literal.of("1")))
      assert(tx1Read.getFiles.asScala.toSeq.map(_.getPath) == A_P1 :: Nil)

      val tx2 = log.startTransaction()
      tx2.markFilesAsRead(Literal.True)
      // TX2 modifies only P1
      tx2.commit(addB_P1 :: Nil, manualUpdate, engineInfo)

      intercept[ConcurrentAppendException] {
        // P1 was modified
        tx1.commit(addC_P2 :: addE_P3 :: Nil, manualUpdate, engineInfo)
      }
    }
  }

  test("block concurrent commit on full table scan") {
    withLog(addA_P1 :: addD_P2 :: Nil) { log =>
      val schema = log.update().getMetadata.getSchema
      val tx1 = log.startTransaction()
      // TX1 full table scan
      tx1.markFilesAsRead(Literal.True)
      tx1.markFilesAsRead(new EqualTo(schema.column("part"), Literal.of("1")))

      val tx2 = log.startTransaction()
      tx2.markFilesAsRead(Literal.True)
      tx2.commit(addC_P2 :: addD_P2.remove :: Nil, manualUpdate, engineInfo)

      intercept[ConcurrentAppendException] {
        tx1.commit(addE_P3 :: addF_P3 :: Nil, manualUpdate, engineInfo)
      }
    }
  }

  test("no data change: allow data rearrange when new files concurrently added") {
    // This tests the case when isolationLevel == SnapshotIsolation
    withLog(addA_P1 :: addB_P1 :: Nil) { log =>
      val tx1 = log.startTransaction()
      tx1.markFilesAsRead(Literal.True)

      val tx2 = log.startTransaction()
      tx1.markFilesAsRead(Literal.True)
      tx2.commit(addE_P3 :: Nil, manualUpdate, engineInfo)

      // tx1 rearranges files (dataChange = false)
      tx1.commit(
        setDataChangeFalse(addA_P1.remove :: addB_P1.remove :: addC_P1 :: Nil),
        manualUpdate, engineInfo)

      assert(log.update().getAllFiles.asScala.map(_.getPath) == C_P1 :: E_P3 :: Nil)
    }
  }

  //////////////////////////////////
  // concurrentDeleteRead tests
  //////////////////////////////////

  test("block concurrent commit on read & delete conflicting partitions") {
    // txn.readFiles will be non-empty, so this covers the first ConcurrentDeleteReadException
    // case in checkForDeletedFilesAgainstCurrentTxnReadFiles
    withLog(addA_P1 :: addB_P1 :: Nil) { log =>
      val schema = log.update().getMetadata.getSchema
      val tx1 = log.startTransaction()
      // read P1
      tx1.markFilesAsRead(new EqualTo(schema.column("part"), Literal.of("1")))

      // tx2 commits before tx1
      val tx2 = log.startTransaction()
      tx2.markFilesAsRead(Literal.True)
      tx2.commit(addA_P1.remove :: Nil, manualUpdate, engineInfo)

      intercept[ConcurrentDeleteReadException] {
        // P1 read by TX1 was removed by TX2
        tx1.commit(addE_P3 :: Nil, manualUpdate, engineInfo)
      }
    }
  }

  test("readWholeTable should block concurrent delete") {
    // txn.readFiles will be empty, so this covers the second ConcurrentDeleteReadException
    // case in checkForDeletedFilesAgainstCurrentTxnReadFiles
    withLog(addA_P1 :: Nil) { log =>
      val tx1 = log.startTransaction()
      tx1.readWholeTable()

      // tx2 removes file
      val tx2 = log.startTransaction()
      tx2.commit(addA_P1.remove :: Nil, manualUpdate, engineInfo)

      intercept[ConcurrentDeleteReadException] {
        // tx1 reads the whole table but tx2 removes files before tx1 commits
        tx1.commit(addB_P1 :: Nil, manualUpdate, engineInfo)
      }
    }
  }

  //////////////////////////////////
  // concurrentDeleteDelete tests
  //////////////////////////////////

  test("block commit with concurrent removes on same file") {
    withLog(addA_P1 :: Nil) { log =>
      val tx1 = log.startTransaction()

      // tx2 removes file
      val tx2 = log.startTransaction()
      tx2.commit(addA_P1.remove :: Nil, manualUpdate, engineInfo)

      intercept[ConcurrentDeleteDeleteException] {
        // tx1 tries to remove the same file
        tx1.commit(addA_P1.remove :: Nil, manualUpdate, engineInfo)
      }
    }
  }

  //////////////////////////////////
  // concurrentTransaction tests
  //////////////////////////////////

  test("block concurrent set-txns with the same app id") {
    withLog(Nil) { log =>
      val tx1 = log.startTransaction()
      tx1.txnVersion("t1")

      val winningTxn = log.startTransaction()
      winningTxn.commit(SetTransaction("t1", 1, Some(1234L)) :: Nil, manualUpdate, engineInfo)

      intercept[ConcurrentTransactionException] {
        tx1.commit(Iterable().asJava, manualUpdate, engineInfo)
      }
    }
  }

  // TODO multiple concurrent commits, not just one (i.e. 1st doesn't conflict, 2nd does)

  // TODO: readWholeTable tests

  // TODO: test Checkpoint > partialWriteVisible (==> useRename)

  // TODO: test Checkpoint > !partialWriteVisible (==> !useRename)

  // TODO: test Checkpoint > correctly checkpoints all action types
}
