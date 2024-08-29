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

// scalastyle:off import.ordering.noEmptyLine
import java.nio.file.FileAlreadyExistsException
import java.util.Optional

import com.databricks.spark.util.Log4jUsageLogger
import org.apache.spark.sql.delta.DeltaOperations.ManualUpdate
import org.apache.spark.sql.delta.DeltaTestUtils.createTestAddFile
import org.apache.spark.sql.delta.actions.{Action, AddFile, CommitInfo, Metadata, Protocol, RemoveFile, SetTransaction}
import org.apache.spark.sql.delta.coordinatedcommits.{CommitCoordinatorBuilder, CommitCoordinatorProvider, InMemoryCommitCoordinator, InMemoryCommitCoordinatorBuilder, TableCommitCoordinatorClient}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaTestImplicits._
import org.apache.spark.sql.delta.util.{FileNames, JsonUtils}
import io.delta.storage.LogStore
import io.delta.storage.commit.{CommitCoordinatorClient, CommitFailedException, CommitResponse, TableDescriptor, UpdatedActions}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.{EqualTo, Literal}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{IntegerType, StructType}
import org.apache.spark.util.ManualClock


class OptimisticTransactionSuite
  extends OptimisticTransactionLegacyTests
  with OptimisticTransactionSuiteBase {

  import testImplicits._

  // scalastyle:off: removeFile
  private val addA = createTestAddFile(encodedPath = "a")
  private val addB = createTestAddFile(encodedPath = "b")

  /* ************************** *
   * Allowed concurrent actions *
   * ************************** */

  check(
    "append / append",
    conflicts = false,
    reads = Seq(
      t => t.metadata
    ),
    concurrentWrites = Seq(
      addA),
    actions = Seq(
      addB))

  check(
    "disjoint txns",
    conflicts = false,
    reads = Seq(
      t => t.txnVersion("t1")
    ),
    concurrentWrites = Seq(
      SetTransaction("t2", 0, Some(1234L))),
    actions = Nil)

  check(
    "disjoint delete / read",
    conflicts = false,
    setup = Seq(
      Metadata(
        schemaString = new StructType().add("x", IntegerType).json,
        partitionColumns = Seq("x")),
      AddFile("a", Map("x" -> "2"), 1, 1, dataChange = true)
    ),
    reads = Seq(
      t => t.filterFiles(EqualTo('x, Literal(1)) :: Nil)
    ),
    concurrentWrites = Seq(
      RemoveFile("a", Some(4))),
    actions = Seq())

  check(
    "disjoint add / read",
    conflicts = false,
    setup = Seq(
      Metadata(
        schemaString = new StructType().add("x", IntegerType).json,
        partitionColumns = Seq("x"))
    ),
    reads = Seq(
      t => t.filterFiles(EqualTo('x, Literal(1)) :: Nil)
    ),
    concurrentWrites = Seq(
      AddFile("a", Map("x" -> "2"), 1, 1, dataChange = true)),
    actions = Seq())

  /* ***************************** *
   * Disallowed concurrent actions *
   * ***************************** */

  check(
    "delete / delete",
    conflicts = true,
    reads = Nil,
    concurrentWrites = Seq(
      RemoveFile("a", Some(4))),
    actions = Seq(
      RemoveFile("a", Some(5))))

  check(
    "add / read + write",
    conflicts = true,
    setup = Seq(
      Metadata(
        schemaString = new StructType().add("x", IntegerType).json,
        partitionColumns = Seq("x"))
    ),
    reads = Seq(
      t => t.filterFiles(EqualTo('x, Literal(1)) :: Nil)
    ),
    concurrentWrites = Seq(
      AddFile("a", Map("x" -> "1"), 1, 1, dataChange = true)),
    actions = Seq(AddFile("b", Map("x" -> "1"), 1, 1, dataChange = true)),
    // commit info should show operation as truncate, because that's the operation used by the
    // harness
    errorMessageHint = Some("[x=1]" :: "TRUNCATE" :: Nil))

  check(
    "add / read + no write",  // no write = no real conflicting change even though data was added
    conflicts = false,        // so this should not conflict
    setup = Seq(
      Metadata(
        schemaString = new StructType().add("x", IntegerType).json,
        partitionColumns = Seq("x"))
    ),
    reads = Seq(
      t => t.filterFiles(EqualTo('x, Literal(1)) :: Nil)
    ),
    concurrentWrites = Seq(
      AddFile("a", Map("x" -> "1"), 1, 1, dataChange = true)),
    actions = Seq())

  check(
    "add in part=2 / read from part=1,2 and write to part=1",
    conflicts = true,
    setup = Seq(
      Metadata(
        schemaString = new StructType().add("x", IntegerType).json,
        partitionColumns = Seq("x"))
    ),
    reads = Seq(
      t => {
        // Filter files twice - once for x=1 and again for x=2
        t.filterFiles(Seq(EqualTo('x, Literal(1))))
        t.filterFiles(Seq(EqualTo('x, Literal(2))))
      }
    ),
    concurrentWrites = Seq(
      AddFile(
        path = "a",
        partitionValues = Map("x" -> "1"),
        size = 1,
        modificationTime = 1,
        dataChange = true)
    ),
    actions = Seq(
      AddFile(
        path = "b",
        partitionValues = Map("x" -> "2"),
        size = 1,
        modificationTime = 1,
        dataChange = true)
    ))

  check(
    "delete / read",
    conflicts = true,
    setup = Seq(
      Metadata(
        schemaString = new StructType().add("x", IntegerType).json,
        partitionColumns = Seq("x")),
      AddFile("a", Map("x" -> "1"), 1, 1, dataChange = true)
    ),
    reads = Seq(
      t => t.filterFiles(EqualTo('x, Literal(1)) :: Nil)
    ),
    concurrentWrites = Seq(
      RemoveFile("a", Some(4))),
    actions = Seq(),
    errorMessageHint = Some("a in partition [x=1]" :: "TRUNCATE" :: Nil))

  check(
    "schema change",
    conflicts = true,
    reads = Seq(
      t => t.metadata
    ),
    concurrentWrites = Seq(
      Metadata()),
    actions = Nil)

  check(
    "conflicting txns",
    conflicts = true,
    reads = Seq(
      t => t.txnVersion("t1")
    ),
    concurrentWrites = Seq(
      SetTransaction("t1", 0, Some(1234L))),
    actions = Nil)

  check(
    "upgrade / upgrade",
    conflicts = true,
    reads = Seq(
      t => t.metadata
    ),
    concurrentWrites = Seq(
      Action.supportedProtocolVersion()),
    actions = Seq(
      Action.supportedProtocolVersion()))

  check(
    "taint whole table",
    conflicts = true,
    setup = Seq(
      Metadata(
        schemaString = new StructType().add("x", IntegerType).json,
        partitionColumns = Seq("x")),
      AddFile("a", Map("x" -> "2"), 1, 1, dataChange = true)
    ),
    reads = Seq(
      t => t.filterFiles(EqualTo('x, Literal(1)) :: Nil),
      // `readWholeTable` should disallow any concurrent change, even if the change
      // is disjoint with the earlier filter
      t => t.readWholeTable()
    ),
    concurrentWrites = Seq(
      AddFile("b", Map("x" -> "3"), 1, 1, dataChange = true)),
    actions = Seq(
      AddFile("c", Map("x" -> "4"), 1, 1, dataChange = true)))

  check(
    "taint whole table + concurrent remove",
    conflicts = true,
    setup = Seq(
      Metadata(schemaString = new StructType().add("x", IntegerType).json),
      AddFile("a", Map.empty, 1, 1, dataChange = true)
    ),
    reads = Seq(
      // `readWholeTable` should disallow any concurrent `RemoveFile`s.
      t => t.readWholeTable()
    ),
    concurrentWrites = Seq(
      RemoveFile("a", Some(4L))),
    actions = Seq(
      AddFile("b", Map.empty, 1, 1, dataChange = true)))

  override def beforeEach(): Unit = {
    super.beforeEach()
    CommitCoordinatorProvider.clearNonDefaultBuilders()
  }

  test("initial commit without metadata should fail") {
    withTempDir { tempDir =>
      val log = DeltaLog.forTable(spark, new Path(tempDir.getCanonicalPath))
      val txn = log.startTransaction()
      withSQLConf(DeltaSQLConf.DELTA_COMMIT_VALIDATION_ENABLED.key -> "true") {
        val e = intercept[DeltaIllegalStateException] {
          txn.commit(Nil, ManualUpdate)
        }
        assert(e.getMessage == DeltaErrors.metadataAbsentException().getMessage)
      }
    }
  }

  test("initial commit with multiple metadata actions should fail") {
    withTempDir { tempDir =>
      val log = DeltaLog.forTable(spark, new Path(tempDir.getAbsolutePath))
      val txn = log.startTransaction()
      val e = intercept[AssertionError] {
        txn.commit(Seq(Metadata(), Metadata()), ManualUpdate)
      }
      assert(e.getMessage.contains("Cannot change the metadata more than once in a transaction."))
    }
  }

  test("enabling Coordinated Commits on an existing table should create commit dir") {
    withTempDir { tempDir =>
      val log = DeltaLog.forTable(spark, new Path(tempDir.getAbsolutePath))
      val metadata = Metadata()
      log.startTransaction().commit(Seq(metadata), ManualUpdate)
      val fs = log.logPath.getFileSystem(log.newDeltaHadoopConf())
      val commitDir = FileNames.commitDirPath(log.logPath)
      // Delete commit directory.
      fs.delete(commitDir)
      assert(!fs.exists(commitDir))
      // With no Coordinated Commits conf, commit directory should not be created.
      log.startTransaction().commit(Seq(metadata), ManualUpdate)
      assert(!fs.exists(commitDir))
      // Enabling Coordinated Commits on an existing table should create the commit dir.
      CommitCoordinatorProvider.registerBuilder(InMemoryCommitCoordinatorBuilder(3))
      val newMetadata = metadata.copy(configuration =
        (metadata.configuration ++
          Map(DeltaConfigs.COORDINATED_COMMITS_COORDINATOR_NAME.key -> "in-memory")).toMap)
      log.startTransaction().commit(Seq(newMetadata), ManualUpdate)
      assert(fs.exists(commitDir))
      log.update().ensureCommitFilesBackfilled()
      // With no new Coordinated Commits conf, commit directory should not be created and so the
      // transaction should fail because of corrupted dir.
      fs.delete(commitDir)
      assert(!fs.exists(commitDir))
      intercept[java.io.FileNotFoundException] {
        log.startTransaction().commit(Seq(newMetadata), ManualUpdate)
      }
    }
  }

  test("AddFile with different partition schema compared to metadata should fail") {
    withTempDir { tempDir =>
      val log = DeltaLog.forTable(spark, new Path(tempDir.getAbsolutePath))
      val initTxn = log.startTransaction()
      initTxn.updateMetadataForNewTable(Metadata(
        schemaString = StructType.fromDDL("col2 string, a int").json,
        partitionColumns = Seq("col2")))
      initTxn.commit(Seq(), ManualUpdate)
      withSQLConf(DeltaSQLConf.DELTA_COMMIT_VALIDATION_ENABLED.key -> "true") {
        val e = intercept[IllegalStateException] {
          log.startTransaction().commit(Seq(AddFile(
            log.dataPath.toString, Map("col3" -> "1"), 12322, 0L, true, null, null)), ManualUpdate)
        }
        assert(e.getMessage == DeltaErrors.addFilePartitioningMismatchException(
          Seq("col3"), Seq("col2")).getMessage)
      }
    }
  }

  test("isolation level shouldn't be null") {
    withTempDir { tempDir =>
      val log = DeltaLog.forTable(spark, new Path(tempDir.getCanonicalPath))

      log.startTransaction().commit(Seq(Metadata()), ManualUpdate)

      val txn = log.startTransaction()
      txn.commit(addA :: Nil, ManualUpdate)

      val isolationLevels = log.history.getHistory(Some(10)).map(_.isolationLevel)
      assert(isolationLevels.size == 2)
      assert(isolationLevels(0).exists(_.contains("Serializable")))
      assert(isolationLevels(0).exists(_.contains("Serializable")))
    }
  }

  test("every transaction should use a unique identifier in the commit") {
    withTempDir { tempDir =>
      // Initialize delta table.
      val clock = new ManualClock()
      val log = DeltaLog.forTable(spark, new Path(tempDir.getCanonicalPath), clock)
      log.startTransaction().commit(Seq(Metadata()), ManualUpdate)
      clock.advance(100)

      // Start two transactions which commits at same time with same content.
      val txn1 = log.startTransaction()
      val txn2 = log.startTransaction()
      clock.advance(100)
      val version1 = txn1.commit(Seq(), ManualUpdate)
      val version2 = txn2.commit(Seq(), ManualUpdate)

      // Validate that actions in both transactions are not exactly same.
      def readActions(version: Long): Seq[Action] = {
        log.store.read(FileNames.unsafeDeltaFile(log.logPath, version), log.newDeltaHadoopConf())
          .map(Action.fromJson)
      }
      def removeTxnIdAndMetricsFromActions(actions: Seq[Action]): Seq[Action] = actions.map {
        case c: CommitInfo => c.copy(txnId = None, operationMetrics = None)
        case other => other
      }
      val actions1 = readActions(version1)
      val actions2 = readActions(version2)
      val actionsWithoutTxnId1 = removeTxnIdAndMetricsFromActions(actions1)
      val actionsWithoutTxnId2 = removeTxnIdAndMetricsFromActions(actions2)
      assert(actions1 !== actions2)
      // Without the txn id, the actions are same as of today but they need not be in future. In
      // future we might have other fields which may make these actions from two different
      // transactions different. In that case, the below assertion can be removed.
      assert(actionsWithoutTxnId1 === actionsWithoutTxnId2)
    }
  }

  test("pre-command actions committed") {
    withTempDir { tempDir =>
      // Initialize delta table.
      val log = DeltaLog.forTable(spark, new Path(tempDir.getCanonicalPath))
      log.startTransaction().commit(Seq(Metadata()), ManualUpdate)

      val txn = log.startTransaction()
      txn.updateSetTransaction("TestAppId", 1L, None)
      val version = txn.commit(Seq(), ManualUpdate)

      def readActions(version: Long): Seq[Action] = {
        log.store.read(FileNames.unsafeDeltaFile(log.logPath, version), log.newDeltaHadoopConf())
          .map(Action.fromJson)
      }
      val actions = readActions(version)
      assert(actions.collectFirst {
        case SetTransaction("TestAppId", 1L, _) =>
      }.isDefined)
    }
  }

  test("has SetTransaction version conflicts") {
    withTempDir { tempDir =>
      // Initialize delta table.
      val log = DeltaLog.forTable(spark, new Path(tempDir.getCanonicalPath))
      log.startTransaction().commit(Seq(Metadata()), ManualUpdate)

      val txn = log.startTransaction()
      txn.updateSetTransaction("TestAppId", 1L, None)
      val e = intercept[IllegalArgumentException] {
        txn.commit(Seq(SetTransaction("TestAppId", 2L, None)), ManualUpdate)
      }
      assert(e.getMessage == DeltaErrors.setTransactionVersionConflict("TestAppId", 2L, 1L)
        .getMessage)
    }
  }

  test("removes duplicate SetTransactions") {
    withTempDir { tempDir =>
      // Initialize delta table.
      val log = DeltaLog.forTable(spark, new Path(tempDir.getCanonicalPath))
      log.startTransaction().commit(Seq(Metadata()), ManualUpdate)

      val txn = log.startTransaction()
      txn.updateSetTransaction("TestAppId", 1L, None)
      val version = txn.commit(Seq(SetTransaction("TestAppId", 1L, None)), ManualUpdate)
      def readActions(version: Long): Seq[Action] = {
        log.store.read(FileNames.unsafeDeltaFile(log.logPath, version), log.newDeltaHadoopConf())
          .map(Action.fromJson)
      }
      assert(readActions(version).collectFirst {
        case SetTransaction("TestAppId", 1L, _) =>
      }.isDefined)
    }
  }

  test("preCommitLogSegment is updated during conflict checking") {
    withTempDir { tempDir =>
      val log = DeltaLog.forTable(spark, new Path(tempDir.getCanonicalPath))
      log.startTransaction().commit(Seq(Metadata()), ManualUpdate)
      sql(s"ALTER TABLE delta.`${tempDir.getAbsolutePath}` " +
        s"SET TBLPROPERTIES (${DeltaConfigs.CHECKPOINT_INTERVAL.key} = 10)")
      val testTxn = log.startTransaction()
      val testTxnStartTs = System.currentTimeMillis()
      for (_ <- 1 to 11) {
        log.startTransaction().commit(Seq.empty, ManualUpdate)
      }
      val testTxnEndTs = System.currentTimeMillis()

      // preCommitLogSegment should not get updated until a commit is triggered
      assert(testTxn.preCommitLogSegment.version == 1)
      assert(testTxn.preCommitLogSegment.lastCommitFileModificationTimestamp < testTxnStartTs)
      assert(testTxn.preCommitLogSegment.deltas.size == 2)
      assert(testTxn.preCommitLogSegment.checkpointProvider.isEmpty)

      testTxn.commit(Seq.empty, ManualUpdate)

      // preCommitLogSegment should get updated to the version right before the txn commits
      assert(testTxn.preCommitLogSegment.version == 12)
      assert(testTxn.preCommitLogSegment.lastCommitFileModificationTimestamp < testTxnEndTs)
      assert(testTxn.preCommitLogSegment.deltas.size == 2)
      assert(testTxn.preCommitLogSegment.checkpointProvider.version == 10)
    }
  }

  test("Limited retries for non-conflict retryable CommitFailedExceptions") {
    val commitCoordinatorName = "retryable-non-conflict-commit-coordinator"
    var commitAttempts = 0
    val numRetries = "100"
    val numNonConflictRetries = "10"
    val initialNonConflictErrors = 5
    val initialConflictErrors = 5

    object RetryableNonConflictCommitCoordinatorBuilder$ extends CommitCoordinatorBuilder {

      override def getName: String = commitCoordinatorName

      val commitCoordinatorClient: InMemoryCommitCoordinator = {
        new InMemoryCommitCoordinator(batchSize = 1000L) {
          override def commit(
              logStore: LogStore,
              hadoopConf: Configuration,
              tableDesc: TableDescriptor,
              commitVersion: Long,
              actions: java.util.Iterator[String],
              updatedActions: UpdatedActions): CommitResponse = {
            // Fail all commits except first one
            if (commitVersion == 0) {
              return super.commit(
                logStore,
                hadoopConf,
                tableDesc,
                commitVersion,
                actions,
                updatedActions)
            }
            commitAttempts += 1
            throw new CommitFailedException(
              true,
              commitAttempts > initialNonConflictErrors &&
                commitAttempts <= (initialNonConflictErrors + initialConflictErrors),
              "")
          }
        }
      }
      override def build(
          spark: SparkSession,
          conf: Map[String, String]): CommitCoordinatorClient = commitCoordinatorClient
    }

    CommitCoordinatorProvider.registerBuilder(RetryableNonConflictCommitCoordinatorBuilder$)

    withSQLConf(
        DeltaSQLConf.DELTA_MAX_RETRY_COMMIT_ATTEMPTS.key -> numRetries,
        DeltaSQLConf.DELTA_MAX_NON_CONFLICT_RETRY_COMMIT_ATTEMPTS.key -> numNonConflictRetries) {
      withTempDir { tempDir =>
        val log = DeltaLog.forTable(spark, new Path(tempDir.getCanonicalPath))
        val conf =
          Map(DeltaConfigs.COORDINATED_COMMITS_COORDINATOR_NAME.key -> commitCoordinatorName)
        log.startTransaction().commit(Seq(Metadata(configuration = conf)), ManualUpdate)
        val testTxn = log.startTransaction()
        intercept[CommitFailedException] { testTxn.commit(Seq.empty, ManualUpdate) }
        // num-attempts = 1 + num-retries
        assert(commitAttempts ==
          (initialNonConflictErrors + initialConflictErrors + numNonConflictRetries.toInt + 1))
      }
    }
  }

  test("No retries for FileAlreadyExistsException with commit-coordinator") {
    val commitCoordinatorName = "file-already-exists-commit-coordinator"
    var commitAttempts = 0

    object FileAlreadyExistsCommitCoordinatorBuilder extends CommitCoordinatorBuilder {

      override def getName: String = commitCoordinatorName

      lazy val commitCoordinatorClient: CommitCoordinatorClient = {
        new InMemoryCommitCoordinator(batchSize = 1000L) {
          override def commit(
              logStore: LogStore,
              hadoopConf: Configuration,
              tableDesc: TableDescriptor,
              commitVersion: Long,
              actions: java.util.Iterator[String],
              updatedActions: UpdatedActions): CommitResponse = {
            // Fail all commits except first one
            if (commitVersion == 0) {
              return super.commit(
                logStore,
                hadoopConf,
                tableDesc,
                commitVersion,
                actions,
                updatedActions)
            }
            commitAttempts += 1
            throw new FileAlreadyExistsException("Commit-File Already Exists")
          }
        }
      }
      override def build(
          spark: SparkSession,
          conf: Map[String, String]): CommitCoordinatorClient = commitCoordinatorClient
    }

    CommitCoordinatorProvider.registerBuilder(FileAlreadyExistsCommitCoordinatorBuilder)

    withSQLConf(
        DeltaSQLConf.DELTA_MAX_RETRY_COMMIT_ATTEMPTS.key -> "100",
        DeltaSQLConf.DELTA_MAX_NON_CONFLICT_RETRY_COMMIT_ATTEMPTS.key -> "10") {
      withTempDir { tempDir =>
        val log = DeltaLog.forTable(spark, new Path(tempDir.getCanonicalPath))
        val conf =
          Map(DeltaConfigs.COORDINATED_COMMITS_COORDINATOR_NAME.key -> commitCoordinatorName)
        log.startTransaction().commit(Seq(Metadata(configuration = conf)), ManualUpdate)
        val testTxn = log.startTransaction()
        intercept[FileAlreadyExistsException] { testTxn.commit(Seq.empty, ManualUpdate) }
        // Test that there are no retries for the FileAlreadyExistsException in
        // CommitCoordinatorClient.commit()
        // num-attempts(1) = 1 + num-retries(0)
        assert(commitAttempts == 1)
      }
    }
  }

  /**
   * Here we test whether ConflictChecker correctly resolves conflicts when using
   * OptimisticTransaction.filterFiles(partitions) to perform dynamic partition overwrites.
   *
   */
  private def testDynamicPartitionOverwrite(
    caseName: String,
    concurrentActions: String => Seq[Action],
    expectedException: Option[String => String] = None) = {

    // We test with a partition column named "partitionValues" to make sure we correctly skip
    // rewriting the filters
    for (partCol <- Seq("part", "partitionValues")) {
      test("filterFiles(partitions) correctly updates readPredicates and ConflictChecker " +
        s"correctly detects conflicts for $caseName with partition column [$partCol]") {
        withTempDir { tempDir =>

            val tablePath = tempDir.getCanonicalPath
            val log = DeltaLog.forTable(spark, tablePath)
            // set up
            log.startTransaction.commit(Seq(
              Metadata(
                schemaString = new StructType()
                  .add(partCol, IntegerType)
                  .add("value", IntegerType).json,
                partitionColumns = Seq(partCol))
            ), ManualUpdate)
            log.startTransaction.commit(
              Seq(AddFile("a", Map(partCol -> "0"), 1, 1, dataChange = true),
                AddFile("b", Map(partCol -> "1"), 1, 1, dataChange = true)),
              ManualUpdate)


            // new data we want to overwrite dynamically to the table
            val newData = Seq(AddFile("x", Map(partCol -> "0"), 1, 1, dataChange = true))

            // txn1: read files in partitions of our new data (part=0)
            val txn = log.startTransaction()
            val addFiles =
                txn.filterFiles(newData.map(_.partitionValues).toSet)

            // txn2
            log.startTransaction().commit(concurrentActions(partCol), ManualUpdate)

            // txn1: remove files read in the partition and commit newData
            def commitTxn1 = {
                txn.commit(addFiles.map(_.remove) ++ newData, ManualUpdate)
            }

            if (expectedException.nonEmpty) {
              val e = intercept[DeltaConcurrentModificationException] {
                commitTxn1
              }
              assert(e.getMessage.contains(expectedException.get(partCol)))
            } else {
              commitTxn1
            }
        }
      }
    }
  }

  testDynamicPartitionOverwrite(
    caseName = "concurrent append in same partition",
    concurrentActions = partCol => Seq(AddFile("y", Map(partCol -> "0"), 1, 1, dataChange = true)),
    expectedException = Some(partCol =>
      s"Files were added to partition [$partCol=0] by a concurrent update.")
  )

  testDynamicPartitionOverwrite(
    caseName = "concurrent append in different partition",
    concurrentActions = partCol => Seq(AddFile("y", Map(partCol -> "1"), 1, 1, dataChange = true))
  )

  testDynamicPartitionOverwrite(
    caseName = "concurrent delete in same partition",
    concurrentActions = partCol => Seq(
      RemoveFile("a", None, partitionValues = Map(partCol -> "0"))),
    expectedException = Some(partCol =>
      "This transaction attempted to delete one or more files that were deleted (for example a) " +
        "by a concurrent update")
  )

  testDynamicPartitionOverwrite(
    caseName = "concurrent delete in different partition",
    concurrentActions = partCol => Seq(
      RemoveFile("b", None, partitionValues = Map(partCol -> "1")))
  )

  test("can set partition columns in first commit") {
    withTempDir { tableDir =>
      val partitionColumns = Array("part")
      val exampleAddFile = AddFile(
        path = "test-path",
        partitionValues = Map("part" -> "one"),
        size = 1234,
        modificationTime = 5678,
        dataChange = true,
        stats = """{"numRecords": 1}""",
        tags = Map.empty)
      val deltaLog = DeltaLog.forTable(spark, tableDir)
      val schema = new StructType()
        .add("id", "long")
        .add("part", "string")
      deltaLog.withNewTransaction { txn =>
        val protocol = Action.supportedProtocolVersion()
        val metadata = Metadata(
          schemaString = schema.json,
          partitionColumns = partitionColumns)
        txn.commit(Seq(protocol, metadata, exampleAddFile), DeltaOperations.ManualUpdate)
      }
      val snapshot = deltaLog.update()
      assert(snapshot.metadata.partitionColumns.sameElements(partitionColumns))
    }
  }

  test("only single Protocol action per commit - implicit") {
    withTempDir { tableDir =>
      val deltaLog = DeltaLog.forTable(spark, tableDir)
      val schema = new StructType()
        .add("id", "long")
        .add("col", "string")
      val e = intercept[java.lang.AssertionError] {
        deltaLog.withNewTransaction { txn =>
          val protocol = Protocol(2, 3)
          val metadata = Metadata(
            schemaString = schema.json,
            configuration = Map("delta.enableChangeDataFeed" -> "true"))
          txn.commit(Seq(protocol, metadata), DeltaOperations.ManualUpdate)
        }
      }
      assert(e.getMessage.contains(
        "assertion failed: Cannot change the protocol more than once in a transaction."))
    }
  }

  test("only single Protocol action per commit - explicit") {
    withTempDir { tableDir =>
      val deltaLog = DeltaLog.forTable(spark, tableDir)
      val e = intercept[java.lang.AssertionError] {
        deltaLog.withNewTransaction { txn =>
          val protocol1 = Protocol(2, 3)
          val protocol2 = Protocol(1, 4)
          txn.commit(Seq(protocol1, protocol2), DeltaOperations.ManualUpdate)
        }
      }
      assert(e.getMessage.contains(
        "assertion failed: Cannot change the protocol more than once in a transaction."))
    }
  }

  test("DVs cannot be added to files without numRecords stat") {
    withTempPath { tempPath =>
      val path = tempPath.getPath
      val deltaLog = DeltaLog.forTable(spark, path)
      val firstFile = writeDuplicateActionsData(path).head
      enableDeletionVectorsInTable(deltaLog)
      val (addFileWithDV, removeFile) = addDVToFileInTable(path, firstFile)
      val addFileWithDVWithoutStats = addFileWithDV.copy(stats = null)
      testRuntimeErrorOnCommit(Seq(addFileWithDVWithoutStats, removeFile), deltaLog) { e =>
        val expErrorClass = "DELTA_DELETION_VECTOR_MISSING_NUM_RECORDS"
        assert(e.getErrorClass == expErrorClass)
        assert(e.getSqlState == "2D521")
      }
    }
  }

  test("commitInfo tags") {
    withTempDir { tableDir =>
      val deltaLog = DeltaLog.forTable(spark, tableDir)
      val schema = new StructType().add("id", "long")

      def checkLastCommitTags(expectedTags: Option[Map[String, String]]): Unit = {
        val ci = deltaLog.getChanges(deltaLog.update().version).map(_._2).flatten.collectFirst {
          case ci: CommitInfo => ci
        }.head
        assert(ci.tags === expectedTags)
      }

      val metadata = Metadata(schemaString = schema.json)
      // Check empty tags
      deltaLog.withNewTransaction { txn =>
        txn.commit(metadata :: Nil, DeltaOperations.ManualUpdate, tags = Map.empty)
      }
      checkLastCommitTags(expectedTags = None)

      deltaLog.withNewTransaction { txn =>
        txn.commit(addA :: Nil, DeltaOperations.Write(SaveMode.Append), tags = Map.empty)
      }
      checkLastCommitTags(expectedTags = None)

      // Check non-empty tags
      val tags1 = Map("testTag1" -> "testValue1")
      deltaLog.withNewTransaction { txn =>
        txn.commit(metadata :: Nil, DeltaOperations.ManualUpdate, tags = tags1)
      }
      checkLastCommitTags(expectedTags = Some(tags1))

      val tags2 = Map("testTag1" -> "testValue1", "testTag2" -> "testValue2")
      deltaLog.withNewTransaction { txn =>
        txn.commit(addB :: Nil, DeltaOperations.Write(SaveMode.Append), tags = tags2)
      }
      checkLastCommitTags(expectedTags = Some(tags2))
    }
  }


  test("empty commits are elided on write by default") {
    withTempDir { tableDir =>
      val df = Seq((1, 0), (2, 1)).toDF("key", "value")
      df.write.format("delta").mode("append").save(tableDir.getCanonicalPath)

      val deltaLog = DeltaLog.forTable(spark, tableDir)

      val expectedSnapshot = deltaLog.update()
      val expectedDeltaVersion = expectedSnapshot.version

      val emptyDf = Seq.empty[(Integer, Integer)].toDF("key", "value")
      emptyDf.write.format("delta").mode("append").save(tableDir.getCanonicalPath)

      val actualSnapshot = deltaLog.update()
      val actualDeltaVersion = actualSnapshot.version

      checkAnswer(spark.read.format("delta").load(tableDir.getCanonicalPath),
        Row(1, 0) :: Row(2, 1) :: Nil)

      assert(expectedDeltaVersion === actualDeltaVersion)
    }
  }

  Seq(true, false).foreach { skip =>
    test(s"Elide empty commits when requested - skipRecordingEmptyCommits=$skip") {
      withSQLConf(DeltaSQLConf.DELTA_SKIP_RECORDING_EMPTY_COMMITS.key -> skip.toString) {
        withTempDir { tableDir =>
          val df = Seq((1, 0), (2, 1)).toDF("key", "value")
          df.write.format("delta").mode("append").save(tableDir.getCanonicalPath)

          val deltaLog = DeltaLog.forTable(spark, tableDir)

          val expectedSnapshot = deltaLog.update()
          val expectedDeltaVersion = if (skip) {
            expectedSnapshot.version
          } else {
            expectedSnapshot.version + 1
          }

          val emptyDf = Seq.empty[(Integer, Integer)].toDF("key", "value")
          emptyDf.write.format("delta").mode("append").save(tableDir.getCanonicalPath)

          val actualSnapshot = deltaLog.update()
          val actualDeltaVersion = actualSnapshot.version

          checkAnswer(spark.read.format("delta").load(tableDir.getCanonicalPath),
            Row(1, 0) :: Row(2, 1) :: Nil)

          assert(expectedDeltaVersion === actualDeltaVersion)
        }
      }
    }
  }

  BOOLEAN_DOMAIN.foreach { conflict =>
    test(s"commitLarge should handle Commit Failed Exception with conflict: $conflict") {
      withTempDir { tempDir =>
        val deltaLog = DeltaLog.forTable(spark, tempDir.getAbsolutePath)
        val commitCoordinatorName = "retryable-conflict-commit-coordinator"
        class RetryableConflictCommitCoordinatorClient
          extends InMemoryCommitCoordinator(batchSize = 5) {
          override def commit(
              logStore: LogStore,
              hadoopConf: Configuration,
              tableDesc: TableDescriptor,
              commitVersion: Long,
              actions: java.util.Iterator[String],
              updatedActions: UpdatedActions): CommitResponse = {
            if (updatedActions.getCommitInfo.asInstanceOf[CommitInfo].operation
                == DeltaOperations.OP_RESTORE) {
              deltaLog.startTransaction().commit(addB :: Nil, ManualUpdate)
              throw new CommitFailedException(true, conflict, "")
            }
            super.commit(logStore, hadoopConf, tableDesc, commitVersion, actions, updatedActions)
          }
        }
        object RetryableConflictCommitCoordinatorBuilder$ extends CommitCoordinatorBuilder {
          lazy val commitCoordinatorClient = new RetryableConflictCommitCoordinatorClient()
          override def getName: String = commitCoordinatorName
          override def build(
              spark: SparkSession,
              conf: Map[String, String]): CommitCoordinatorClient = commitCoordinatorClient
        }
        CommitCoordinatorProvider.registerBuilder(RetryableConflictCommitCoordinatorBuilder$)
        val conf =
          Map(DeltaConfigs.COORDINATED_COMMITS_COORDINATOR_NAME.key -> commitCoordinatorName)
        deltaLog.startTransaction().commit(Seq(Metadata(configuration = conf)), ManualUpdate)
        deltaLog.startTransaction().commit(addA :: Nil, ManualUpdate)
        val records = Log4jUsageLogger.track {
          // commitLarge must fail because of a conflicting commit at version-2.
          val e = intercept[Exception] {
            deltaLog.startTransaction().commitLarge(
              spark,
              nonProtocolMetadataActions = (addB :: Nil).iterator,
              newProtocolOpt = None,
              op = DeltaOperations.Restore(Some(0), None),
              context = Map.empty,
              metrics = Map.empty)
          }
          if (conflict) {
            assert(e.isInstanceOf[ConcurrentWriteException])
            assert(
              e.getMessage.contains(
                "A concurrent transaction has written new data since the current transaction " +
                  s"read the table. Please try the operation again"))
          } else {
              assert(e.isInstanceOf[CommitFailedException])
          }
          assert(deltaLog.update().version == 2)
        }
        val failureRecord = filterUsageRecords(records, "delta.commitLarge.failure")
        assert(failureRecord.size == 1)
        val data = JsonUtils.fromJson[Map[String, Any]](failureRecord.head.blob)
        assert(data("fromCoordinatedCommits") == true)
        assert(data("fromCoordinatedCommitsConflict") == conflict)
        assert(data("fromCoordinatedCommitsRetryable") == true)
      }
    }
  }

  test("Append does not trigger snapshot state computation") {
    withTempDir { tableDir =>
      val df = Seq((1, 0), (2, 1)).toDF("key", "value")
      df.write.format("delta").mode("append").save(tableDir.getCanonicalPath)

      val deltaLog = DeltaLog.forTable(spark, tableDir)
      val preCommitSnapshot = deltaLog.update()
      assert(!preCommitSnapshot.stateReconstructionTriggered)

      df.write.format("delta").mode("append").save(tableDir.getCanonicalPath)

      val postCommitSnapshot = deltaLog.update()
      assert(!preCommitSnapshot.stateReconstructionTriggered)
      assert(!postCommitSnapshot.stateReconstructionTriggered)
    }
  }

}
