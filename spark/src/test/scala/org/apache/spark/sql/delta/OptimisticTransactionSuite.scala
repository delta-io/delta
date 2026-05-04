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
import java.io.File
import java.nio.file.FileAlreadyExistsException
import java.util.concurrent.{CountDownLatch, Executors, TimeUnit}

import scala.collection.JavaConverters._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._

import com.databricks.spark.util.Log4jUsageLogger
import org.apache.spark.sql.delta.DeltaOperations.{ManualUpdate, Truncate}
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
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.{EqualTo, Literal}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.{IntegerType, StructType, TimestampType}
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
    expectedErrorClass = Some("DELTA_CONCURRENT_APPEND.WITH_PARTITION_HINT"),
    expectedErrorMessageParameters = Some(Map(
      "operation" -> "TRUNCATE",
      "version" -> "1",
      "partitionValues" -> "\\[x=1\\]",
      "docLink" -> ".*"
    )))

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
    expectedErrorClass = Some("DELTA_CONCURRENT_DELETE_READ.WITH_PARTITION_HINT"),
    expectedErrorMessageParameters = Some(Map(
      "operation" -> "TRUNCATE",
      "version" -> "2",
      "partitionValues" -> "\\[x=1\\]",
      "docLink" -> ".*"
    )))

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
      Action.supportedProtocolVersion(featuresToExclude = Seq(CatalogOwnedTableFeature))),
    actions = Seq(
      Action.supportedProtocolVersion(featuresToExclude = Seq(CatalogOwnedTableFeature))))

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

  test("concurrent feature enablement with failConcurrentTransactionsAtUpgrade should conflict") {
    withSQLConf(
      DeltaSQLConf.DELTA_CONFLICT_CHECKER_ENFORCE_FEATURE_ENABLEMENT_VALIDATION.key -> "true") {
      withTempDir { tempDir =>
        val log = DeltaLog.forTable(spark, new Path(tempDir.getCanonicalPath))

        val metadata = Metadata(
          schemaString = new StructType().add("x", IntegerType).json,
          partitionColumns = Seq("x"))
        val protocol = Protocol(3, 7)
        log.startTransaction().commit(Seq(metadata, protocol), ManualUpdate)

        // Start a transaction that will write to the table concurrently with feature enablement.
        val txn = log.startTransaction()

        // Concurrently, enable the MaterializePartitionColumns feature
        // This feature has failConcurrentTransactionsAtUpgrade = true
        val newProtocol = txn.snapshot.protocol.withFeatures(
          Set(MaterializePartitionColumnsTableFeature))

        log.startTransaction().commit(Seq(newProtocol), ManualUpdate)

        // The original transaction should fail when trying to commit
        // because a feature with failConcurrentTransactionsAtUpgrade=true was added
        val e = intercept[io.delta.exceptions.ProtocolChangedException] {
          txn.commit(
            Seq(AddFile("test", Map("x" -> "1"), 1, 1, dataChange = true)),
            ManualUpdate)
        }

        // Verify the error message
        assert(e.getMessage.contains("The protocol version of the Delta table has been changed"))
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

  test("conflict event logs winningTxnOperation for observability") {
    withTempDir { tempDir =>
      val log = DeltaLog.forTable(spark, new Path(tempDir.getCanonicalPath))
      // Initialize delta table with partitioned schema.
      val metadata = Metadata(
        schemaString = new StructType().add("x", IntegerType).json,
        partitionColumns = Seq("x"),
        // Set isolation level to SERIALIZABLE to ensure conflict detection.
        configuration = Map(DeltaConfigs.ISOLATION_LEVEL.key -> Serializable.toString))
      log.startTransaction().commit(Seq(metadata), ManualUpdate)

      // Start a transaction that reads partition x=1.
      val txn = log.startTransaction()
      txn.filterFiles(EqualTo('x, Literal(1)) :: Nil)

      // Commit a concurrent write to the same partition that will cause conflict.
      log.startTransaction().commit(
        Seq(AddFile("a", Map("x" -> "1"), 1, 1, dataChange = true)),
        Truncate())

      // Attempt to write to the same partition - should conflict.
      val conflictLogs = Log4jUsageLogger.track {
        intercept[DeltaConcurrentModificationException] {
          txn.commit(
            Seq(AddFile("b", Map("x" -> "1"), 1, 1, dataChange = true)),
            Truncate())
        }
      }.filter(usageLog =>
        usageLog.metric == "tahoeEvent" &&
          usageLog.tags.getOrElse("opType", "").startsWith("delta.commit.conflict"))

      // Verify the conflict event is logged with winningTxnOperation.
      assert(conflictLogs.size == 1, "Expected exactly one conflict event to be logged")
      val conflictBlob = JsonUtils.fromJson[Map[String, Any]](conflictLogs.head.blob)
      assert(conflictBlob.get("winningTxnOperation")
        .exists(_.toString == "TRUNCATE"))
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
    expectedErrorClass: Option[String] = None,
    expectedErrorMessageParameters: String => Map[String, String] = _ => Map.empty) = {

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
            val addFiles = txn.filterFiles(newData)

            // txn2
            log.startTransaction().commit(concurrentActions(partCol), ManualUpdate)

            // txn1: remove files read in the partition and commit newData
            def commitTxn1 = {
                txn.commit(addFiles.map(_.remove) ++ newData, ManualUpdate)
            }

            if (expectedErrorClass.isDefined) {
              val e = intercept[DeltaConcurrentModificationException] {
                commitTxn1
              }
              checkError(
                e.asInstanceOf[DeltaThrowable],
                expectedErrorClass.get,
                Some("2D521"),
                expectedErrorMessageParameters(partCol)
                  ++ Map("tableName" -> s"delta.`${log.dataPath}`"),
                matchPVals = true
              )
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
    expectedErrorClass = Some("DELTA_CONCURRENT_APPEND.WITH_PARTITION_HINT"),
    expectedErrorMessageParameters = partCol => Map(
      "operation" -> "Manual Update",
      "partitionValues" -> s"\\[$partCol=0\\]",
      "version" -> "2",
      "docLink" -> ".*"
    )
  )

  testDynamicPartitionOverwrite(
    caseName = "concurrent append in different partition",
    concurrentActions = partCol => Seq(AddFile("y", Map(partCol -> "1"), 1, 1, dataChange = true))
  )

  testDynamicPartitionOverwrite(
    caseName = "concurrent delete in same partition",
    concurrentActions = partCol => Seq(
      RemoveFile("a", None, partitionValues = Map(partCol -> "0"))),
    expectedErrorClass = Some("DELTA_CONCURRENT_DELETE_DELETE.WITH_PARTITION_HINT"),
    expectedErrorMessageParameters = partCol => Map(
      "operation" -> "Manual Update",
      "partitionValues" -> s"\\[$partCol=0\\]",
      "version" -> "2",
      "docLink" -> ".*"
    )
  )

  testDynamicPartitionOverwrite(
    caseName = "concurrent delete in different partition",
    concurrentActions = partCol => Seq(
      RemoveFile("b", None, partitionValues = Map(partCol -> "1")))
  )

  for (enableNormalization <- BOOLEAN_DOMAIN) {
    test("filterFiles for timestamp partitions with different string formats, " +
      s"enableNormalization = $enableNormalization") {
      withSQLConf(
        DeltaSQLConf.DELTA_DYNAMIC_PARTITION_OVERWRITE_PARSE_PARTITION_VALUES.key ->
          enableNormalization.toString
      ) {
        DeltaTestUtils.withTimeZone("UTC") {
          withTempDir { tempDir =>
            val tablePath = tempDir.getCanonicalPath
            val log = DeltaLog.forTable(spark, tablePath)

            log.startTransaction().commit(Seq(
              Metadata(
                schemaString = new StructType()
                  .add("ts", TimestampType)
                  .add("value", IntegerType).json,
                partitionColumns = Seq("ts"))
            ), ManualUpdate)

            // Add files with non-UTC formatted timestamp partition values
            val nonUtcTimestamp = "2000-01-01 12:00:00"
            log.startTransaction().commit(
              Seq(
                AddFile("a", Map("ts" -> nonUtcTimestamp), 1, 1, dataChange = true),
                AddFile("b", Map("ts" -> "2000-02-02 12:00:00"), 1, 1, dataChange = true)),
              ManualUpdate)

            // Query using UTC formatted timestamp (different string, same logical value)
            val utcTimestamp = "2000-01-01T12:00:00.000000Z"
            val txn = log.startTransaction()
            val utcAddFile = AddFile("tmp", Map("ts" -> utcTimestamp), 0, 0, dataChange = false)
            val matchedFiles = txn.filterFiles(Seq(utcAddFile))

            if (enableNormalization) {
              assert(matchedFiles.map(_.path).toSet == Set("a"))
            } else {
              assert(matchedFiles.isEmpty)
            }
          }
        }
      }
    }
  }

  for (failOnError <- BOOLEAN_DOMAIN) {
    test("filterFiles falls back to string comparison when partition parsing fails, " +
      s"failOnError = $failOnError") {
      withSQLConf(
        DeltaSQLConf.DELTA_DYNAMIC_PARTITION_OVERWRITE_PARSE_PARTITION_VALUES.key -> "true",
        DeltaSQLConf.DELTA_FAIL_ON_PARTITION_VALUE_PARSING_ERROR.key -> failOnError.toString
      ) {
        withTempDir { tempDir =>
          val tablePath = tempDir.getCanonicalPath
          val log = DeltaLog.forTable(spark, tablePath)

          log.startTransaction().commit(Seq(
            Metadata(
              schemaString = new StructType()
                .add("part", IntegerType)
                .add("value", IntegerType).json,
              partitionColumns = Seq("part"))
          ), ManualUpdate)

          // Add existing file with an unparseable partition value.
          val badValue = "not_a_number"
          log.startTransaction().commit(
            Seq(AddFile("a", Map("part" -> badValue), 1, 1, dataChange = true)),
            ManualUpdate)

          // New file also has the same unparseable value
          val txn = log.startTransaction()
          val newFile = AddFile("tmp", Map("part" -> badValue), 0, 0, dataChange = false)

          if (failOnError) {
            checkError(
              intercept[DeltaRuntimeException] {
                txn.filterFiles(Seq(newFile))
              },
              condition = "DELTA_PARTITION_COLUMN_CAST_FAILED",
              sqlState = "22525",
              parameters = Map(
                "value" -> badValue,
                "dataType" -> "IntegerType",
                "columnName" -> "part")
            )
          } else {
            // Falls back to raw string comparison — strings match, so file "a" is returned
            val matched = txn.filterFiles(Seq(newFile))
            assert(matched.map(_.path).toSet == Set("a"))
          }
        }
      }
    }
  }

  for (failOnError <- BOOLEAN_DOMAIN) {
    test("filterFiles when existing files have unparseable partition values, " +
      s"failOnError = $failOnError") {
      withSQLConf(
        DeltaSQLConf.DELTA_DYNAMIC_PARTITION_OVERWRITE_PARSE_PARTITION_VALUES.key -> "true",
        DeltaSQLConf.DELTA_FAIL_ON_PARTITION_VALUE_PARSING_ERROR.key -> failOnError.toString
      ) {
        withTempDir { tempDir =>
          val tablePath = tempDir.getCanonicalPath
          val log = DeltaLog.forTable(spark, tablePath)

          log.startTransaction().commit(Seq(
            Metadata(
              schemaString = new StructType()
                .add("part", IntegerType)
                .add("value", IntegerType).json,
              partitionColumns = Seq("part"))
          ), ManualUpdate)

          // Existing file has an unparseable partition value.
          val badValue = "not_a_number"
          log.startTransaction().commit(
            Seq(AddFile("a", Map("part" -> badValue), 1, 1, dataChange = true)),
            ManualUpdate)

          // New file has a valid partition value. Only the UDF fails
          val txn = log.startTransaction()
          val newFile = AddFile("tmp", Map("part" -> "1"), 0, 0, dataChange = false)

          if (failOnError) {
            checkError(
              intercept[DeltaRuntimeException] {
                txn.filterFiles(Seq(newFile))
              },
              condition = "DELTA_PARTITION_COLUMN_CAST_FAILED",
              sqlState = "22525",
              parameters = Map(
                "value" -> badValue,
                "dataType" -> "IntegerType",
                "columnName" -> "part")
            )
          } else {
            // Falls back to raw string comparison — "1" != "not_a_number", so no match
            val matched = txn.filterFiles(Seq(newFile))
            assert(matched.isEmpty)
          }
        }
      }
    }
  }

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
        val protocol = Action.supportedProtocolVersion(
          featuresToExclude = Seq(CatalogOwnedTableFeature))
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
    withSQLConf(
      DeltaSQLConf.DELTA_WRITE_CHECKSUM_ENABLED.key -> "false",
      DeltaSQLConf.INCREMENTAL_COMMIT_ENABLED.key -> "true",
      DeltaSQLConf.INCREMENTAL_COMMIT_FORCE_VERIFY_IN_TESTS.key -> "false",
      DeltaSQLConf.DELTA_ALL_FILES_IN_CRC_VERIFICATION_MODE_ENABLED.key -> "false",
      DeltaSQLConf.DELTA_ALL_FILES_IN_CRC_FORCE_VERIFICATION_MODE_FOR_NON_UTC_ENABLED.key ->
        "false",
      DeltaSQLConf.INCREMENTAL_COMMIT_FORCE_VERIFY_IN_TESTS.key -> "false"
    ) {
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


  test("partition column changes not thrown for valid CREATE/REPLACE operations") {
    withTempDir { tempDir =>
      val tablePath = tempDir.getAbsolutePath

      sql(s"""CREATE TABLE delta.`$tablePath`
              USING delta
              PARTITIONED BY (part)
              AS SELECT id, id % 3 as part FROM range(10)""")

      sql(s"""CREATE OR REPLACE TABLE delta.`$tablePath`
              USING delta
              PARTITIONED BY (newpart)
              AS SELECT id, id % 5 as newpart FROM range(10)""")

      sql(s"""REPLACE TABLE delta.`$tablePath`
              USING delta
              PARTITIONED BY (newpart)
              AS SELECT id, id % 5 as newpart FROM range(10)""")
    }
  }

  Seq(("path", true), ("catalog", false)).foreach { case (tableType, isPath) =>
    Seq("dfv1", "dfv2", "sql").foreach { method =>
      Seq("error", "overwrite")
        .filterNot(mode => method == "dfv2" && mode == "overwrite")
        .foreach { mode =>
          test(s"partition column changes not thrown for $method $mode on new $tableType table") {
            withTempDir { tempDir =>
              val pathOrTable = getPathOrTable(tempDir, isPath, s"${method}_${mode}_${tableType}")

              try {
                writePartitionedTable(
                  pathOrTable, isPath, "part", 10, mode = mode, method = method)
                assertPartitionColumnsForTest(pathOrTable, isPath, Seq("part"))
              } finally {
                if (!isPath) sql(s"DROP TABLE IF EXISTS $pathOrTable")
              }
            }
          }
        }
    }
  }

  Seq(("path", true), ("catalog", false)).foreach { case (tableType, isPath) =>
    Seq("sql", "dfv1", "dfv2").foreach { method =>
      test(s"partition column changes not thrown for $method append on $tableType") {
        withTempDir { tempDir =>
          val pathOrTable = getPathOrTable(tempDir, isPath, s"${method}_append_${tableType}")

          try {
            writePartitionedTable(
              pathOrTable, isPath, "part", 10, mode = "error", method = method)
            writePartitionedTable(
              pathOrTable, isPath, "part", 10, mode = "append",
              rangeStart = 10, rangeEnd = 20, method = method)
            assertPartitionColumnsForTest(pathOrTable, isPath, Seq("part"))
          } finally {
            if (!isPath) sql(s"DROP TABLE IF EXISTS $pathOrTable")
          }
        }
      }
    }
  }

  // Among others, this includes a test containing an overwrite using .saveAsTable() which
  // creates a ReplaceTable operation under the hood.
  Seq(
    ("path", true),
    ("catalog", false)
  ).foreach { case (tableType, isPath) =>
    Seq("dfv1", "dfv2", "sql").foreach { method =>
      Seq("overwrite", "createOrReplace").foreach { mode =>
        // Skip unsupported combinations
        if (!(method == "dfv1" && mode == "createOrReplace")) {
          test(s"partition col changes allowed for $method $tableType $mode") {
            withTempDir { tempDir =>
              val pathOrTable = if (isPath) {
                tempDir.getAbsolutePath
              } else {
                s"test_table_${method}_${tableType}_${mode}"
              }

              // Create initial table
              writeThreeColumnTable(pathOrTable, "col1")

              if (isPath) {
                assertPartitionColumns(pathOrTable, Seq("col1"))
              } else {
                assertPartitionColumns(new TableIdentifier(pathOrTable), Seq("col1"))
              }

              // Overwrite with different partition column
              writeThreeColumnTable(pathOrTable, "col2", method = method, mode = mode)

              val expectedAnswer = spark.range(10)
                .withColumn("col1", col("id") % 3).withColumn("col2", col("id") % 5)

              if (isPath) {
                assertPartitionColumns(pathOrTable, Seq("col2"))
                checkAnswer(spark.read.format("delta").load(pathOrTable), expectedAnswer)
              } else {
                assertPartitionColumns(new TableIdentifier(pathOrTable), Seq("col2"))
                checkAnswer(spark.table(pathOrTable), expectedAnswer)
                sql(s"DROP TABLE IF EXISTS $pathOrTable")
              }
            }
          }
        }
      }
    }
  }

  test("partition column changes validation modes for UPDATE operations") {
    val testCases = Seq(
      ("true", Some(classOf[DeltaAnalysisException]), true),
      ("log-only", None, true),
      ("false", None, false)
    )

    testCases.foreach { case (mode, exceptionClassOpt, expectLogEvent) =>
      withSQLConf(DeltaSQLConf.DELTA_PARTITION_COLUMN_CHANGE_CHECK.key -> mode) {
        withTempDir { tempDir =>
          val tablePath = tempDir.getAbsolutePath
          writeThreeColumnTable(tablePath, "col1")

          val deltaLog = DeltaLog.forTable(spark, tablePath)
          val txn = deltaLog.startTransaction()
          val newMetadata = txn.metadata.copy(partitionColumns = Seq("col2"))

          val logRecords = Log4jUsageLogger.track {
            exceptionClassOpt match {
              case Some(_) =>
                checkError(
                  intercept[DeltaAnalysisException] {
                    txn.commit(
                      Seq(newMetadata),
                      DeltaOperations.Update(predicate = Some(EqualTo(Literal(1), Literal(1))))
                    )
                  },
                  condition = "DELTA_UNSUPPORTED_PARTITION_COLUMN_CHANGE",
                  sqlState = "42P10",
                  parameters = Map(
                    "operation" -> "UPDATE",
                    "oldPartitionColumns" -> "col1",
                    "newPartitionColumns" -> "col2"
                  )
                )
              case None =>
                // Should succeed without throwing
                txn.commit(
                  Seq(newMetadata),
                  DeltaOperations.Update(predicate = Some(EqualTo(Literal(1), Literal(1))))
                )
            }
          }

          // Check for log event if expected
          if (expectLogEvent) {
            val matchingRecords =
              filterUsageRecords(logRecords, "delta.metadataCheck.illegalPartitionColumnChange")
            assert(matchingRecords.nonEmpty,
              "Expected to find log event 'delta.metadataCheck.illegalPartitionColumnChange' " +
                "but it was not logged")
          }

          // Verify final state only if commit succeeded
          if (exceptionClassOpt.isEmpty) {
            assertPartitionColumns(tablePath, Seq("col2"))
          }
        }
      }
    }
  }

  test("partition column changes validation default mode blocks UPDATE operations") {
    // Test that the default behavior (without explicit config) is to block partition column changes
    withTempDir { tempDir =>
      val tablePath = tempDir.getAbsolutePath
      writeThreeColumnTable(tablePath, "col1")

      val deltaLog = DeltaLog.forTable(spark, tablePath)
      val txn = deltaLog.startTransaction()
      val newMetadata = txn.metadata.copy(partitionColumns = Seq("col2"))

      checkError(
        intercept[DeltaAnalysisException] {
          txn.commit(
            Seq(newMetadata),
            DeltaOperations.Update(predicate = Some(EqualTo(Literal(1), Literal(1))))
          )
        },
        condition = "DELTA_UNSUPPORTED_PARTITION_COLUMN_CHANGE",
        sqlState = "42P10",
        parameters = Map(
          "operation" -> "UPDATE",
          "oldPartitionColumns" -> "col1",
          "newPartitionColumns" -> "col2"
        )
      )
      assertPartitionColumns(tablePath, Seq("col1"))
    }
  }

  Seq(
    // Recreation of running DFv1 .save() overwrite changing partition cols.
    ("blocked for Write(Overwrite, partitionBy)", "WRITE",
      (_: Metadata) => DeltaOperations.Write(SaveMode.Overwrite, partitionBy = Some(Seq("col2")))),

    // Recreation of running DFv1 .save() replaceWhere changing partition cols.
    ("blocked for Write(Overwrite, partitionBy, predicate)", "WRITE",
      (_: Metadata) => DeltaOperations.Write(SaveMode.Overwrite, partitionBy = Some(Seq("col2")),
        predicate = Some("col1=0"))),

    // Recreation of running DFv1 .save() DPO changing partition cols.
    ("blocked for Write(Overwrite, partitionBy, DPO)", "WRITE",
      (_: Metadata) => DeltaOperations.Write(SaveMode.Overwrite, partitionBy = Some(Seq("col2")),
        isDynamicPartitionOverwrite = Some(true))),

    // Recreation of running DFv1 .saveAsTable() overwrite changing partition cols.
    ("blocked for ReplaceTable(isV1SaveAsTableOverwrite=true)", "CREATE OR REPLACE TABLE AS SELECT",
      (newMeta: Metadata) => DeltaOperations.ReplaceTable(
        metadata = newMeta,
        isManaged = true,
        orCreate = true,
        asSelect = true,
        isV1SaveAsTableOverwrite = Some(true)))
  ).foreach { case (testSuffix, expectedOpName, mkOp) =>
    test(s"partition column changes $testSuffix") {
      withTempDir { tempDir =>
        val tablePath = tempDir.getAbsolutePath
        writeThreeColumnTable(tablePath, "col1")

        val deltaLog = DeltaLog.forTable(spark, tablePath)
        val txn = deltaLog.startTransaction()
        val newMetadata = txn.metadata.copy(partitionColumns = Seq("col2"))

        checkError(
          intercept[DeltaAnalysisException] {
            txn.commit(Seq(newMetadata), mkOp(newMetadata))
          },
          condition = "DELTA_UNSUPPORTED_PARTITION_COLUMN_CHANGE",
          sqlState = "42P10",
          parameters = Map(
            "operation" -> expectedOpName,
            "oldPartitionColumns" -> "col1",
            "newPartitionColumns" -> "col2"
          )
        )
        assertPartitionColumns(tablePath, Seq("col1"))
      }
    }
  }

  test("partition column changes allowed for CLONE operations") {
    withTempDir { sourceDir =>
      withTempDir { targetDir =>
        val sourcePath = sourceDir.getAbsolutePath
        val targetPath = targetDir.getAbsolutePath

        writePartitionedTable(sourcePath, isPath = true, "part", 10)

        // Test SHALLOW CLONE
        sql(s"CREATE TABLE delta.`$targetPath` SHALLOW CLONE delta.`$sourcePath`")
        assertPartitionColumns(targetPath, Seq("part"))
      }
    }
  }

  test("partition column changes allowed for RenameColumn when partition column renamed") {
    withTempDir { tempDir =>
      val tablePath = tempDir.getAbsolutePath
      writePartitionedTable(tablePath, isPath = true, "part", 10)

      sql(s"ALTER TABLE delta.`$tablePath` SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name')")
      sql(s"ALTER TABLE delta.`$tablePath` RENAME COLUMN part TO renamed_part")

      assertPartitionColumns(tablePath, Seq("renamed_part"))
    }
  }

  Seq(("path", true), ("catalog", false)).foreach { case (tableType, isPath) =>
    test(s"dfv1 overwrite without overwriteSchema blocks partition column changes - $tableType") {
      withTempDir { tempDir =>
        val pathOrTable = getPathOrTable(tempDir, isPath, s"dfv1_overwrite_${tableType}")

        try {
          writeThreeColumnTable(pathOrTable, "col1")
          assertPartitionColumnsForTest(pathOrTable, isPath, Seq("col1"))

          val df = spark.range(10)
            .withColumn("col1", col("id") % 3)
            .withColumn("col2", col("id") % 5)

          val ex = intercept[DeltaAnalysisException] {
            val writer = df.write.format("delta").mode("overwrite").partitionBy("col2")
            if (isPath) writer.save(pathOrTable) else writer.saveAsTable(pathOrTable)
          }

          assert(ex.getMessage.contains("overwriteSchema") ||
            ex.getMessage.contains("incompatible"))
          assertPartitionColumnsForTest(pathOrTable, isPath, Seq("col1"))
        } finally {
          if (!isPath) sql(s"DROP TABLE IF EXISTS $pathOrTable")
        }
      }
    }
  }

  // Helper methods
  private def writePartitionedTable(
    pathOrTable: String,
    isPath: Boolean,
    partitionCol: String,
    range: Int,
    mode: String = "error",
    rangeStart: Int = 0,
    rangeEnd: Int = -1,
    method: String = "dfv1"): Unit = {

    val end = if (rangeEnd == -1) range else rangeEnd
    val df = spark.range(rangeStart, end)
      .withColumn(partitionCol, col("id") % 3)

    val tableRef = if (isPath) s"delta.`$pathOrTable`" else pathOrTable
    val v2WriteDf = df.writeTo(tableRef).using("delta").partitionedBy(col(partitionCol))

    method match {
      case "dfv1" =>
        val writer = df.write.format("delta").mode(mode).partitionBy(partitionCol)
        if (isPath) {
          writer.save(pathOrTable)
        } else {
          writer.saveAsTable(pathOrTable)
        }
      case "dfv2" =>
        mode match {
          case "error" | "errorifexists" =>
            v2WriteDf.create()
          case "overwrite" =>
            v2WriteDf.replace()
          case "append" =>
            df.writeTo(tableRef).append()
          case "createOrReplace" =>
            v2WriteDf.createOrReplace()
        }
      case "sql" =>
        val tempView = s"temp_view_${System.nanoTime()}"
        df.createOrReplaceTempView(tempView)
        val stmt = mode match {
          case "append" => s"INSERT INTO $tableRef SELECT * FROM $tempView"
          case _ =>
            s"""CREATE OR REPLACE TABLE $tableRef
               |USING delta PARTITIONED BY ($partitionCol)
               |AS SELECT * FROM $tempView""".stripMargin
        }
        sql(stmt)
    }
  }

  private def writeThreeColumnTable(
    pathOrTable: String,
    partitionCol: String,
    method: String = "dfv1",
    mode: String = "error"): Unit = {

    val df = spark.range(10)
      .withColumn("col1", col("id") % 3)
      .withColumn("col2", col("id") % 5)

    val isPath = pathOrTable.contains("/")

    method match {
      case "dfv1" =>
        val writer = df.write.format("delta").partitionBy(partitionCol)
        if (mode == "overwrite") {
          writer.option("overwriteSchema", "true")
        }
        if (isPath) {
          writer.mode(mode).save(pathOrTable)
        } else {
          writer.mode(mode).saveAsTable(pathOrTable)
        }

      case "dfv2" =>
        val tableRef = if (isPath) s"delta.`$pathOrTable`" else pathOrTable
        mode match {
          case "error" =>
            df.writeTo(tableRef).using("delta").partitionedBy(col(partitionCol)).create()
          case "overwrite" =>
            df.writeTo(tableRef).using("delta").partitionedBy(col(partitionCol)).replace()
          case "createOrReplace" =>
            df.writeTo(tableRef).using("delta").partitionedBy(col(partitionCol))
              .createOrReplace()
        }

      case "sql" =>
        val tempView = s"temp_view_${System.nanoTime()}"
        df.createOrReplaceTempView(tempView)

        val tableRef = if (isPath) s"delta.`$pathOrTable`" else pathOrTable
        val stmt = mode match {
          case "append" => s"INSERT INTO $tableRef SELECT * FROM $tempView"
          case _ =>
            s"""CREATE OR REPLACE TABLE $tableRef
               |USING delta PARTITIONED BY ($partitionCol)
               |AS SELECT * FROM $tempView""".stripMargin
        }
        sql(stmt)
    }
  }

  private def assertPartitionColumns(pathOrTableId: Any, expected: Seq[String]): Unit = {
    val deltaLog = pathOrTableId match {
      case path: String => DeltaLog.forTable(spark, path)
      case tableId: TableIdentifier => DeltaLog.forTable(spark, tableId)
    }
    assert(deltaLog.update().metadata.partitionColumns === expected)
  }

  // Helper to generate path or table name based on test context
  private def getPathOrTable(
      tempDir: File,
      isPath: Boolean,
      testSuffix: String): String = {
    if (isPath) {
      tempDir.getAbsolutePath
    } else {
      s"test_table_$testSuffix"
    }
  }

  // Helper to assert partition columns for either path or table
  private def assertPartitionColumnsForTest(
      pathOrTable: String,
      isPath: Boolean,
      expected: Seq[String]): Unit = {
    if (isPath) {
      assertPartitionColumns(pathOrTable, expected)
    } else {
      assertPartitionColumns(new TableIdentifier(pathOrTable), expected)
    }
  }

  test("filesForScan is thread-safe when invoked concurrently on a single transaction") {
    withTempDir { tempDir =>
      val tablePath = tempDir.getCanonicalPath
      val log = DeltaLog.forTable(spark, tablePath)

      // Set up a partitioned table with one file per partition.
      log.startTransaction().commit(Seq(
        Metadata(
          schemaString = new StructType()
            .add("part", IntegerType)
            .add("value", IntegerType).json,
          partitionColumns = Seq("part"))
      ), ManualUpdate)

      val numPartitions = 16
      val seedFiles = (0 until numPartitions).map { i =>
        AddFile(s"f$i", Map("part" -> i.toString), 1, 1, dataChange = true)
      }
      log.startTransaction().commit(seedFiles, ManualUpdate)

      val txn = log.startTransaction()

      // Sanity check: filesForScan returns expected files when called concurrently. Each
      // worker queries a single partition and must observe its own file independently of
      // the other threads racing to update the transaction's state.
      val numThreads = 8
      val scanPool = Executors.newFixedThreadPool(numThreads)
      locally {
        implicit val ec: ExecutionContext = ExecutionContext.fromExecutorService(scanPool)
        try {
          val scanLatch = new CountDownLatch(1)
          val scanFutures = (0 until numPartitions).map { i =>
            Future {
              scanLatch.await()
              val filter = EqualTo('part, Literal(i))
              val scan = txn.filesForScan(filter :: Nil)
              assert(scan.files.map(_.path).toSet === Set(s"f$i"))
            }
          }
          scanLatch.countDown()
          Await.result(Future.sequence(scanFutures), 60.seconds)
        } finally {
          scanPool.shutdown()
          scanPool.awaitTermination(60, TimeUnit.SECONDS)
        }
      }

      // Stress the mutator path that filesForScan ultimately uses (trackFilesRead) with a
      // large, evenly partitioned write load timed so all threads start simultaneously.
      // With a non-thread-safe collection (e.g. mutable.HashSet) this deterministically
      // loses entries or corrupts the table on the box this test ran on; with a
      // ConcurrentHashMap-backed set every entry is observed.
      val stressThreads = 32
      val perThread = 1000
      val totalExpected = stressThreads * perThread
      val stressBatches = (0 until stressThreads).map { t =>
        (0 until perThread).map { j =>
          AddFile(s"stress-t${t}-j${j}", Map("part" -> "0"), 1, 1, dataChange = true)
        }
      }
      val pool = Executors.newFixedThreadPool(stressThreads)
      locally {
        implicit val ec: ExecutionContext = ExecutionContext.fromExecutorService(pool)
        try {
          val startLatch = new CountDownLatch(1)
          val readyLatch = new CountDownLatch(stressThreads)
          val futures = stressBatches.map { batch =>
            Future {
              readyLatch.countDown()
              startLatch.await()
              txn.trackFilesRead(batch)
            }
          }
          assert(readyLatch.await(60, TimeUnit.SECONDS),
            "Timed out waiting for stress workers to reach the start barrier.")
          startLatch.countDown()
          Await.result(Future.sequence(futures), 120.seconds)
        } finally {
          pool.shutdown()
          pool.awaitTermination(60, TimeUnit.SECONDS)
        }
      }

      // Access the protected `readFiles` field reflectively so the test is decoupled
      // from whether the underlying collection is a Scala or Java Set.
      val readFilesField = txn.getClass.getDeclaredFields
        .find(_.getName.endsWith("readFiles"))
        .getOrElse(fail("Could not locate readFiles field on the transaction class"))
      readFilesField.setAccessible(true)
      val tracked: Set[AddFile] = readFilesField.get(txn) match {
        case javaSet: java.util.Collection[_] =>
          javaSet.asScala.toSet.asInstanceOf[Set[AddFile]]
        case scalaSet: scala.collection.Iterable[_] =>
          scalaSet.toSet.asInstanceOf[Set[AddFile]]
        case other => fail(s"Unexpected readFiles container type: ${other.getClass}")
      }
      // Tracked files = the files initially scanned (one per partition) plus every
      // synthetic file added by the stress phase.
      val expectedSize = numPartitions + totalExpected
      assert(tracked.size === expectedSize,
        s"Expected $expectedSize tracked read files, got ${tracked.size}. " +
          s"This indicates lost updates from concurrent updates to readFiles, meaning " +
          s"the underlying collection is not thread-safe.")
    }
  }
}
