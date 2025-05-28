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

import java.util.{Collections, Optional}

import scala.collection.JavaConverters._
import scala.collection.immutable.Seq

import io.delta.kernel.Table
import io.delta.kernel.data.Row
import io.delta.kernel.defaults.utils.TestRow
import io.delta.kernel.engine.Engine
import io.delta.kernel.expressions.Literal
import io.delta.kernel.hook.PostCommitHook
import io.delta.kernel.internal.{DeltaLogActionUtils, SnapshotImpl}
import io.delta.kernel.internal.TableConfig.TOMBSTONE_RETENTION
import io.delta.kernel.internal.actions._
import io.delta.kernel.internal.compaction.LogCompactionWriter
import io.delta.kernel.internal.fs.Path
import io.delta.kernel.internal.hook.LogCompactionHook
import io.delta.kernel.internal.replay.ActionsIterator
import io.delta.kernel.internal.util.FileNames.DeltaLogFileType
import io.delta.kernel.internal.util.ManualClock
import io.delta.kernel.internal.util.Utils.singletonCloseableIterator
import io.delta.kernel.types.{IntegerType, StructType}
import io.delta.kernel.utils.FileStatus

import org.apache.spark.sql.delta.{DeltaLog, DomainMetadataTableFeature}
import org.apache.spark.sql.delta.DeltaOperations.Truncate
import org.apache.spark.sql.delta.actions.{DomainMetadata => DeltaSparkDomainMetadata, TableFeatureProtocolUtils}
import org.apache.spark.sql.delta.sources.DeltaSQLConf

/**
 * Test suite for io.delta.kernel.internal.compaction.LogCompactionWriter
 */
class LogCompactionWriterSuite extends CheckpointSuiteBase {
  val COMPACTED_SCHEMA =
    new StructType()
      .add("txn", SetTransaction.FULL_SCHEMA)
      .add("add", AddFile.FULL_SCHEMA)
      .add("remove", RemoveFile.FULL_SCHEMA)
      .add("metaData", Metadata.FULL_SCHEMA)
      .add("protocol", Protocol.FULL_SCHEMA)
      // .add("cdc", new StructType())
      .add("domainMetadata", DomainMetadata.FULL_SCHEMA);

  val ADD_INDEX = 1
  val REMOVE_INDEX = 2
  val METADATA_INDEX = 3
  val PROTOCOL_INDEX = 4
  val DM_INDEX = 5

  val ADD_REM_PATH_INDEX = 0
  val DM_NAME_INDEX = 0

  // check if a row is all null
  def rowIsNull(row: Row): Boolean = {
    val schema = row.getSchema()
    for (ordinal <- 0 until schema.length()) {
      if (!row.isNullAt(ordinal)) {
        return false
      }
    }
    true
  }

  // Get the expected actions from the log.  We filter down to just the actions that end up in a
  // compacted log file, and also filter out adds that have been removed as well as duplicate
  // metadata/protocol actions
  def getActionsFromLog(
      tablePath: Path,
      engine: Engine,
      startVersion: Long,
      endVersion: Long): Seq[TestRow] = {
    val files = DeltaLogActionUtils.listDeltaLogFilesAsIter(
      engine,
      Collections.singleton(DeltaLogFileType.COMMIT),
      tablePath,
      startVersion,
      Optional.of(endVersion),
      false /* mustBeRecreatable */ )
      .toInMemoryList()
    Collections.reverse(files) // we want things in reverse order
    val actions =
      new ActionsIterator(engine, files, COMPACTED_SCHEMA, Optional.empty())
    val removed = scala.collection.mutable.HashSet.empty[String]
    val seenDomains = scala.collection.mutable.HashSet.empty[String]
    var seenMetadata = false
    var seenProtocol = false
    actions.toSeq.flatMap { wrapper =>
      wrapper.getColumnarBatch().getRows.toSeq.flatMap { row =>
        if (!row.isNullAt(REMOVE_INDEX)) {
          val removeRow = row.getStruct(REMOVE_INDEX)
          val path = removeRow.getString(ADD_REM_PATH_INDEX)
          removed += path
        }

        if (!row.isNullAt(ADD_INDEX)) {
          val addRow = row.getStruct(ADD_INDEX)
          val path = addRow.getString(ADD_REM_PATH_INDEX)
          if (!removed.contains(path)) {
            Some(TestRow(row))
          } else {
            None
          }
        } else if (!row.isNullAt(METADATA_INDEX)) {
          if (!seenMetadata) {
            seenMetadata = true
            Some(TestRow(row))
          } else {
            None
          }
        } else if (!row.isNullAt(PROTOCOL_INDEX)) {
          if (!seenProtocol) {
            seenProtocol = true
            Some(TestRow(row))
          } else {
            None
          }
        } else if (!row.isNullAt(DM_INDEX)) {
          val dm = row.getStruct(DM_INDEX)
          val domain = dm.getString(DM_NAME_INDEX)
          if (!seenDomains.contains(domain)) {
            seenDomains += domain
            Some(TestRow(row))
          } else {
            None
          }
        } else if (!rowIsNull(row)) {
          Some(TestRow(row))
        } else {
          None
        }
      }
    }
  }

  def getActionsFromCompacted(
      compactedPath: String,
      engine: Engine): Seq[Row] = {
    val fileStatus = FileStatus.of(compactedPath, 0, 0)
    val batches = engine
      .getJsonHandler()
      .readJsonFiles(
        singletonCloseableIterator(fileStatus),
        COMPACTED_SCHEMA,
        Optional.empty())
    batches.toSeq.flatMap(_.getRows().toSeq)
  }

  def addDomainMetadata(path: String, d1Val: String, d2Val: String): Unit = {
    spark.sql(
      s"""
         |ALTER TABLE delta.`$path`
         |SET TBLPROPERTIES
         |('${TableFeatureProtocolUtils.propertyKey(DomainMetadataTableFeature)}' = 'enabled')
         |""".stripMargin)

    val deltaLog = DeltaLog.forTable(spark, path)
    val domainMetadata = DeltaSparkDomainMetadata("testDomain1", d1Val, false) ::
      DeltaSparkDomainMetadata("testDomain2", d2Val, false) :: Nil
    deltaLog.startTransaction().commit(domainMetadata, Truncate())
  }

  Seq(false, true).foreach { includeRemoves =>
    Seq(false, true).foreach { includeDM =>
      val removesMsg = if (includeRemoves) " and removes" else ""
      val dmMsg = if (includeDM) ", include multiple PandM and DomainMetadata" else ""
      test(s"commits containing adds${removesMsg}${dmMsg}") {
        withTempDirAndEngine { (tablePath, engine) =>
          addData(tablePath, alternateBetweenAddsAndRemoves = includeRemoves, numberIter = 8)
          if (includeDM) {
            addDomainMetadata(tablePath, "", "{\"key1\":\"value1\"}")
            addDomainMetadata(tablePath, "here", "{\"key2\":\"value2\"}")
          }

          val expectedLastCommit = if (includeDM) {
            11 // 0-7 for add/removes + 2 for enable+set DomainMetatdata
          } else {
            7 // 0-7 for add/removes
          }

          val actionsFromCommits =
            getActionsFromLog(new Path(tablePath), engine, 0, expectedLastCommit)

          val dataPath = new Path(s"file:${tablePath}")
          val logPath = new Path(s"file:${tablePath}", "_delta_log")

          val hook = new LogCompactionHook(
            dataPath,
            logPath,
            0,
            expectedLastCommit,
            0)
          hook.threadSafeInvoke(engine)
          val endCommitStr = f"$expectedLastCommit%020d"
          val compactedPath =
            tablePath + s"/_delta_log/00000000000000000000.${endCommitStr}.compacted.json"
          val actionsFromCompacted = getActionsFromCompacted(compactedPath, engine)

          checkAnswer(actionsFromCompacted, actionsFromCommits)
        }
      }
    }
  }

  Seq(false, true).foreach { includeRemoves =>
    val testMsgUpdate = if (includeRemoves) " and removes" else ""
    test(s"Read table with adds$testMsgUpdate") {
      withTempDirAndEngine { (tablePath, engine) =>
        addData(tablePath, alternateBetweenAddsAndRemoves = includeRemoves, numberIter = 10)

        spark.conf.set(DeltaSQLConf.DELTALOG_MINOR_COMPACTION_USE_FOR_READS.key, "false")
        val withoutCompactionData = readUsingSpark(tablePath)

        val dataPath = new Path(s"file:${tablePath}")
        val logPath = new Path(s"file:${tablePath}", "_delta_log")
        val hook = new LogCompactionHook(dataPath, logPath, 0, 9, 0)
        hook.threadSafeInvoke(engine)

        spark.conf.set(DeltaSQLConf.DELTALOG_MINOR_COMPACTION_USE_FOR_READS.key, "true")
        val withCompactionData = readUsingSpark(tablePath)

        checkAnswer(withCompactionData, withoutCompactionData)
      }
    }
  }

  test(s"Error if not enough commits") {
    withTempDirAndEngine { (tablePath, engine) =>
      addData(tablePath, alternateBetweenAddsAndRemoves = false, numberIter = 2)
      val dataPath = new Path(s"file:${tablePath}")
      val logPath = new Path(s"file:${tablePath}", "_delta_log")
      val hook = new LogCompactionHook(dataPath, logPath, 0, 5, 0)
      val ex = intercept[IllegalArgumentException] {
        hook.threadSafeInvoke(engine)
      }
      assert(ex.getMessage.contains(
        "Asked to compact between versions 0 and 5, but found 2 delta files"))
    }
  }

  test("Hook is generated correctly and when expected") {
    withTempDirAndEngine { (tablePath, engine) =>
      val clock = new ManualClock(0)
      val schema = new StructType().add("col", IntegerType.INTEGER)
      val dataPath = new Path(s"file:${tablePath}")
      val logPath = new Path(s"file:${tablePath}", "_delta_log")
      createEmptyTable(engine, tablePath, schema, clock = clock)
      val table = Table.forPath(engine, tablePath)
      val metadata = table.getLatestSnapshot(engine).asInstanceOf[SnapshotImpl].getMetadata()
      val tombstoneRetention = TOMBSTONE_RETENTION.fromMetadata(metadata)
      clock.setTime(tombstoneRetention) // set to the retention time so (time - retention) == 0
      val compactionInterval = 3
      var hooksFound = 0
      // start at 1 since the create of the table is 0
      for (commitNum <- 1 to 5) {
        val txn =
          createTxn(engine, tablePath, clock = clock, logCompactionInterval = compactionInterval)
        val data = generateData(
          schema,
          Seq.empty,
          Map.empty[String, Literal],
          batchSize = 1,
          numBatches = 1)
        val commitResult =
          commitAppendData(engine, txn, data = Seq(Map.empty[String, Literal] -> data))
        // expect every compactionInterval
        val expectHook = ((commitNum + 1) % compactionInterval == 0)
        assert(LogCompactionWriter.shouldCompact(commitNum, compactionInterval) == expectHook)
        var foundHook = false
        for (hook <- commitResult.getPostCommitHooks().asScala) {
          if (hook.getType() == PostCommitHook.PostCommitHookType.LOG_COMPACTION) {
            assert(!foundHook) // there should never be more than one
            foundHook = true
            hooksFound += 1
            val logCompactionHook = hook.asInstanceOf[LogCompactionHook]
            assert(logCompactionHook.getDataPath() == dataPath)
            assert(logCompactionHook.getLogPath() == logPath)
            assert(logCompactionHook.getStartVersion() == commitNum + 1 - compactionInterval)
            assert(logCompactionHook.getCommitVersion() == commitNum)
            assert(logCompactionHook.getMinFileRetentionTimestampMillis() == 0)
          }
          hook.threadSafeInvoke(engine)
        }
        assert(foundHook == expectHook)
      }
      assert(hooksFound == 2) // expect 0<->2 and 3<->5
    }
  }
}
