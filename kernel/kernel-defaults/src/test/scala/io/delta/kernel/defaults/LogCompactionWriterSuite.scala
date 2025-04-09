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

import java.util.{Arrays, Collections, HashSet, Optional}

import scala.collection.mutable

import io.delta.kernel.data.Row
import io.delta.kernel.defaults.utils.TestRow
import io.delta.kernel.engine.Engine
import io.delta.kernel.internal.DeltaLogActionUtils
import io.delta.kernel.internal.actions._
import io.delta.kernel.internal.fs.Path
import io.delta.kernel.internal.hook.LogCompactionHook
import io.delta.kernel.internal.replay.ActionsIterator
import io.delta.kernel.internal.util.FileNames.DeltaLogFileType
import io.delta.kernel.internal.util.Utils.singletonCloseableIterator
import io.delta.kernel.types.StructType
import io.delta.kernel.utils.FileStatus

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
  // compacted log file, and also filter out adds that have been removed
  def getActionsFromLog(
      tablePath: Path,
      engine: Engine,
      startVersion: Long,
      endVersion: Long): Seq[TestRow] = {
    val files = DeltaLogActionUtils.listDeltaLogFilesAsIter(
      engine,
      new HashSet(Arrays.asList(DeltaLogFileType.COMMIT)),
      tablePath,
      startVersion,
      Optional.of(endVersion),
      false /* mustBeRecreatable */ )
      .toInMemoryList()
    Collections.reverse(files) // we want things in reverse order
    val actions =
      new ActionsIterator(engine, files, COMPACTED_SCHEMA, Optional.empty())
    val removed = mutable.HashSet.empty[String]
    val resBuilder = Seq.newBuilder[TestRow]
    while (actions.hasNext()) {
      val batch = actions.next().getColumnarBatch()
      val rows = batch.getRows()
      while (rows.hasNext()) {
        val row = rows.next()
        if (!row.isNullAt(2)) {
          // this is a remove
          val removeRow = row.getStruct(2)
          val path = removeRow.getString(0)
          removed += path
        }
        if (!row.isNullAt(1)) {
          // this is an add
          val addRow = row.getStruct(1)
          val path = addRow.getString(0)
          if (!removed.contains(path)) {
            resBuilder += TestRow(row)
          }
        } else if (!rowIsNull(row)) {
          resBuilder += TestRow(row)
        }
      }
    }
    resBuilder.result()
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
    val resBuilder = Seq.newBuilder[Row]
    while (batches.hasNext()) {
      val batch = batches.next()
      val rows = batch.getRows()
      while (rows.hasNext()) {
        resBuilder += rows.next()
      }
    }
    resBuilder.result()
  }

  Seq(false, true).foreach { includeRemoves =>
    val testMsgUpdate = if (includeRemoves) " and removes" else ""
    test(s"commits containing adds$testMsgUpdate") {
      withTempDirAndEngine { (tablePath, engine) =>
        addData(tablePath, alternateBetweenAddsAndRemoves = includeRemoves, numberIter = 10)

        val actionsFromCommits = getActionsFromLog(new Path(tablePath), engine, 0, 9)

        val hook = new LogCompactionHook(new Path(tablePath), 0, 9, 0)
        hook.threadSafeInvoke(engine)
        val compactedPath =
          tablePath + "/_delta_log/00000000000000000000.00000000000000000009.compacted.json"
        val actionsFromCompacted = getActionsFromCompacted(compactedPath, engine)

        checkAnswer(actionsFromCompacted, actionsFromCommits)
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

        val hook = new LogCompactionHook(new Path(tablePath), 0, 9, 0)
        hook.threadSafeInvoke(engine)

        spark.conf.set(DeltaSQLConf.DELTALOG_MINOR_COMPACTION_USE_FOR_READS.key, "true")
        val withCompactionData = readUsingSpark(tablePath)

        checkAnswer(withCompactionData, withoutCompactionData)
      }
    }
  }
}
