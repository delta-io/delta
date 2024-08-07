/*
 * Copyright (2024) The Delta Lake Project Authors.
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

package io.delta.kernel.defaults.internal.coordinatedcommits

import io.delta.kernel.defaults.DeltaTableWriteSuiteBase
import io.delta.kernel.internal.TableConfig._
import io.delta.kernel.{Snapshot, Table}
import io.delta.kernel.data.{FilteredColumnarBatch, Row}
import io.delta.kernel.defaults.engine.DefaultCommitCoordinatorClientHandler
import io.delta.kernel.defaults.utils.TestRow
import io.delta.kernel.engine.Engine
import io.delta.kernel.exceptions.ConcurrentWriteException
import io.delta.kernel.internal.{SnapshotImpl, TableConfig, TableImpl}
import io.delta.kernel.internal.actions.{CommitInfo, Metadata, Protocol, SingleAction}
import io.delta.kernel.internal.actions.SingleAction.{createMetadataSingleAction, FULL_SCHEMA}
import io.delta.kernel.internal.fs.{Path => KernelPath}
import io.delta.kernel.internal.snapshot.{SnapshotManager, TableCommitCoordinatorClientHandler}
import io.delta.kernel.internal.util.{CoordinatedCommitsUtils, FileNames, ManualClock}
import io.delta.kernel.internal.util.Preconditions.checkArgument
import io.delta.kernel.internal.util.Utils.{closeCloseables, singletonCloseableIterator, toCloseableIterator}
import io.delta.kernel.utils.{CloseableIterator, FileStatus}
import io.delta.storage.commit.{CommitCoordinatorClient, CommitResponse, GetCommitsResponse, InMemoryCommitCoordinator, UpdatedActions, CoordinatedCommitsUtils => CCU}
import io.delta.storage.commit.actions.{AbstractMetadata, AbstractProtocol}
import io.delta.storage.LogStore
import org.apache.hadoop.conf.Configuration

import java.{lang, util}
import io.delta.kernel.expressions.Literal
import io.delta.kernel.internal.fs.Path
import io.delta.kernel.utils.CloseableIterable.emptyIterable
import org.apache.hadoop.fs.{Path => HadoopPath}

import java.io.File
import java.util.{Collections, Optional}
import scala.collection.convert.ImplicitConversions.{`collection AsScalaIterable`, `iterator asScala`}
import scala.collection.JavaConverters._
import scala.math
import scala.collection.immutable.Seq

class Demo extends DeltaTableWriteSuiteBase with CoordinatedCommitsTestUtils {
  def testWithCoordinatorCommits(
    hadoopConf: Map[String, String] = Map.empty, f: (String, Engine) => Unit): Unit = {
    InMemoryCommitCoordinatorBuilder.clearInMemoryInstances()
    withTempDirAndEngine(f, hadoopConf)
  }

  def displayTableSnapshot(result: Seq[Row], v: Int): Unit = {
    printf("\n")
    printf(s"Table Snapshot (v$v):\n")
    printf(s"---------------------\n")
    result.takeRight(2).foreach { row =>
      printf(s"${row.getInt(0)}\n")
    }
    result.dropRight(2).foreach { row =>
      printf(s"${row.getInt(0)}\n")
    }
    printf("\n")
  }

  test("dynamodb with spark table") {
    val config = Map(
      CommitCoordinatorProvider.getCommitCoordinatorNameConfKey("dynamodb") ->
        classOf[DynamoDBCommitCoordinatorClientBuilder].getName,
      "fs.s3a.access.key" -> "your-access-key",
      "fs.s3a.secret.key" -> "your-secret-key",
    )
    testWithCoordinatorCommits(config, {
      (_, engine) =>

        val tablePath = "s3a://delta-kernel-coordinated-commits/demotable16"

        /* 1. Load demo table created by Delta-Spark Engine using Kernel-API. */
        val table = Table.forPath(engine, tablePath)
        val snapshot1 = table.getLatestSnapshot(engine).asInstanceOf[SnapshotImpl]

        /* 2. Assert the demo table is managed by DynamoDB commit coordinator. */
        val metadata = snapshot1.getMetadata
        checkArgument(
          TableConfig.COORDINATED_COMMITS_COORDINATOR_NAME.fromMetadata(engine, metadata) ===
            Optional.of("dynamodb"))

        /*
         * 3. Read data from the demo table using Kernel-API and we can see Delta-Kernel now
         * supports coordinated commit read.
         */
        printf("Reading data from the demo table created by Delta-Spark Engine:\n")
        val result1 = readSnapshot(snapshot1, snapshot1.getSchema(engine), null, null, engine)
        displayTableSnapshot(result1.toList, 1)

        val schema = snapshot1.getSchema(engine)

        /*
         * 4. Append data to the demo table using Kernel-API and we can see Delta-Kernel now
         * supports coordinated commit write.
         */
        printf("Appending (2) to the demo table with Delta-Kernel Engine:\n")
        appendData(
          engine,
          tablePath,
          isNewTable = false,
          schema,
          data = Seq(Map.empty[String, Literal] -> generateDefinedData(schema, Seq.range(2, 3))))

        val snapshot2 = table.getSnapshotAsOfVersion(engine, 2L)
        val result2 = readSnapshot(snapshot2, snapshot2.getSchema(engine), null, null, engine)
        displayTableSnapshot(result2.toList, 2)

        printf("Appending (3, 4) to the demo table with Delta-Kernel Engine:\n")
        appendData(
          engine,
          tablePath,
          isNewTable = false,
          schema,
          data = Seq(Map.empty[String, Literal] -> generateDefinedData(schema, Seq.range(3, 5))))

        val snapshot3 = table.getSnapshotAsOfVersion(engine, 3L)
        val result3 = readSnapshot(snapshot3, snapshot3.getSchema(engine), null, null, engine)
        displayTableSnapshot(result3.toList, 3)

        printf("Appending (5, 6, 7) to the demo table with Delta-Kernel Engine:\n")
        appendData(
          engine,
          tablePath,
          isNewTable = false,
          schema,
          data = Seq(Map.empty[String, Literal] -> generateDefinedData(schema, Seq.range(5, 8))))

        val snapshot4 = table.getLatestSnapshot(engine)
        val result4 = readSnapshot(snapshot4, snapshot4.getSchema(engine), null, null, engine)
        displayTableSnapshot(result4.toList, 4)
    })
  }
}
