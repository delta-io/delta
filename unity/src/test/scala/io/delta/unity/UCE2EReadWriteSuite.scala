/*
 * Copyright (2025) The Delta Lake Project Authors.
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

package io.delta.unity

import java.net.URI
import java.util.{Collections, Optional}

import scala.collection.JavaConverters._

import io.delta.kernel.{Operation, Snapshot, Transaction}
import io.delta.kernel.TransactionSuite.longVector
import io.delta.kernel.data.{FilteredColumnarBatch, Row}
import io.delta.kernel.defaults.engine.DefaultEngine
import io.delta.kernel.defaults.internal.data.DefaultColumnarBatch
import io.delta.kernel.defaults.utils.TestRow
import io.delta.kernel.engine.Engine
import io.delta.kernel.expressions.Literal
import io.delta.kernel.internal.{InternalScanFileUtils, SnapshotImpl}
import io.delta.kernel.internal.data.ScanStateRow
import io.delta.kernel.internal.util.Utils.toCloseableIterator
import io.delta.kernel.utils.CloseableIterable
import io.delta.storage.commit.uccommitcoordinator.UCTokenBasedRestClient

import io.unitycatalog.client.ApiClient
import io.unitycatalog.client.api.{TablesApi, TemporaryCredentialsApi}
import io.unitycatalog.client.model.{GenerateTemporaryTableCredential, TableOperation, TemporaryCredentials}
import org.apache.hadoop.conf.Configuration
import org.scalatest.funsuite.AnyFunSuite

// scalastyle:off
class UCE2EReadWriteSuite extends AnyFunSuite {
  val baseUri = "https://e2-dogfood.staging.cloud.databricks.com/"
  val token = "xxx"

  /** Creates a new Engine instance with credentials configured for the given storage location. */
  private def createEngineWithCredentials(credentials: TemporaryCredentials): Engine = {
    val conf = new Configuration()
    conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    conf.set("fs.s3a.access.key", credentials.getAwsTempCredentials.getAccessKeyId)
    conf.set("fs.s3a.secret.key", credentials.getAwsTempCredentials.getSecretAccessKey)
    conf.set("fs.s3a.session.token", credentials.getAwsTempCredentials.getSessionToken)
    conf.set("fs.s3a.path.style.access", "true")
    conf.set("fs.s3.impl.disable.cache", "true")
    conf.set("fs.s3a.impl.disable.cache", "true")
    DefaultEngine.create(conf)
  }

  private def getUcApiClient(): ApiClient = {
    val parsedUri = new URI(baseUri)
    new ApiClient()
      .setScheme(parsedUri.getScheme())
      .setHost(parsedUri.getHost())
      .setPort(parsedUri.getPort())
      .setRequestInterceptor(request => request.header("Authorization", "Bearer " + token))
  }

  private def scanAllRows(engine: Engine, snapshot: Snapshot): Seq[Row] = {
    val scan = snapshot.getScanBuilder().build()
    val scanFiles = scan.getScanFiles(engine)
    val outputRows = scala.collection.mutable.ArrayBuffer[Row]()

    try {
      scanFiles.asScala.foreach { fileColumnarBatch =>
        fileColumnarBatch.getRows.asScala.foreach { scanFileRow =>
          val fileStatus = InternalScanFileUtils.getAddFileStatus(scanFileRow)
          val physicalDataReadSchema =
            ScanStateRow.getPhysicalDataReadSchema(scan.getScanState(engine))
          val physicalDataIter = engine.getParquetHandler.readParquetFiles(
            io.delta.kernel.internal.util.Utils.singletonCloseableIterator(fileStatus),
            physicalDataReadSchema,
            Optional.empty()).map(_.getData)

          val dataBatches = io.delta.kernel.Scan.transformPhysicalData(
            engine,
            scan.getScanState(engine),
            scanFileRow,
            physicalDataIter)

          try {
            dataBatches.asScala.foreach { batch =>
              val data = batch.getData
              val selectionVector = batch.getSelectionVector
              val rowIter = data.getRows
              try {
                var i = 0
                while (rowIter.hasNext) {
                  val row = rowIter.next()
                  if (!selectionVector.isPresent || selectionVector.get.getBoolean(i)) {
                    outputRows += row
                  }
                  i += 1
                }
              } finally {
                rowIter.close()
              }
            }
          } finally {
            dataBatches.close()
          }
        }
      }
    } finally {
      scanFiles.close()
    }

    outputRows.toSeq
  }

  private def printDeltaLogContents(
      engine: Engine,
      tablePath: String,
      hadoopConf: Configuration): Unit = {
    println("========== DELTA LOG CONTENTS ==========")

    import org.apache.hadoop.fs.{FileSystem, Path}

    val deltaLogDir = s"$tablePath/_delta_log"
    val stagedCommitsDir = s"$tablePath/_delta_log/_staged_commits"

    try {

      // List main delta log directory contents using listing prefix approach
      println("_delta_log directory contents:")
      val deltaLogListingPrefix = s"$deltaLogDir/00000000000000000000"
      val deltaLogFiles = engine.getFileSystemClient().listFrom(deltaLogListingPrefix)
      try {
        var foundFiles = false
        deltaLogFiles.asScala.foreach { fileStatus =>
          val filePath = fileStatus.getPath
          if (filePath.startsWith(s"$deltaLogDir/") && !filePath.startsWith(s"$deltaLogDir/_")) {
            val fileName = filePath.substring(deltaLogDir.length + 1)
            if (!fileName.contains("/") && fileName.nonEmpty) {
              println(s"  $fileName (size: ${fileStatus.getSize}, modified: ${fileStatus.getModificationTime})")
              foundFiles = true
            }
          }
        }
        if (!foundFiles) println("  (no files)")
      } finally {
        deltaLogFiles.close()
      }

      // List staged commits directory using Hadoop FileSystem directly
      println("_staged_commits directory contents:")
      try {
        val stagedCommitsPath = new Path(stagedCommitsDir)
        val fs = stagedCommitsPath.getFileSystem(hadoopConf)

        if (fs.exists(stagedCommitsPath)) {
          val fileStatuses = fs.listStatus(stagedCommitsPath)
          if (fileStatuses.nonEmpty) {
            fileStatuses.foreach { fileStatus =>
              if (fileStatus.isFile) {
                val fileName = fileStatus.getPath.getName
                println(s"  $fileName (size: ${fileStatus.getLen}, modified: ${fileStatus.getModificationTime})")
              }
            }
          } else {
            println("  (no files)")
          }
        } else {
          println("  (directory does not exist)")
        }
      } catch {
        case e: Exception => println(s"  Error listing staged commits: ${e.getMessage}")
      }

    } catch {
      case e: Exception => println(s"Error listing delta log contents: ${e.getMessage}")
    }
    println()
  }

  private def loadLatestSnapshotAndPrint(
      engine: Engine,
      ucCatalogManagedClient: UCCatalogManagedClient,
      ucTableId: String,
      tablePath: String): Snapshot = {
    val latestSnapshot = ucCatalogManagedClient
      .loadSnapshot(engine, ucTableId, tablePath, Optional.empty())
      .asInstanceOf[SnapshotImpl]
    println(s"version: ${latestSnapshot.getVersion}")
    println(s"schema: ${latestSnapshot.getSchema}")
    println(s"protocol: ${latestSnapshot.getProtocol}")
    println(s"number of ratified commits: ${latestSnapshot.getRatifiedCommits.size()}")
    println(s"should publish: ${latestSnapshot.getStatistics.shouldPublish()}")
    scanAllRows(engine, latestSnapshot).foreach { row => println(TestRow(row)) }
    latestSnapshot
  }

  private def writeDataAndCommit(
      engine: Engine,
      snapshot: Snapshot,
      lowInc: Int,
      highInc: Int,
      txnNumber: Int,
      tablePath: String,
      hadoopConf: Configuration): Unit = {
    println(s"========== TRANSACTION $txnNumber: STARTING ==========")
    println(s"Read version: ${snapshot.getVersion}")
    println(
      s"Attempting to write data + commit: startVersion = ${snapshot.getVersion}, data = [$lowInc, $highInc]")

    val schema = snapshot.getSchema
    val txn = snapshot.buildUpdateTableTransaction("custom", Operation.WRITE).build(engine)
    val txnStateRow = txn.getTransactionState(engine)
    val col1Vector = longVector((lowInc.toLong to highInc.toLong).map(java.lang.Long.valueOf).toSeq)
    val columnarBatchData =
      new DefaultColumnarBatch(highInc - lowInc + 1, schema, Array(col1Vector))
    val filteredColumnarBatchData = new FilteredColumnarBatch(columnarBatchData, Optional.empty())
    val partitionValues = Collections.emptyMap[String, Literal]()

    val physicalDataIter = Transaction.transformLogicalData(
      engine,
      txnStateRow,
      toCloseableIterator(Seq(filteredColumnarBatchData).toIterator.asJava),
      partitionValues)

    println("Created physicalDataIter")

    val writeContext = Transaction.getWriteContext(engine, txnStateRow, partitionValues)

    val writeResultIter = engine
      .getParquetHandler
      .writeParquetFiles(
        writeContext.getTargetDirectory,
        physicalDataIter,
        writeContext.getStatisticsColumns)

    println("Created writeResultIter")

    val addRowsIter =
      Transaction.generateAppendActions(engine, txnStateRow, writeResultIter, writeContext)

    println("Created addRowsIter")

    val result = txn.commit(engine, CloseableIterable.inMemoryIterable(addRowsIter))

    println(s"========== TRANSACTION $txnNumber: COMMITTED ==========")
    println(s"Committed version: ${result.getVersion}")

    // Print post-commit snapshot information
    val postCommitSnapshot = result.getPostCommitSnapshot.asInstanceOf[SnapshotImpl]
    println(s"========== POST-COMMIT SNAPSHOT (Transaction $txnNumber) ==========")
    println(s"Post-commit snapshot version: ${postCommitSnapshot.getVersion}")
    println(s"Number of ratified commits: ${postCommitSnapshot.getRatifiedCommits.size()}")
    println(s"Should publish: ${postCommitSnapshot.getStatistics.shouldPublish()}")

    println("Commit SUCCESS!!!!")

    // Print delta log contents after the commit
    printDeltaLogContents(engine, tablePath, hadoopConf)
  }

  test("basic write") {
    // ========== Credential and Client Setup ==========
    val ucApiClient = getUcApiClient()
    val tablesApi = new TablesApi(ucApiClient)
    val tableInfo = tablesApi.getTable("scott.main.ccv2_test_rrr")
    val ucTableId = tableInfo.getTableId
    val tablePath = tableInfo.getStorageLocation
    val temporaryCredentialsApi = new TemporaryCredentialsApi(ucApiClient)
    val temporaryCredentials = temporaryCredentialsApi
      .generateTemporaryTableCredentials(
        new GenerateTemporaryTableCredential()
          .tableId(ucTableId).operation(TableOperation.READ_WRITE))
    val ucDeltaStorageClient = new UCTokenBasedRestClient(baseUri, token)
    val hadoopConf = new Configuration()
    hadoopConf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoopConf.set("fs.s3a.access.key", temporaryCredentials.getAwsTempCredentials.getAccessKeyId)
    hadoopConf.set(
      "fs.s3a.secret.key",
      temporaryCredentials.getAwsTempCredentials.getSecretAccessKey)
    hadoopConf.set(
      "fs.s3a.session.token",
      temporaryCredentials.getAwsTempCredentials.getSessionToken)
    hadoopConf.set("fs.s3a.path.style.access", "true")
    hadoopConf.set("fs.s3.impl.disable.cache", "true")
    hadoopConf.set("fs.s3a.impl.disable.cache", "true")
    val engine = DefaultEngine.create(hadoopConf)
    val ucCatalogManagedClient = new UCCatalogManagedClient(ucDeltaStorageClient)

    println("========== INITIAL READ ==========")
    var latestSnapshot =
      loadLatestSnapshotAndPrint(engine, ucCatalogManagedClient, ucTableId, tablePath)

    // ========== 30 WRITE OPERATIONS WITH BACKFILL EVERY 5 TRANSACTIONS ==========

    var currentSnapshot = latestSnapshot

    for (i <- 1 to 30) {
      val dataValue = i

      // Single row of data (lowInc == highInc)
      writeDataAndCommit(engine, currentSnapshot, dataValue, dataValue, i, tablePath, hadoopConf)
      currentSnapshot =
        loadLatestSnapshotAndPrint(engine, ucCatalogManagedClient, ucTableId, tablePath)

      // Publish every 5 transactions
      if (i % 5 == 0) {
        println(s"========== BACKFILL OPERATION (after Transaction $i) ==========")
        val publishedSnapshot = currentSnapshot.publish(engine).asInstanceOf[SnapshotImpl]
        println(s"Backfill completed. Published snapshot version: ${publishedSnapshot.getVersion}")
        println(
          s"Number of ratified commits after backfill: ${publishedSnapshot.getRatifiedCommits.size()}")
        println(
          s"Should publish after backfill: ${publishedSnapshot.getStatistics.shouldPublish()}")

        // Print delta log contents after backfill
        printDeltaLogContents(engine, tablePath, hadoopConf)

        // Update current snapshot to the published one
        currentSnapshot = publishedSnapshot
      }
    }

    println("========== FINAL SUMMARY ==========")
    println(s"Final snapshot version: ${latestSnapshot.getVersion}")
    println(s"Final number of ratified commits: ${latestSnapshot.getRatifiedCommits.size()}")
    println(s"Final should publish: ${latestSnapshot.getStatistics.shouldPublish()}")
  }
}
