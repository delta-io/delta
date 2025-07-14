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
import scala.collection.mutable.ArrayBuffer

import io.delta.kernel.Transaction
import io.delta.kernel.data.FilteredColumnarBatch
import io.delta.kernel.defaults.engine.DefaultEngine
import io.delta.kernel.defaults.internal.data.DefaultColumnarBatch
import io.delta.kernel.defaults.utils.TestUtils
import io.delta.kernel.engine.Engine
import io.delta.kernel.expressions.Literal
import io.delta.kernel.internal.files.ParsedLogData.ParsedLogType
import io.delta.kernel.internal.table.ResolvedTableInternal
import io.delta.kernel.internal.tablefeatures.TableFeatures.{CATALOG_MANAGED_R_W_FEATURE_PREVIEW, TABLE_FEATURES_MIN_READER_VERSION, TABLE_FEATURES_MIN_WRITER_VERSION}
import io.delta.kernel.internal.util.FileNames
import io.delta.storage.commit.Commit
import io.delta.kernel.internal.util.Utils.toCloseableIterator
import io.delta.kernel.test.VectorTestUtils
import io.delta.storage.commit.uccommitcoordinator.UCTokenBasedRestClient

import io.unitycatalog.client.ApiClient
import io.unitycatalog.client.api.{TablesApi, TemporaryCredentialsApi}
import io.unitycatalog.client.model.{GenerateTemporaryTableCredential, TableOperation, TemporaryCredentials}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.funsuite.AnyFunSuite

// scalastyle:off

/** Unit tests for [[UCCatalogManagedClient]]. */
class UCCatalogManagedClientSuite
    extends AnyFunSuite with TestUtils with UCCatalogManagedTestUtils with VectorTestUtils {

  /** Creates a new Engine instance with credentials configured for the given storage location. */
  def createEngineWithCredentials(credentials: TemporaryCredentials): Engine = {
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

  test("aaa") {
    val baseUri = "https://e2-dogfood.staging.cloud.databricks.com/"
    val token = "xxx"
    val parsedUri = new URI(baseUri)
    val ucApiClient = new ApiClient()
      .setScheme(parsedUri.getScheme())
      .setHost(parsedUri.getHost())
      .setPort(parsedUri.getPort())
    ucApiClient.setRequestInterceptor(request => request.header("Authorization", "Bearer " + token))
    println("created ucApiClient")
    val tablesApi = new TablesApi(ucApiClient)
    println("created tablesApi")
    val tableInfo = tablesApi.getTable("scott.main.ccv2_test_fff")
    println(s"created tableInfo $tableInfo")
    println(tableInfo.getName)
    println(tableInfo.getTableId)
    println(tableInfo.getStorageLocation)
    val ucTableId = tableInfo.getTableId
    val tablePath = tableInfo.getStorageLocation
    val temporaryCredentialsApi = new TemporaryCredentialsApi(ucApiClient)
    println("created temporaryCredentialsApi")
    val temporaryCredentials = temporaryCredentialsApi
      .generateTemporaryTableCredentials(
        new GenerateTemporaryTableCredential()
          .tableId(ucTableId).operation(TableOperation.READ_WRITE))
    println("created temporaryCredentials")
    val ucDeltaStorageClient = new UCTokenBasedRestClient(baseUri, token)
    println("created ucDeltaStorageClient")
    val engine = createEngineWithCredentials(temporaryCredentials)
    println("created engine with credentials")
    val ucCatalogManagedClient = new UCCatalogManagedClient(ucDeltaStorageClient)
    println("created ucCatalogManagedClient")

    writeAndCommit(0, 99)
    writeAndCommit(100, 199)
    writeAndCommit(200, 299)

    def writeAndCommit(lowInc: Int, highInc: Int): Unit = {
      println(s"Attempting to write data + commit: data = [$lowInc, $highInc]")
      println(s"Loading resolved table ... tableId = $ucTableId, tablePath = $tablePath")
      val resolvedTable = ucCatalogManagedClient.loadTable(engine, ucTableId, tablePath)
      println(s"Loaded resolved table at version ${resolvedTable.getVersion}")
      val schema = resolvedTable.getSchema
      println(s"Schema: $schema")

      val txn = resolvedTable.forceCreateTransaction()
      val txnStateRow = txn.getTransactionState()
      val col1Vector = longVector(lowInc.toLong to highInc.toLong: _*)
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

      val commitContext = txn.getInitialCommitContext(engine, addRowsIter)

      println("Created commitContext")

      resolvedTable.getCommitter.commit(
        engine,
        commitContext.getFinalizedActions,
        commitContext.getCommitMetadata)

      println("Commit SUCCESS!!!!")
    }
  }

  private def testCatalogManagedTable(versionToLoad: Long): Unit = {
    // Step 1: Create the in-memory table data (ratified commits v1, v2)
    val tablePath = getTestResourceFilePath("catalog-owned-preview")
    val ucClient = new InMemoryUCClient("ucMetastoreId")
    val fs = FileSystem.get(new Configuration())
    val catalogCommits = Seq(
      // scalastyle:off line.size.limit
      getTestResourceFilePath("catalog-owned-preview/_delta_log/_staged_commits/00000000000000000001.4cb9708e-b478-44de-b203-53f9ba9b2876.json"),
      getTestResourceFilePath("catalog-owned-preview/_delta_log/_staged_commits/00000000000000000002.5b9bba4a-0085-430d-a65e-b0d38c1afbe9.json"))
        // scalastyle:on line.size.limit
        .map { path => fs.getFileStatus(new Path(path)) }
        .map { fileStatus =>
          new Commit(
            FileNames.deltaVersion(fileStatus.getPath.toString),
            fileStatus,
            fileStatus.getModificationTime)
        }
    val tableData = new InMemoryUCClient.TableData(2, ArrayBuffer(catalogCommits: _*))
    ucClient.createTableIfNotExistsOrThrow("ucTableId", tableData)

    // Step 2: Load the table using UCCatalogManagedClient at the desired versionToLoad
    val ucCatalogManagedClient = new UCCatalogManagedClient(ucClient)
    val resolvedTable = ucCatalogManagedClient
        .loadTable(defaultEngine, "ucTableId", tablePath, versionToLoad)
        .asInstanceOf[ResolvedTableInternal]

    // Step 3: Validate
    val protocol = resolvedTable.getProtocol
    assert(resolvedTable.getVersion == versionToLoad)
    assert(protocol.getMinReaderVersion == TABLE_FEATURES_MIN_READER_VERSION)
    assert(protocol.getMinWriterVersion == TABLE_FEATURES_MIN_WRITER_VERSION)
    assert(protocol.getReaderFeatures.contains(CATALOG_MANAGED_R_W_FEATURE_PREVIEW.featureName()))
    assert(protocol.getWriterFeatures.contains(CATALOG_MANAGED_R_W_FEATURE_PREVIEW.featureName()))
  }

  test("constructor throws on invalid input") {
    assertThrows[NullPointerException] {
      new UCCatalogManagedClient(null)
    }
  }

  test("loadTable throws on invalid input") {
    val ucClient = new InMemoryUCClient("ucMetastoreId")
    val ucCatalogManagedClient = new UCCatalogManagedClient(ucClient)

    assertThrows[NullPointerException] {
      ucCatalogManagedClient.loadTable(null, "ucTableId", "tablePath", 0L) // engine is null
    }
    assertThrows[NullPointerException] {
      ucCatalogManagedClient.loadTable(defaultEngine, null, "tablePath", 0L) // ucTableId is null
    }
    assertThrows[NullPointerException] {
      ucCatalogManagedClient.loadTable(defaultEngine, "ucTableId", null, 0L) // tablePath is null
    }
    assertThrows[IllegalArgumentException] {
      ucCatalogManagedClient.loadTable(defaultEngine, "ucTableId", "tablePath", -1L) // version < 0
    }
  }

  test("loadTable throws if version to load is greater than max ratified version") {
    val exMsg = intercept[IllegalArgumentException] {
      testCatalogManagedTable(versionToLoad = 9L)
    }.getMessage

    assert(exMsg.contains("Cannot load table version 9 as the latest version ratified by UC is 2"))
  }

  test("loadTable correctly loads a UC table -- versionToLoad is a ratified commit (the max)") {
    testCatalogManagedTable(versionToLoad = 2L)
  }

  test("loadTable correctly loads a UC table -- versionToLoad is a ratified commit (not the max)") {
    testCatalogManagedTable(versionToLoad = 1L)
  }

  test("loadTable correctly loads a UC table -- versionToLoad is a published commit") {
    testCatalogManagedTable(versionToLoad = 0L)
  }

  test("converts UC Commit into Kernel ParsedLogData.RATIFIED_STAGED_COMMIT") {
    val ucCommit = createCommit(1)
    val hadoopFS = ucCommit.getFileStatus

    val kernelParsedLogData = UCCatalogManagedClient
      .getSortedKernelLogDataFromRatifiedCommits("ucTableId", Seq(ucCommit).asJava)
      .get(0)
    val kernelFS = kernelParsedLogData.getFileStatus

    assert(kernelParsedLogData.`type` == ParsedLogType.RATIFIED_STAGED_COMMIT)
    assert(kernelFS.getPath == hadoopFS.getPath.toString)
    assert(kernelFS.getSize == hadoopFS.getLen)
    assert(kernelFS.getModificationTime == hadoopFS.getModificationTime)
  }

  test("sorts UC commits by version") {
    val ucCommitsUnsorted = Seq(createCommit(1), createCommit(2), createCommit(3)).asJava

    val kernelParsedLogData = UCCatalogManagedClient
      .getSortedKernelLogDataFromRatifiedCommits("ucTableId", ucCommitsUnsorted)

    assert(kernelParsedLogData.size() == 3)
    assert(kernelParsedLogData.get(0).version == 1)
    assert(kernelParsedLogData.get(1).version == 2)
    assert(kernelParsedLogData.get(2).version == 3)
  }

}
