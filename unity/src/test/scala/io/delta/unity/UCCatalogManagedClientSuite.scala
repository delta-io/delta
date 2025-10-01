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

import java.lang.{Long => JLong}
import java.net.URI
import java.util.Optional

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import io.delta.kernel.engine.Engine
import io.delta.kernel.exceptions.KernelException
import io.delta.kernel.internal.{CreateTableTransactionBuilderImpl, SnapshotImpl}
import io.delta.kernel.internal.tablefeatures.TableFeatures.{CATALOG_MANAGED_R_W_FEATURE_PREVIEW, TABLE_FEATURES_MIN_READER_VERSION, TABLE_FEATURES_MIN_WRITER_VERSION}
import io.delta.kernel.internal.util.FileNames
import io.delta.storage.commit.{Commit, GetCommitsResponse}
import io.delta.storage.commit.uccommitcoordinator.{InvalidTargetTableException, UCClient}
import io.delta.storage.commit.uccommitcoordinator.InvalidTargetTableException
import io.delta.unity.InMemoryUCClient.TableData

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.funsuite.AnyFunSuite

/** Unit tests for [[UCCatalogManagedClient]]. */
class UCCatalogManagedClientSuite extends AnyFunSuite with UCCatalogManagedTestUtils {

  import UCCatalogManagedClientSuite._

  // TODO: [delta-io/delta#5118] If UC changes CREATE semantics, update logic here.
  /**
   * When a new UC table is created, it will have Delta version 0 but the max ratified verison in
   * UC is -1. This is a special edge case.
   */
  private def createUCCatalogManagedClientForTableWithMaxRatifiedVersionNegativeOne(
      ucTableId: String = "ucTableId"): UCCatalogManagedClient = {
    val ucClient = new InMemoryUCClient("ucMetastoreId")
    val tableData = new TableData(-1, ArrayBuffer[Commit]())
    ucClient.createTableIfNotExistsOrThrow(ucTableId, tableData)
    new UCCatalogManagedClient(ucClient)
  }

  /** Helper method with reasonable defaults */
  private def loadSnapshot(
      ucCatalogManagedClient: UCCatalogManagedClient,
      engine: Engine = defaultEngine,
      ucTableId: String = "ucTableId",
      tablePath: String = "tablePath",
      versionToLoad: Optional[java.lang.Long] = emptyLongOpt,
      timestampToLoad: Optional[java.lang.Long] = emptyLongOpt): SnapshotImpl = {
    ucCatalogManagedClient.loadSnapshot(
      engine,
      ucTableId,
      tablePath,
      versionToLoad,
      timestampToLoad).asInstanceOf[SnapshotImpl]
  }

  /**
   * If present, loads the given `versionToLoad`, else loads the maxRatifiedVersion of 2.
   *
   * Also asserts that the desired `versionToLoad` is, in fact, loaded.
   */
  private def testCatalogManagedTable(
      versionToLoad: Optional[java.lang.Long] = emptyLongOpt,
      timestampToLoad: Optional[java.lang.Long] = emptyLongOpt,
      expectedVersion: Option[Long] = None): Unit = {
    require(!versionToLoad.isPresent || !timestampToLoad.isPresent)
    // If timestamp time-travel, must provide expected version
    require(!timestampToLoad.isPresent || expectedVersion.isDefined)

    // Step 1: Create the in-memory table data (ratified commits v1, v2)
    val maxRatifiedVersion = 2L
    val tablePath = getTestResourceFilePath("catalog-owned-preview")
    val ucClient = new InMemoryUCClientWithMetrics("ucMetastoreId")
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
    val tableData = new TableData(maxRatifiedVersion, ArrayBuffer(catalogCommits: _*))
    ucClient.createTableIfNotExistsOrThrow("ucTableId", tableData)

    // Step 2: Load the table using UCCatalogManagedClient
    val ucCatalogManagedClient = new UCCatalogManagedClient(ucClient)
    val snapshot = loadSnapshot(
      ucCatalogManagedClient,
      tablePath = tablePath,
      versionToLoad = versionToLoad,
      timestampToLoad = timestampToLoad)

    // Step 3: Validate
    val version = expectedVersion.getOrElse(versionToLoad.orElse(maxRatifiedVersion))
    val protocol = snapshot.getProtocol
    assert(snapshot.getVersion == version)
    assert(protocol.getMinReaderVersion == TABLE_FEATURES_MIN_READER_VERSION)
    assert(protocol.getMinWriterVersion == TABLE_FEATURES_MIN_WRITER_VERSION)
    assert(protocol.getReaderFeatures.contains(CATALOG_MANAGED_R_W_FEATURE_PREVIEW.featureName()))
    assert(protocol.getWriterFeatures.contains(CATALOG_MANAGED_R_W_FEATURE_PREVIEW.featureName()))
    assert(ucClient.getNumGetCommitCalls == 1)
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
      // engine is null
      loadSnapshot(ucCatalogManagedClient, engine = null)
    }
    assertThrows[NullPointerException] {
      // ucTableId is null
      loadSnapshot(ucCatalogManagedClient, ucTableId = null)
    }
    assertThrows[NullPointerException] {
      // tablePath is null
      loadSnapshot(ucCatalogManagedClient, tablePath = null)
    }
    assertThrows[NullPointerException] {
      // versionToLoad is null
      loadSnapshot(ucCatalogManagedClient, versionToLoad = null)
    }
    assertThrows[NullPointerException] {
      // timestampToLoad is null
      loadSnapshot(ucCatalogManagedClient, timestampToLoad = null)
    }
    assertThrows[IllegalArgumentException] {
      // version < 0
      loadSnapshot(ucCatalogManagedClient, versionToLoad = Optional.of(-1L))
    }
    assertThrows[IllegalArgumentException] {
      // timestamp < 0
      loadSnapshot(ucCatalogManagedClient, timestampToLoad = Optional.of(-1L))
    }
    assertThrows[IllegalArgumentException] {
      // cannot provide both timestamp and version
      loadSnapshot(
        ucCatalogManagedClient,
        versionToLoad = Optional.of(10L),
        timestampToLoad = Optional.of(10L))
    }
  }

  Seq(
    (emptyLongOpt, emptyLongOpt, "latest (implicitly)"),
    (javaLongOpt(0L), emptyLongOpt, "v0 (explicitly by version)"),
    (emptyLongOpt, javaLongOpt(1749830855993L), "v0 (explicitly by timestamp")).foreach {
    case (versionToLoad, timestampToLoad, description) =>
      test(s"loadTable throws when table doesn't exist in catalog -- $description") {
        val ucClient = new InMemoryUCClient("ucMetastoreId")
        val ucCatalogManagedClient = new UCCatalogManagedClient(ucClient)

        val ex = intercept[RuntimeException] {
          loadSnapshot(
            ucCatalogManagedClient,
            ucTableId = "nonExistentTableId",
            versionToLoad = versionToLoad,
            timestampToLoad = timestampToLoad)
        }
        assert(ex.getCause.isInstanceOf[InvalidTargetTableException])
      }
  }

  Seq(
    (emptyLongOpt, emptyLongOpt, "latest (implicitly)"),
    (javaLongOpt(0L), emptyLongOpt, "v0 (explicitly by version)"),
    (emptyLongOpt, javaLongOpt(1749830855993L), "v0 (explicitly by timestamp")).foreach {
    case (versionToLoad, timestampToLoad, description) =>
      test(s"table version 0 is loaded when UC maxRatifiedVersion is -1 -- $description") {
        val tablePath = getTestResourceFilePath("catalog-owned-preview")
        val ucCatalogManagedClient =
          createUCCatalogManagedClientForTableWithMaxRatifiedVersionNegativeOne()
        val snapshot = loadSnapshot(
          ucCatalogManagedClient,
          tablePath = tablePath,
          versionToLoad = versionToLoad,
          timestampToLoad = timestampToLoad)

        assert(snapshot.getVersion == 0L)
      }
  }

  test("loadTable correctly loads a UC table -- versionToLoad is empty => load latest") {
    // Since versionToLoad is empty, it asserts that the latest version (2) is loaded
    testCatalogManagedTable()
  }

  /* ---- Time-travel-by-version tests --- */
  test("loadTable correctly loads a UC table -- versionToLoad is a ratified commit (the max)") {
    testCatalogManagedTable(versionToLoad = Optional.of(2L))
  }

  test("loadTable correctly loads a UC table -- versionToLoad is a ratified commit (not the max)") {
    testCatalogManagedTable(versionToLoad = Optional.of(1L))
  }

  test("loadTable correctly loads a UC table -- versionToLoad is a published commit") {
    testCatalogManagedTable(versionToLoad = Optional.of(0L))
  }

  test("loadTable throws if version to load is greater than max ratified version") {
    val exMsg = intercept[IllegalArgumentException] {
      testCatalogManagedTable(versionToLoad = Optional.of(9L))
    }.getMessage

    assert(exMsg.contains("Cannot load table version 9 as the latest version ratified by UC is 2"))
  }

  /* ---- Time-travel-by-timestamp tests --- */
  val v0Ts = 1749830855993L // published commit
  val v1Ts = 1749830871085L // ratified staged commit
  val v2Ts = 1749830881799L // ratified staged commit

  test("loadTable correctly loads a UC table -- " +
    "timestampToLoad is exactly a ratified commit (the max)") {
    testCatalogManagedTable(timestampToLoad = Optional.of(v2Ts), expectedVersion = Some(2L))
  }

  test("loadTable correctly loads a UC table -- timestampToLoad is between ratified commits") {
    testCatalogManagedTable(timestampToLoad = Optional.of(v2Ts - 50L), expectedVersion = Some(1L))
  }

  test("loadTable correctly loads a UC table -- " +
    "timestampToLoad is exactly a ratified commit (not the max)") {
    testCatalogManagedTable(timestampToLoad = Optional.of(v1Ts), expectedVersion = Some(1L))
  }

  test("loadTable correctly loads a UC table -- " +
    "timestampToLoad is between ratified and published commits") {
    testCatalogManagedTable(timestampToLoad = Optional.of(v1Ts - 50L), expectedVersion = Some(0L))
  }

  test("loadTable correctly loads a UC table -- timestampToLoad is exactly a published commit") {
    testCatalogManagedTable(timestampToLoad = Optional.of(v0Ts), expectedVersion = Some(0L))
  }

  test("loadTable throws if timestampToLoad is before the earliest commit") {
    val exMsg = intercept[KernelException] {
      testCatalogManagedTable(timestampToLoad = Optional.of(v0Ts - 1), expectedVersion = Some(0))
    }.getMessage

    assert(exMsg.contains("The provided timestamp 1749830855992 ms (2025-06-13T16:07:35.992Z) is " +
      "before the earliest available version 0"))
  }

  test("loadTable throws if timestampToLoad is after the latest commit") {
    val exMsg = intercept[KernelException] {
      testCatalogManagedTable(timestampToLoad = Optional.of(v2Ts + 1), expectedVersion = Some(2))
    }.getMessage

    assert(exMsg.contains("The provided timestamp 1749830881800 ms (2025-06-13T16:08:01.800Z) is " +
      "after the latest available version 2"))
  }

  /* ---- end time-travel-by-timestamp tests ---- */

  test("converts UC Commit into Kernel ParsedLogData.RATIFIED_STAGED_COMMIT") {
    val ucCommit = createCommit(1)
    val hadoopFS = ucCommit.getFileStatus

    val kernelParsedDeltaData = UCCatalogManagedClient
      .getSortedKernelParsedDeltaDataFromRatifiedCommits("ucTableId", Seq(ucCommit).asJava)
      .get(0)
    val kernelFS = kernelParsedDeltaData.getFileStatus

    assert(kernelParsedDeltaData.isFile)
    assert(kernelFS.getPath == hadoopFS.getPath.toString)
    assert(kernelFS.getSize == hadoopFS.getLen)
    assert(kernelFS.getModificationTime == hadoopFS.getModificationTime)
  }

  test("sorts UC commits by version") {
    val ucCommitsUnsorted = Seq(createCommit(1), createCommit(2), createCommit(3)).asJava

    val kernelParsedLogData = UCCatalogManagedClient
      .getSortedKernelParsedDeltaDataFromRatifiedCommits("ucTableId", ucCommitsUnsorted)

    assert(kernelParsedLogData.size() == 3)
    assert(kernelParsedLogData.get(0).getVersion == 1)
    assert(kernelParsedLogData.get(1).getVersion == 2)
    assert(kernelParsedLogData.get(2).getVersion == 3)
  }

  test("creates snapshot with UCCatalogManagedCommitter") {
    val tablePath = getTestResourceFilePath("catalog-owned-preview")
    val ucCatalogManagedClient =
      createUCCatalogManagedClientForTableWithMaxRatifiedVersionNegativeOne()
    val snapshot =
      loadSnapshot(ucCatalogManagedClient, tablePath = tablePath, versionToLoad = Optional.of(0L))
    assert(snapshot.getCommitter.isInstanceOf[UCCatalogManagedCommitter])
  }

  test("buildCreateTableTransaction sets required properties and uses UC committer") {
    // ===== GIVEN =====
    val ucClient = new InMemoryUCClient("ucMetastoreId")
    val ucCatalogManagedClient = new UCCatalogManagedClient(ucClient)

    // ===== WHEN =====
    val createTableTxnBuilder = ucCatalogManagedClient
      .buildCreateTableTransaction("ucTableId", baseTestTablePath, testSchema, "test-engine")
      .withTableProperties(Map("foo" -> "bar").asJava)
      .asInstanceOf[CreateTableTransactionBuilderImpl]

    // ===== THEN =====
    val builderTableProperties = createTableTxnBuilder.getTablePropertiesOpt.get()
    assert(builderTableProperties.get("delta.feature.catalogOwned-preview") == "supported")
    assert(builderTableProperties.get("ucTableId") == "ucTableId")
    assert(builderTableProperties.get("foo") == "bar")

    val committerOpt = createTableTxnBuilder.getCommitterOpt
    assert(committerOpt.get().isInstanceOf[UCCatalogManagedCommitter])
  }
}

object UCCatalogManagedClientSuite {

  private val emptyLongOpt = Optional.empty[java.lang.Long]()

  private def javaLongOpt(value: Long): Optional[java.lang.Long] = {
    Optional.of(value)
  }

  /** Wrapper class around InMemoryUCClient that tracks number of getCommit calls made */
  class InMemoryUCClientWithMetrics(ucMetastoreId: String) extends InMemoryUCClient(ucMetastoreId) {
    private var numGetCommitsCalls: Long = 0

    override def getCommits(
        tableId: String,
        tableUri: URI,
        startVersion: Optional[JLong],
        endVersion: Optional[JLong]): GetCommitsResponse = {
      numGetCommitsCalls += 1
      super.getCommits(tableId, tableUri, startVersion, endVersion)
    }

    def getNumGetCommitCalls: Long = numGetCommitsCalls
  }
}
