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
import java.util.Optional

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import io.delta.kernel.internal.{CreateTableTransactionBuilderImpl, SnapshotImpl}
import io.delta.kernel.internal.tablefeatures.TableFeatures.{CATALOG_MANAGED_R_W_FEATURE_PREVIEW, TABLE_FEATURES_MIN_READER_VERSION, TABLE_FEATURES_MIN_WRITER_VERSION}
import io.delta.kernel.internal.util.FileNames
import io.delta.storage.commit.Commit
import io.delta.storage.commit.uccommitcoordinator.InvalidTargetTableException
import io.delta.unity.InMemoryUCClient.TableData

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.funsuite.AnyFunSuite

/** Unit tests for [[UCCatalogManagedClient]]. */
class UCCatalogManagedClientSuite extends AnyFunSuite with UCCatalogManagedTestUtils {

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

  /**
   * If present, loads the given `versionToLoad`, else loads the maxRatifiedVersion of 2.
   *
   * Also asserts that the desired `versionToLoad` is, in fact, loaded.
   */
  private def testCatalogManagedTable(versionToLoad: Optional[java.lang.Long]): Unit = {
    // Step 1: Create the in-memory table data (ratified commits v1, v2)
    val maxRatifiedVersion = 2L
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
    val tableData = new TableData(maxRatifiedVersion, ArrayBuffer(catalogCommits: _*))
    ucClient.createTableIfNotExistsOrThrow("ucTableId", tableData)

    // Step 2: Load the table using UCCatalogManagedClient at the desired versionToLoad
    val ucCatalogManagedClient = new UCCatalogManagedClient(ucClient)
    val snapshot = ucCatalogManagedClient
      .loadSnapshot(defaultEngine, "ucTableId", tablePath, versionToLoad)
      .asInstanceOf[SnapshotImpl]

    // Step 3: Validate
    val protocol = snapshot.getProtocol
    assert(snapshot.getVersion == versionToLoad.orElse(maxRatifiedVersion))
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
      // engine is null
      ucCatalogManagedClient.loadSnapshot(null, "ucTableId", "tablePath", Optional.of(0L))
    }
    assertThrows[NullPointerException] {
      // ucTableId is null
      ucCatalogManagedClient.loadSnapshot(defaultEngine, null, "tablePath", Optional.of(0L))
    }
    assertThrows[NullPointerException] {
      // tablePath is null
      ucCatalogManagedClient.loadSnapshot(defaultEngine, "ucTableId", null, Optional.of(0L))
    }
    assertThrows[IllegalArgumentException] {
      // version < 0
      ucCatalogManagedClient.loadSnapshot(defaultEngine, "ucTableId", "tablePath", Optional.of(-1L))
    }
  }

  Seq(
    (Optional.empty[java.lang.Long](), "latest (implicitly)"),
    (Optional.of(JLong.valueOf(0L)), "v0 (explicitly)")).foreach {
    case (versionToLoad, description) =>
      test(s"loadTable throws when table doesn't exist in catalog -- $description") {
        val ucClient = new InMemoryUCClient("ucMetastoreId")
        val ucCatalogManagedClient = new UCCatalogManagedClient(ucClient)

        val ex = intercept[RuntimeException] {
          ucCatalogManagedClient
            .loadSnapshot(defaultEngine, "nonExistentTableId", "tablePath", versionToLoad)
        }
        assert(ex.getCause.isInstanceOf[InvalidTargetTableException])
      }
  }

  Seq(
    (Optional.empty[java.lang.Long](), "latest (implicitly)"),
    (Optional.of(JLong.valueOf(0L)), "v0 (explicitly)")).foreach {
    case (versionToLoad, description) =>
      test(s"table version 0 is loaded when UC maxRatifiedVersion is -1 -- $description") {
        val tablePath = getTestResourceFilePath("catalog-owned-preview")
        val ucCatalogManagedClient =
          createUCCatalogManagedClientForTableWithMaxRatifiedVersionNegativeOne()
        val snapshot = ucCatalogManagedClient
          .loadSnapshot(defaultEngine, "ucTableId", tablePath, versionToLoad)

        assert(snapshot.getVersion == 0L)
      }
  }

  test("loadTable throws if version to load is greater than max ratified version") {
    val exMsg = intercept[IllegalArgumentException] {
      testCatalogManagedTable(versionToLoad = Optional.of(9L))
    }.getMessage

    assert(exMsg.contains("Cannot load table version 9 as the latest version ratified by UC is 2"))
  }

  test("loadTable correctly loads a UC table -- versionToLoad is empty => load latest") {
    // Since versionToLoad is empty, it asserts that the latest version (2) is loaded
    testCatalogManagedTable(versionToLoad = Optional.empty())
  }

  test("loadTable correctly loads a UC table -- versionToLoad is a ratified commit (the max)") {
    testCatalogManagedTable(versionToLoad = Optional.of(2L))
  }

  test("loadTable correctly loads a UC table -- versionToLoad is a ratified commit (not the max)") {
    testCatalogManagedTable(versionToLoad = Optional.of(1L))
  }

  test("loadTable correctly loads a UC table -- versionToLoad is a published commit") {
    testCatalogManagedTable(versionToLoad = Optional.of(0L))
  }

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
    val snapshot = ucCatalogManagedClient
      .loadSnapshot(defaultEngine, "ucTableId", tablePath, Optional.of(0L))
      .asInstanceOf[SnapshotImpl]
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
