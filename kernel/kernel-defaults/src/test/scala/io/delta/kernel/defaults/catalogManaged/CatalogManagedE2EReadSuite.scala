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

package io.delta.kernel.defaults.catalogManaged

import scala.collection.JavaConverters._

import io.delta.kernel.TableManager
import io.delta.kernel.defaults.utils.{TestRow, TestUtilsWithTableManagerAPIs}
import io.delta.kernel.internal.files.ParsedLogData
import io.delta.kernel.internal.table.ResolvedTableBuilderImpl
import io.delta.kernel.internal.tablefeatures.TableFeatures.{CATALOG_MANAGED_R_W_FEATURE_PREVIEW, TABLE_FEATURES_MIN_READER_VERSION, TABLE_FEATURES_MIN_WRITER_VERSION}
import io.delta.kernel.utils.FileStatus

import org.scalatest.funsuite.AnyFunSuite

/**
 * Test suite for end-to-end reads of catalog-managed Delta tables.
 *
 * The goal of this suite is to simulate how a real "Catalog-Managed-Client" would read a
 * catalog-managed Delta table, without introducing a full, or even partial (e.g. in-memory)
 * catalog client implementation.
 *
 * The catalog boundary is simulated by tests manually providing [[ParsedLogData]]. For example,
 * there can be X commits in the _staged_commits directory, and a given test can decide that Y
 * commits (subset of X) are in fact "ratified". The test can then turn those commits into
 * [[ParsedLogData]] and inject them into the [[io.delta.kernel.ResolvedTableBuilder]]. This is,
 * in essence, doing exactly what we would expect a "Catalog-Managed-Client" to do.
 */
class CatalogManagedE2EReadSuite extends AnyFunSuite with TestUtilsWithTableManagerAPIs {

  test("simple e2e read of catalogOwned-preview table with staged ratified commits") {
    val tablePath = getTestResourceFilePath("catalog-owned-preview")

    // Note: We need to *resolve* each test resource file path, because the table root file path
    //       will itself be resolved when we create a ResolvedTable. If we resolved some paths but
    //       not others, we would get an error like `File <commit-file> doesn't belong in the
    //       transaction log at <log-path>`.

    val parsedLogData = Seq(
      // scalastyle:off line.size.limit
      getTestResourceFilePath("catalog-owned-preview/_delta_log/_staged_commits/00000000000000000001.4cb9708e-b478-44de-b203-53f9ba9b2876.json"),
      getTestResourceFilePath("catalog-owned-preview/_delta_log/_staged_commits/00000000000000000002.5b9bba4a-0085-430d-a65e-b0d38c1afbe9.json"))
      // scalastyle:on line.size.limit
      .map { path => defaultEngine.getFileSystemClient.resolvePath(path) }
      .map { p => FileStatus.of(p) }
      .map { fs => ParsedLogData.forFileStatus(fs) }
      .toList
      .asJava

    val resolvedTable = TableManager
      .loadTable(tablePath)
      .asInstanceOf[ResolvedTableBuilderImpl]
      .atVersion(2)
      .withLogData(parsedLogData)
      .build(defaultEngine)

    assert(resolvedTable.getVersion === 2)
    assert(resolvedTable.getLogSegment.getDeltas.size() === 3)

    val protocol = resolvedTable.getProtocol
    assert(protocol.getMinReaderVersion == TABLE_FEATURES_MIN_READER_VERSION)
    assert(protocol.getMinWriterVersion == TABLE_FEATURES_MIN_WRITER_VERSION)
    assert(protocol.getReaderFeatures.contains(CATALOG_MANAGED_R_W_FEATURE_PREVIEW.featureName()))
    assert(protocol.getWriterFeatures.contains(CATALOG_MANAGED_R_W_FEATURE_PREVIEW.featureName()))

    val actualResult = readSnapshot(resolvedTable)
    val expectedResult = (0 to 199).map { x => TestRow(x / 100, x) }
    checkAnswer(actualResult, expectedResult)
  }
}
