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
import io.delta.kernel.defaults.utils.TestUtils
import io.delta.kernel.internal.files.ParsedLogData
import io.delta.kernel.internal.table.ResolvedTableInternal
import io.delta.kernel.utils.FileStatus
import org.scalatest.funsuite.AnyFunSuite

class CatalogManagedE2ESuite extends AnyFunSuite with TestUtils {
  test("simple e2e read of catalogOwned-preview table with staged ratified commits") {
    val tablePath = getTestResourceFilePath("catalog-owned-preview")
    val parsedLogData = Seq(
      // scalastyle:off line.size.limit
      getTestResourceFilePath("catalog-owned-preview/_delta_log/_staged_commits/00000000000000000001.4cb9708e-b478-44de-b203-53f9ba9b2876.json"),
      getTestResourceFilePath("catalog-owned-preview/_delta_log/_staged_commits/00000000000000000002.5b9bba4a-0085-430d-a65e-b0d38c1afbe9.json"))
        .map { p => s"file:$p" }
//      .map { p => new URI(p) }
      // scalastyle:on line.size.limit
      .map { p => FileStatus.of(p) }
      .map { fs => ParsedLogData.forFileStatus(fs) }
      .toList
      .asJava

    val resolvedTable = TableManager
      .loadTable(tablePath)
      .atVersion(2)
      .withLogData(parsedLogData)
      .build(defaultEngine)
      .asInstanceOf[ResolvedTableInternal]

    val protocol = resolvedTable.getProtocol
    val metadata = resolvedTable.getMetadata
  }
}
