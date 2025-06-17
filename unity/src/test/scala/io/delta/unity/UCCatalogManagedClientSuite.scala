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

import scala.collection.JavaConverters._

import io.delta.kernel.internal.files.ParsedLogData.ParsedLogType
import io.delta.kernel.internal.util.FileNames
import io.delta.storage.commit.Commit

import org.apache.hadoop.fs.{FileStatus => HadoopFileStatus, Path}
import org.scalatest.funsuite.AnyFunSuite

/** Unit tests for [[UCCatalogManagedClient]]. */
class UCCatalogManagedClientSuite extends AnyFunSuite {

  def hadoopCommitFileStatus(version: Int): HadoopFileStatus = {
    val filePath = FileNames.stagedCommitFile("fake/logPath", version)

    new HadoopFileStatus(
      version, /* length */
      false, /* isDir */
      version, /* blockReplication */
      version, /* blockSize */
      version, /* modificationTime */
      new Path(filePath))
  }

  test("converts UC Commit into Kernel ParsedLogData.RATIFIED_STAGED_COMMIT") {
    val hadoopFS = hadoopCommitFileStatus(1)
    val ucCommit = new Commit(1 /* version */, hadoopFS, 1 /* commitTimestamp */ )

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
    val ucCommit1 = new Commit(1 /* version */, hadoopCommitFileStatus(1), 1)
    val ucCommit2 = new Commit(2 /* version */, hadoopCommitFileStatus(2), 2)
    val ucCommit3 = new Commit(3 /* version */, hadoopCommitFileStatus(3), 3)

    val ucCommitsUnsorted = Seq(ucCommit2, ucCommit3, ucCommit1).asJava

    val kernelParsedLogData = UCCatalogManagedClient
      .getSortedKernelLogDataFromRatifiedCommits("ucTableId", ucCommitsUnsorted)

    assert(kernelParsedLogData.size() == 3)
    assert(kernelParsedLogData.get(0).version == 1)
    assert(kernelParsedLogData.get(1).version == 2)
    assert(kernelParsedLogData.get(2).version == 3)
  }

}
