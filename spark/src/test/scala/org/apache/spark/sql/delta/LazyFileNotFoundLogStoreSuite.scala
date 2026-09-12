/*
 * Copyright (2021) The Delta Lake Project Authors.
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

package org.apache.spark.sql.delta

import java.io.{FileNotFoundException, IOException, Reader, UncheckedIOException}

import org.apache.spark.sql.delta.storage.LocalLogStore
import org.apache.spark.sql.delta.storage.LogStore.logStoreClassConfKey
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.util.DeltaFileOperations
import io.delta.storage.LineCloseableIterator
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}

import org.apache.spark.SparkConf
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession

/**
 * A [[Reader]] that always throws [[FileNotFoundException]] on the first read, imitating Hadoop
 * connectors (e.g. GCS) that open input streams lazily and only surface a missing path when the
 * stream is first read rather than when it is opened.
 */
private class FileNotFoundReader(message: String) extends Reader {
  override def read(cbuf: Array[Char], off: Int, len: Int): Int =
    throw new FileNotFoundException(message)
  override def close(): Unit = {}
}

/**
 * A [[LocalLogStore]] that reproduces the behavior of a lazy-listing filesystem such as GCS: a
 * missing path is surfaced as a `FileNotFoundException` wrapped in an [[UncheckedIOException]]
 * instead of a bare `FileNotFoundException`. `read` routes through the real
 * [[LineCloseableIterator]] so the wrapping happens exactly as it does in production; `listFrom`
 * throws the wrapped form directly.
 */
class LazyFileNotFoundLogStore(sparkConf: SparkConf, defaultHadoopConf: Configuration)
  extends LocalLogStore(sparkConf, defaultHadoopConf) {

  private def exists(path: Path, hadoopConf: Configuration): Boolean = {
    val fs = path.getFileSystem(hadoopConf)
    fs.exists(path)
  }

  override def read(path: Path, hadoopConf: Configuration): Seq[String] = {
    if (!exists(path, hadoopConf)) {
      // Drive the production iterator to force the same UncheckedIOException(FileNotFoundException)
      // that a lazily-opened GCS stream produces on the first read.
      val iter = new LineCloseableIterator(
        new FileNotFoundReader(s"Item not found: '$path'."))
      try {
        iter.hasNext
      } finally {
        iter.close()
      }
    }
    super.read(path, hadoopConf)
  }

  override def listFrom(path: Path, hadoopConf: Configuration): Iterator[FileStatus] = {
    if (!exists(path.getParent, hadoopConf)) {
      throw new UncheckedIOException(
        new FileNotFoundException(s"Item not found: '${path.getParent}'."))
    }
    super.listFrom(path, hadoopConf)
  }
}

/**
 * Regression coverage for writes to brand-new Delta paths on lazy-listing filesystems (e.g. GCS on
 * Dataproc). The missing `_last_checkpoint` / `_delta_log` of a new table surfaces as a
 * `FileNotFoundException` wrapped in an [[UncheckedIOException]], which must be treated the same as
 * a bare `FileNotFoundException` when probing a not-yet-existing table.
 */
class LazyFileNotFoundLogStoreSuite extends QueryTest
  with SharedSparkSession
  with DeltaSQLCommandTest {

  override def sparkConf: SparkConf = {
    super.sparkConf.set(logStoreClassConfKey, classOf[LazyFileNotFoundLogStore].getName)
  }

  test("write to a brand-new Delta path succeeds when a missing path surfaces as " +
      "UncheckedIOException(FileNotFoundException)") {
    withTempDir { dir =>
      val path = new Path(dir.getCanonicalPath, "new_table").toString
      spark.range(10).write.format("delta").mode("overwrite").save(path)
      checkAnswer(spark.read.format("delta").load(path), spark.range(10).toDF())
    }
  }

  test("isFileNotFoundException recognizes bare and UncheckedIOException-wrapped forms") {
    val bare = new FileNotFoundException("missing")
    assert(DeltaFileOperations.isFileNotFoundException(bare))
    assert(DeltaFileOperations.isFileNotFoundException(new UncheckedIOException(bare)))
    // A non-FileNotFound IOException wrapped in UncheckedIOException is not treated as missing.
    assert(!DeltaFileOperations.isFileNotFoundException(
      new UncheckedIOException(new IOException("boom"))))
    assert(!DeltaFileOperations.isFileNotFoundException(new RuntimeException("unrelated")))
  }
}
