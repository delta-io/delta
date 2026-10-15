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

package org.apache.spark.sql.delta.storage.dv

import java.io.{IOException, OutputStream}
import java.net.URI
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import org.apache.spark.sql.delta.{DeletionVectorsTestUtils, DeltaLog}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.storage.LocalLogStore
import org.apache.spark.sql.delta.test.{DeltaSQLCommandTest, DeltaSQLTestUtils}
import org.apache.hadoop.fs.{FSDataOutputStream, Path, RawLocalFileSystem}
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.util.Progressable

import org.apache.spark.internal.Logging
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession

/**
 * A deletion vector's `close()` finalises its upload; if that close fails the `.bin` is never
 * written. Closing the DV writer via `closeQuietly` swallows such a failure, so the DML commits a
 * descriptor for a missing file and the table becomes permanently unreadable. Each test asserts
 * the contract: an injected DV close failure must abort the DML and leave the table intact (version
 * not advanced, all rows readable), never commit a dangling DV.
 *
 * [[DvCloseFailingFileSystem]] discards the bytes and throws from `close()` on DV paths, so the
 * file is missing for the same reason as in production. The fix is on by default and gated by a
 * kill-switch flag (`DELETION_VECTOR_PROPAGATE_CLOSE_FAILURE`) that restores the old behavior.
 *
 * DELETE, UPDATE and MERGE all write deletion vectors through the same writer, so all three are
 * covered. The contract is asserted the same way for each: the commands differ in whether they
 * abort or read the DV back before committing, but none may leave a committed, unreadable table.
 */
class DeletionVectorWriteDurabilitySuite extends QueryTest
    with SharedSparkSession
    with DeltaSQLTestUtils
    with DeltaSQLCommandTest
    with DeletionVectorsTestUtils {

  import DeletionVectorWriteDurabilitySuite._

  // Log stores resolve per scheme from the SparkConf, so unlike fs.*.impl this cannot go through
  // withSQLConf.
  override protected def sparkConf =
    super.sparkConf
      .set(s"spark.delta.logStore.$failingScheme.impl", classOf[LocalLogStore].getName)

  private val fileSystemConf = Seq(
    s"fs.$failingScheme.impl" -> classOf[DvCloseFailingFileSystem].getName,
    // Otherwise Hadoop caches one instance across tests and the failure toggle leaks.
    s"fs.$failingScheme.impl.disable.cache" -> "true")

  /** The scheme has to be registered on the Hadoop conf used to resolve the filesystem. */
  private def hadoopConfWithFailingFs = {
    // scalastyle:off deltahadoopconfiguration
    val conf = spark.sessionState.newHadoopConf()
    // scalastyle:on deltahadoopconfiguration
    fileSystemConf.foreach { case (k, v) => conf.set(k, v) }
    conf
  }

  private def withDvClosesFailing[T](body: => T): T = {
    DvCloseFailingFileSystem.failDvClose.set(true)
    try body finally DvCloseFailingFileSystem.failDvClose.set(false)
  }

  private def withDataFileClosesFailing[T](body: => T): T = {
    DvCloseFailingFileSystem.failDataFileClose.set(true)
    try body finally DvCloseFailingFileSystem.failDataFileClose.set(false)
  }

  /** Runs `body` and returns how many close failures were injected while it ran. */
  private def countingInjections[T](body: => T): (T, Int) = {
    DvCloseFailingFileSystem.injectionCount.set(0)
    val result = body
    (result, DvCloseFailingFileSystem.injectionCount.get())
  }

  /**
   * Runs `dml` on a fresh 50-row table with DV closes failing (tolerating either outcome), then
   * asserts the contract: version not advanced and all 50 rows still readable. `dml` must touch
   * only part of a file (so a DV is written, not a whole-file removal); `extraConf` pins a code
   * path such as the MERGE executor.
   */
  private def assertDmlAbortsCleanlyOnDvCloseFailure(
      extraConf: Seq[(String, String)] = Nil)(dml: String => Unit): Unit = {
    withTempDir { dir =>
      withSQLConf(fileSystemConf ++ extraConf: _*) {
        withDeletionVectorsEnabled() {
          val tablePath = s"$failingScheme://${dir.getCanonicalPath}/tbl"
          spark.range(end = 50).toDF("id").write.format("delta").save(tablePath)
          val versionBefore = deltaLogVersion(tablePath)

          val (_, injections) = countingInjections {
            withDvClosesFailing {
              try dml(tablePath) catch { case _: Exception => }
            }
          }
          assert(injections > 0, "no DV close failure was injected, so this proves nothing")

          // Whether the DML threw or not, the table must be readable and unchanged: a swallowed
          // close that committed a dangling DV would fail this read or drop rows.
          val rows = spark.read.format("delta").load(tablePath).collect()
          assert(rows.length === 50, s"expected the original 50 rows, got ${rows.length}")
          assert(deltaLogVersion(tablePath) === versionBefore,
            "a DML that failed to durably write its DV must not advance the table version")
        }
      }
    }
  }

  /**
   * Guards the experiment. `closeQuietly` discards the injected exception without a trace, so if
   * the filesystem stopped failing the reproduction would pass for the wrong reason.
   */
  test("harness: a failed DV close creates no object, and other writes are unaffected") {
    withTempDir { dir =>
      withSQLConf(fileSystemConf: _*) {
        val base = s"$failingScheme://${dir.getCanonicalPath}"
        val fs = new Path(base).getFileSystem(hadoopConfWithFailingFs)

        withDvClosesFailing {
          val dv = new Path(s"$base/deletion_vector_harness.bin")
          val out = fs.create(dv, true)
          out.write(Array[Byte](1, 2, 3))
          // The object only ever appears on a successful finalize, so it must be absent both
          // before and after the failure, never partially visible.
          assert(!fs.exists(dv), "an unfinalised upload must not be visible")
          val error = intercept[IOException](out.close())
          assert(error.getMessage.contains(injectedFailureMessage), s"unexpected error: $error")
          assert(!fs.exists(dv), "a failed finalize must leave no object behind")

          // Data and _delta_log writes must keep working, otherwise the DML would fail for an
          // unrelated reason and the reproduction would prove nothing.
          val data = new Path(s"$base/part-00000.parquet")
          val dataOut = fs.create(data, true)
          dataOut.write(Array[Byte](1))
          dataOut.close()
          assert(fs.exists(data), "non-DV writes must be unaffected")
        }
      }
    }
  }

  test("a DELETE whose deletion vector was never written does not corrupt the table") {
    assertDmlAbortsCleanlyOnDvCloseFailure() { tablePath =>
      sql(s"DELETE FROM delta.`$tablePath` WHERE id IN (1, 5, 9)")
    }
  }


  /** A matched-DELETE MERGE that writes a DV for the partially matched file. */
  private def mergeMatchedDelete(tablePath: String): Unit = {
    spark.range(start = 1, end = 10, step = 4).toDF("id") // ids 1, 5, 9
      .createOrReplaceTempView("merge_source")
    sql(
      s"""MERGE INTO delta.`$tablePath` AS t
         |USING merge_source AS s
         |ON t.id = s.id
         |WHEN MATCHED THEN DELETE""".stripMargin)
  }

  test("a MERGE whose deletion vector was never written does not corrupt the table") {
    val classicConf = Seq.empty[(String, String)]
    assertDmlAbortsCleanlyOnDvCloseFailure(classicConf)(mergeMatchedDelete)
  }


  test("an UPDATE whose deletion vector was never written does not corrupt the table") {
    assertDmlAbortsCleanlyOnDvCloseFailure() { tablePath =>
      sql(s"UPDATE delta.`$tablePath` SET id = id + 1000 WHERE id IN (1, 5, 9)")
    }
  }

  /**
   * Control for the DV test above. The same injected close failure, applied to every file rather
   * than only deletion vectors, must not produce a committed-but-unreadable table: an ordinary data
   * write goes through the Spark output committer, so a file that was never finalised is never
   * promoted into a commit.
   *
   * This isolates what is specific about deletion vectors. If this test were to fail the same way
   * the DV test does, the defect would not be a DV-specific gap at all but a general write-path
   * one, which would change both the diagnosis and the scope of any fix.
   */
  test("control: an ordinary write whose close fails does not commit") {
    withTempDir { dir =>
      withSQLConf(fileSystemConf: _*) {
        val tablePath = s"$failingScheme://${dir.getCanonicalPath}/tbl"
        spark.range(end = 50).toDF("id").write.format("delta").save(tablePath)
        val versionBefore = deltaLogVersion(tablePath)

        // An append whose data-file close() fails. Expected to surface as an error rather than be
        // swallowed. Tolerate either outcome here and assert on the table state below, so that the
        // test reports the interesting fact (what got committed) rather than the exception type.
        val (appendFailed, injections) = countingInjections {
          try {
            withDataFileClosesFailing {
              spark.range(start = 100, end = 110).toDF("id")
                .write.format("delta").mode("append").save(tablePath)
            }
            false
          } catch {
            case _: Exception => true
          }
        }

        // Without this the test would pass vacuously if the injection never reached a parquet
        // write, which is exactly what happened before the fuller create() overloads were
        // intercepted. The comparison with deletion vectors is only meaningful if both actually
        // had a close() failure injected.
        assert(injections > 0, "no close failure was injected, so this proves nothing")

        // Whatever happened, the table must still be readable. Either the append failed and no
        // commit was made, or it somehow succeeded and the data is genuinely there.
        val rows = spark.read.format("delta").load(tablePath).collect()
        val versionAfter = deltaLogVersion(tablePath)

        if (appendFailed) {
          assert(versionAfter === versionBefore,
            "a failed append must not advance the table version")
          assert(rows.length === 50, s"expected the original 50 rows, got ${rows.length}")
        } else {
          assert(rows.length === 60,
            s"an append that reported success must be readable, got ${rows.length} rows")
        }
      }
    }
  }

  // Kill-switch: with the flag off, the fix's tryWithSafeFinally is bypassed and the close goes
  // back through tryWithResource. That reproduces the pre-fix behavior, but whether pre-fix
  // *corrupts* depends on the runtime's tryWithResource: it corrupts where close is swallowed and
  // aborts where close propagates. So assert only what always holds with the flag off: the DELETE
  // does not silently succeed into a readable, correctly-shrunk table (the fix's guarantee). Either
  // it aborts (all 50 rows remain) or it commits a dangling DV (the read fails).
  test("the kill-switch flag off does not apply the propagate-close fix") {
    withTempDir { dir =>
      withSQLConf(
          (fileSystemConf :+
            (DeltaSQLConf.DELETION_VECTOR_PROPAGATE_CLOSE_FAILURE.key -> "false")): _*) {
        withDeletionVectorsEnabled() {
          val tablePath = s"$failingScheme://${dir.getCanonicalPath}/tbl"
          spark.range(end = 50).toDF("id").write.format("delta").save(tablePath)

          val (deleteThrew, injections) = countingInjections {
            withDvClosesFailing {
              try { sql(s"DELETE FROM delta.`$tablePath` WHERE id IN (1, 5, 9)"); false }
              catch { case _: Exception => true }
            }
          }
          assert(injections > 0, "no DV close failure was injected, so this proves nothing")

          if (deleteThrew) {
            // Close propagated (unswallowed tryWithResource): DELETE aborted, table intact.
            assert(spark.read.format("delta").load(tablePath).count() === 50)
          } else {
            // Close swallowed: DELETE committed a dangling DV, so the read fails.
            intercept[Exception](spark.read.format("delta").load(tablePath).collect())
          }
        }
      }
    }
  }

  private def deltaLogVersion(tablePath: String): Long = {
    DeltaLog.forTable(spark, new Path(tablePath)).update().version
  }
}

object DeletionVectorWriteDurabilitySuite {
  val failingScheme = "dvclosefail"
  val injectedFailureMessage = "Injected deletion vector close failure"
}

/**
 * A local filesystem that models a cloud store whose upload is finalised on `close()`, failing that
 * finalize for deletion vector files only. Data files and `_delta_log` writes go to the real local
 * filesystem untouched, so the DML's own write and commit proceed as they would in production.
 *
 * `failDataFileClose` widens the injection to data files, which the control test uses to check that
 * an ordinary write is protected by the output committer where a deletion vector is not.
 */
class DvCloseFailingFileSystem extends RawLocalFileSystem {
  override def getScheme: String = DeletionVectorWriteDurabilitySuite.failingScheme

  override def getUri: URI = URI.create(s"$getScheme:///")

  // Deliberately does not touch the underlying file: a resumable upload creates no object until the
  // finalizing request, so a failed finalize leaves nothing to clean up.
  private def failingStream(f: Path) =
    new FSDataOutputStream(new DiscardingFailOnCloseStream(f), null)

  // The overload DeletionVectorStore calls.
  override def create(f: Path, overwrite: Boolean): FSDataOutputStream = {
    if (DvCloseFailingFileSystem.shouldFailCloseFor(f)) {
      failingStream(f)
    } else {
      super.create(f, overwrite)
    }
  }

  // Parquet's writer does not use the two-argument overload, so intercept the fuller ones too.
  // Without these the control test below would pass vacuously, never having injected anything.
  override def create(
      f: Path,
      overwrite: Boolean,
      bufferSize: Int,
      replication: Short,
      blockSize: Long,
      progress: Progressable): FSDataOutputStream = {
    if (DvCloseFailingFileSystem.shouldFailCloseFor(f)) {
      failingStream(f)
    } else {
      super.create(f, overwrite, bufferSize, replication, blockSize, progress)
    }
  }

  override def create(
      f: Path,
      permission: FsPermission,
      overwrite: Boolean,
      bufferSize: Int,
      replication: Short,
      blockSize: Long,
      progress: Progressable): FSDataOutputStream = {
    if (DvCloseFailingFileSystem.shouldFailCloseFor(f)) {
      failingStream(f)
    } else {
      super.create(f, permission, overwrite, bufferSize, replication, blockSize, progress)
    }
  }
}

object DvCloseFailingFileSystem {
  /** Enabled only around the DML, so table setup writes normally. */
  val failDvClose = new AtomicBoolean(false)

  /** Counts injected failures, so a test cannot pass having silently injected nothing. */
  val injectionCount = new AtomicInteger(0)

  /**
   * Widens the injection to ordinary parquet data files, for the non-DV control test. Deliberately
   * excludes `_delta_log` writes: if the commit itself failed, the control would prove nothing
   * about the output committer, only that an unwritable log aborts the transaction.
   */
  val failDataFileClose = new AtomicBoolean(false)

  // The DV file name prefix is configurable (testOnly.dvFileNamePrefix), so match a substring
  // rather than the start of the name.
  def shouldFailCloseFor(f: Path): Boolean = {
    val isDeltaLog = f.toString.contains("_delta_log")
    val isDv = f.getName.contains("deletion_vector")
    if (isDeltaLog) {
      false
    } else if (isDv) {
      failDvClose.get()
    } else {
      failDataFileClose.get() && f.getName.endsWith(".parquet")
    }
  }
}

/**
 * Accepts and discards the written bytes, then throws from `close()`. The finalize that would have
 * created the object never succeeds, so the object never exists.
 */
private class DiscardingFailOnCloseStream(path: Path) extends OutputStream with Logging {
  override def write(b: Int): Unit = {}

  override def write(b: Array[Byte], off: Int, len: Int): Unit = {}

  override def close(): Unit = {
    val failure = new IOException(
      s"${DeletionVectorWriteDurabilitySuite.injectedFailureMessage} for $path")
    DvCloseFailingFileSystem.injectionCount.incrementAndGet()
    // closeQuietly discards this without a trace, so this line is the only proof the injection
    // fired. Seeing it next to a successful commit is the defect.
    logWarning(s"DV-CLOSE-INJECTION: failing close() for $path", failure)
    throw failure
  }
}
