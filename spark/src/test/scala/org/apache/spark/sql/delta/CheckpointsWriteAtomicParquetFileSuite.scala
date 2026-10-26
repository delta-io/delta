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

import java.io.File
import java.net.URI
import java.util.UUID

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, RawLocalFileSystem}

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.functions.{col, struct}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType

/** Tests for the generic [[Checkpoints.writeAtomicCheckpointParquetFile]] helper */
class CheckpointsWriteAtomicParquetFileSuite extends QueryTest with SharedSparkSession {

  import testImplicits._

  /** Non-hidden entries in `dir` (filters out checksum sidecars like `.<name>.crc`). */
  private def visibleEntries(dir: File): Seq[File] =
    Option(dir.listFiles()).map(_.toSeq).getOrElse(Seq.empty).filterNot(_.getName.startsWith("."))

  /** True if `t` or any of its causes carries `marker` in its message. */
  private def exceptionCauseChainContains(t: Throwable, marker: String): Boolean =
    Iterator.iterate(t)(_.getCause).takeWhile(_ != null)
      .exists(e => Option(e.getMessage).exists(_.contains(marker)))

  // These tests write standalone parquet files with no backing log, so there are no DataFrame
  // options to honor and a plain session hadoop conf is appropriate.
  // scalastyle:off deltahadoopconfiguration
  private def newHadoopConf(): Configuration = spark.sessionState.newHadoopConf()
  // scalastyle:on deltahadoopconfiguration

  test("writes a single parquet file at the exact caller-specified path") {
    withTempDir { tempDir =>
      val finalPath = new Path(tempDir.getCanonicalPath, s"leaf-${UUID.randomUUID()}.parquet")
      val df = Seq((1, "a"), (2, "b")).toDF("id", "name")

      Checkpoints.writeAtomicCheckpointParquetFile(
        spark = spark,
        df = df,
        finalPath = finalPath,
        hadoopConf = newHadoopConf(),
        useRename = false)

      // The file lands at the exact path the caller asked for -- no Spark `part-*` naming.
      val finalFile = new File(finalPath.toUri.getPath)
      assert(finalFile.exists() && finalFile.isFile)

      // No staging directory and no extra data files are created next to it.
      val entries = visibleEntries(tempDir)
      assert(entries.map(_.getName).toSet === Set(finalPath.getName),
        s"expected only ${finalPath.getName}, found ${entries.map(_.getName).mkString(", ")}")
      assert(entries.forall(_.isFile), "no staging subdirectory should be created")

      checkAnswer(spark.read.parquet(finalPath.toString), df)
    }
  }

  test("useRename = true writes via a temp file and atomic-renames, leaving no temp behind") {
    withTempDir { tempDir =>
      val finalPath = new Path(tempDir.getCanonicalPath, s"root-${UUID.randomUUID()}.parquet")
      val df = Seq((10, "x"), (20, "y")).toDF("id", "name")

      Checkpoints.writeAtomicCheckpointParquetFile(
        spark = spark,
        df = df,
        finalPath = finalPath,
        hadoopConf = newHadoopConf(),
        useRename = true)

      assert(new File(finalPath.toUri.getPath).exists())
      // The `.<name>.<uuid>.tmp` staging file must not survive a successful write.
      val leftovers = Option(tempDir.listFiles()).map(_.toSeq).getOrElse(Seq.empty)
        .filter(_.getName.contains(".tmp"))
      assert(leftovers.isEmpty,
        s"temp files left behind: ${leftovers.map(_.getName).mkString(", ")}")

      checkAnswer(spark.read.parquet(finalPath.toString), df)
    }
  }

  test("is schema-agnostic and returns the written (nullable) schema") {
    withTempDir { tempDir =>
      val finalPath = new Path(tempDir.getCanonicalPath, "nested.parquet")
      // A schema that has nothing to do with the V2 checkpoint `SingleAction` columns.
      val df = spark.range(0, 5).toDF("value")
        .withColumn("nested", struct(col("value").as("a"), (col("value") * 2).as("b")))

      val writtenSchema = Checkpoints.writeAtomicCheckpointParquetFile(
        spark = spark,
        df = df,
        finalPath = finalPath,
        hadoopConf = newHadoopConf(),
        useRename = false)

      assert(writtenSchema.fieldNames === Array("value", "nested"))
      assert(writtenSchema("nested").dataType.isInstanceOf[StructType])
      checkAnswer(spark.read.parquet(finalPath.toString), df)
    }
  }


  test("raises failOnCheckpointRename when the renamed file is missing") {
    withTempDir { tempDir =>
      val hadoopConf = newHadoopConf()
      // Route the write through a filesystem whose `rename` reports success but moves nothing,
      // so `finalPath` never materializes and the post-rename missing-file path is exercised.
      hadoopConf.set(
        s"fs.${RenameNoOpLocalFileSystem.scheme}.impl",
        classOf[RenameNoOpLocalFileSystem].getName)
      val finalPath = new Path(
        s"${RenameNoOpLocalFileSystem.scheme}://${tempDir.getCanonicalPath}/" +
          s"root-${UUID.randomUUID()}.parquet")
      val df = Seq((1, "a")).toDF("id", "name")

      // The missing-after-rename path raises DeltaErrors.failOnCheckpointRename, a
      // DeltaIllegalStateException with error class DELTA_CANNOT_RENAME_PATH.
      val ex = intercept[DeltaIllegalStateException] {
        Checkpoints.writeAtomicCheckpointParquetFile(
          spark = spark,
          df = df,
          finalPath = finalPath,
          hadoopConf = hadoopConf,
          useRename = true)
      }
      assert(ex.getErrorClass == "DELTA_CANNOT_RENAME_PATH")
      assert(exceptionCauseChainContains(ex, finalPath.getName))
    }
  }
}

/**
 * A [[RawLocalFileSystem]] whose `rename` is a no-op that reports success, leaving the
 * destination absent. Lets a test drive `writeAtomicCheckpointParquetFile` into its
 * missing-after-rename branch.
 */
class RenameNoOpLocalFileSystem extends RawLocalFileSystem {
  private var uri: URI = _

  override def getScheme: String = RenameNoOpLocalFileSystem.scheme

  override def initialize(name: URI, conf: Configuration): Unit = {
    uri = URI.create(name.getScheme + ":///")
    super.initialize(name, conf)
  }

  override def getUri: URI = if (uri == null) super.getUri else uri

  override def rename(src: Path, dst: Path): Boolean = true
}

object RenameNoOpLocalFileSystem {
  val scheme = "renamenoopfs"
}
