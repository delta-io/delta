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

package org.apache.spark.sql.delta.perf

import org.apache.spark.{ShuffleDependency, SparkConf, TaskContext}
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.network.shuffle.MergedBlockMeta
import org.apache.spark.shuffle._
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.storage.{BlockId, ShuffleMergedBlockId}

/**
 * A [[ShuffleManager]] that serves data only through its own reader. getBlockData() throws
 * unless the read came through getReader(), which is what a remote shuffle service amounts
 * to: its blocks are not addressable as local files.
 *
 * Deliberately self-contained, using nothing from the optimized-write read paths, so it
 * compiles against any Delta tree. The suite below fails with a direct-block-fetch error if
 * DeltaOptimizedWriterExec reads shuffle blocks directly rather than through the shuffle
 * manager's reader.
 */
class NoBlockAccessShuffleManager(conf: SparkConf) extends ShuffleManager {
  private val delegate = new SortShuffleManager(conf)

  override def registerShuffle[K, V, C](
      shuffleId: Int,
      dependency: ShuffleDependency[K, V, C]): ShuffleHandle =
    delegate.registerShuffle(shuffleId, dependency)

  override def getWriter[K, V](
      handle: ShuffleHandle,
      mapId: Long,
      context: TaskContext,
      metrics: ShuffleWriteMetricsReporter): ShuffleWriter[K, V] =
    delegate.getWriter(handle, mapId, context, metrics)

  override def getReader[K, C](
      handle: ShuffleHandle,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext,
      metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C] = {
    val inner = delegate.getReader[K, C](
      handle, startMapIndex, endMapIndex, startPartition, endPartition, context, metrics)
    new ShuffleReader[K, C] {
      override def read(): Iterator[Product2[K, C]] = {
        val it = NoBlockAccessShuffleManager.throughReader(inner.read())
        new Iterator[Product2[K, C]] {
          override def hasNext: Boolean =
            NoBlockAccessShuffleManager.throughReader(it.hasNext)
          override def next(): Product2[K, C] =
            NoBlockAccessShuffleManager.throughReader(it.next())
        }
      }
    }
  }

  override def unregisterShuffle(shuffleId: Int): Boolean = delegate.unregisterShuffle(shuffleId)

  override def shuffleBlockResolver: ShuffleBlockResolver = new ShuffleBlockResolver {
    private val inner = delegate.shuffleBlockResolver
    override def getBlockData(blockId: BlockId, dirs: Option[Array[String]]): ManagedBuffer = {
      if (!NoBlockAccessShuffleManager.insideReader.get()) {
        throw new UnsupportedOperationException(
          s"direct block fetch of $blockId: this shuffle manager does not expose blocks " +
            "as local files; read through ShuffleManager.getReader()")
      }
      inner.getBlockData(blockId, dirs)
    }
    override def getMergedBlockData(
        blockId: ShuffleMergedBlockId,
        dirs: Option[Array[String]]): Seq[ManagedBuffer] = inner.getMergedBlockData(blockId, dirs)
    override def getMergedBlockMeta(
        blockId: ShuffleMergedBlockId,
        dirs: Option[Array[String]]): MergedBlockMeta = inner.getMergedBlockMeta(blockId, dirs)
    override def stop(): Unit = inner.stop()
  }

  override def stop(): Unit = delegate.stop()
}

object NoBlockAccessShuffleManager {
  /** Set while a read is running through this manager's reader. */
  val insideReader: ThreadLocal[java.lang.Boolean] =
    ThreadLocal.withInitial[java.lang.Boolean](() => false)

  def throughReader[T](body: => T): T = {
    val prev = insideReader.get()
    insideReader.set(true)
    try body finally insideReader.set(prev)
  }
}

/**
 * Regression test for optimized writes on a shuffle manager that does not expose blocks as
 * local files, which is every remote shuffle service.
 */
class OptimizedWritesNoBlockAccessSuite extends QueryTest
  with SharedSparkSession
  with DeltaSQLCommandTest {

  override protected def sparkConf: SparkConf = super.sparkConf
    .set("spark.shuffle.manager", classOf[NoBlockAccessShuffleManager].getName)
    .set("spark.databricks.delta.optimizeWrite.enabled", "true")

  test("optimized write succeeds when shuffle blocks are not addressable as files") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath + "/tbl"
      val df = spark.range(0, 1000, 1, 8).toDF("id")
      df.write.format("delta").save(path)
      val back = spark.read.format("delta").load(path)
      assert(back.count() === 1000)
      assert(back.select("id").distinct().count() === 1000)
      // Without optimized write the 8 input partitions produce 8 files, so this also shows
      // the feature was in effect rather than silently skipped.
      val files = new java.io.File(path).listFiles.count(_.getName.endsWith(".parquet"))
      assert(files === 1, s"expected optimized write to coalesce to one file, got $files")
    }
  }

  test("optimized write succeeds on a partitioned write") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath + "/tbl"
      val df = spark.range(0, 1000, 1, 8).toDF("id").withColumn("p", col("id") % 4)
      df.write.format("delta").partitionBy("p").save(path)
      val back = spark.read.format("delta").load(path)
      assert(back.count() === 1000)
      assert(back.select("id").distinct().count() === 1000)
    }
  }
}
