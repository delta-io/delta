/*
 * Copyright 2019 Databricks, Inc.
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

package org.apache.spark.sql.delta.util

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.delta.Snapshot

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.LogicalRDD
import org.apache.spark.storage.StorageLevel

/**
 * Machinary that caches the reconstructed state of a Delta table
 * using the RDD cache. The cache is designed so that the first access
 * will materialize the results.  However once uncache is called,
 * all data will be flushed and will not be cached again.
 */
trait StateCache {
  protected def spark: SparkSession

  /** If state RDDs for this snapshot should still be cached. */
  private var isCached = true
  /** A list of RDDs that we need to uncache when we are done with this snapshot. */
  private val cached = ArrayBuffer[RDD[_]]()

  class CachedDS[A](ds: Dataset[A], name: String) {
    private val rddCache = cached.synchronized {
      if (isCached) {
        val rdd = ds.queryExecution.toRdd.map(_.copy())
        rdd.setName(name)
        rdd.persist(StorageLevel.MEMORY_AND_DISK_SER)
        cached += rdd
        Some(rdd)
      } else {
        None
      }
    }

    /**
     * Get the DS from the cache.
     *
     * If a RDD cache is available, we reconstruct the DS from the RDD cache;
     * otherwise, we return the original DF but replace its spark session with the current active
     * spark session.
     *
     * As a cached DeltaLog can be accessed from multiple Spark sessions, this method prevents the
     * original Spark session from polluting other Spark sessions in which the DeltaLog is accessed.
     */
    def getDS: Dataset[A] = {
      implicit val enc = ds.exprEnc
      if (cached.synchronized(isCached) && rddCache.isDefined) {
        Dataset.ofRows(
          spark,
          LogicalRDD(
            ds.queryExecution.analyzed.output,
            rddCache.get)(
            spark)).as[A]
      } else {
        Dataset.ofRows(
          spark,
          ds.queryExecution.logical
        ).as[A]
      }
    }
  }

  /**
   * Create a CachedDS instance for the given Dataset and the name.
   */
  def cacheDS[A](ds: Dataset[A], name: String): CachedDS[A] = {
    new CachedDS[A](ds, name)
  }

  /** Drop any cached data for this [[Snapshot]]. */
  def uncache(): Unit = cached.synchronized {
    if (isCached) {
      isCached = false
      cached.foreach(_.unpersist(blocking = false))
    }
  }
}
