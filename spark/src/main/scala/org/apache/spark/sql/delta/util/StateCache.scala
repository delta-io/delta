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

package org.apache.spark.sql.delta.util

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.delta.{DataFrameUtils, Snapshot}
import org.apache.spark.sql.delta.ClassicColumnConversions._
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.sources.DeltaSQLConf

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.execution.{LogicalRDD, SQLExecution}
import org.apache.spark.storage.StorageLevel

/**
 * Machinary that caches the reconstructed state of a Delta table
 * using the RDD cache. The cache is designed so that the first access
 * will materialize the results.  However once uncache is called,
 * all data will be flushed and will not be cached again.
 */
trait StateCache extends DeltaLogging {
  protected def spark: SparkSession

  /** If state RDDs for this snapshot should still be cached. */
  private var _isCached = true
  /** A list of RDDs that we need to uncache when we are done with this snapshot. */
  private val cached = ArrayBuffer[RDD[_]]()
  private val cached_refs = ArrayBuffer[DatasetRefCache[_]]()

  /** Method to expose the value of _isCached for testing. */
  private[delta] def isCached: Boolean = _isCached

  private val storageLevel = StorageLevel.fromString(
    spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_SNAPSHOT_CACHE_STORAGE_LEVEL))

  class CachedDS[A] private[StateCache](ds: Dataset[A], name: String) {
    // While we cache RDD to avoid re-computation in different spark sessions, `Dataset` can only be
    // reused by the session that created it to avoid session pollution. So we use `DatasetRefCache`
    // to re-create a new `Dataset` when the active session is changed. This is an optimization for
    // single-session scenarios to avoid the overhead of `Dataset` creation which can take 100ms.
    private val cachedDs = cached.synchronized {
      if (isCached) {
        val qe = ds.queryExecution
        val rdd = SQLExecution.withNewExecutionId(qe, Some(s"Cache $name")) {
          val rdd = recordFrameProfile("Delta", "CachedDS.toRdd") {
            // toRdd should always trigger execution
            qe.toRdd.map(_.copy())
          }
          rdd.setName(name)
          rdd.persist(storageLevel)
        }
        cached += rdd
        val dsCache = datasetRefCache { () =>
          val logicalRdd = LogicalRDD(qe.analyzed.output, rdd)(spark)
          DataFrameUtils.ofRows(spark, logicalRdd)
        }
        Some(dsCache)
      } else {
        None
      }
    }

    /**
     * Retrieves the cached RDD in Dataframe form.
     *
     * If a RDD cache is available,
     * - return the cached DF if called from the same session in which the cached DF is created, or
     * - reconstruct the DF using the RDD cache if called from a different session.
     *
     * If no RDD cache is available,
     * - return a copy of the original DF with updated spark session.
     *
     * Since a cached DeltaLog can be accessed from multiple Spark sessions, this interface makes
     * sure that the original Spark session in the cached DF does not leak into the current active
     * sessions.
     */
    def getDF: DataFrame = {
      if (cached.synchronized(isCached) && cachedDs.isDefined) {
        cachedDs.get.get
      } else {
        DataFrameUtils.ofRows(spark, ds.queryExecution.logical)
      }
    }

    /**
     * Retrieves the cached RDD as a strongly-typed Dataset.
     */
    def getDS: Dataset[A] = getDF.as(ds.encoder)
  }

  /**
   * Create a CachedDS instance for the given Dataset and the name.
   */
  def cacheDS[A](ds: Dataset[A], name: String): CachedDS[A] = recordFrameProfile(
    "Delta", "CachedDS.cacheDS") {
    new CachedDS[A](ds, name)
  }

  def datasetRefCache[A](creator: () => Dataset[A]): DatasetRefCache[A] = {
    val dsCache = new DatasetRefCache(creator)
    cached_refs += dsCache
    dsCache
  }

  /** Drop any cached data for this [[Snapshot]]. */
  def uncache(): Unit = cached.synchronized {
    if (isCached) {
      _isCached = false
      cached.foreach(_.unpersist(blocking = false))
      cached_refs.foreach(_.invalidate())
    }
  }
}
