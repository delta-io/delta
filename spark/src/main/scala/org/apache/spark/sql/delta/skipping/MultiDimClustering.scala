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

package org.apache.spark.sql.delta.skipping

import java.util.UUID

import org.apache.spark.sql.delta.skipping.MultiDimClusteringFunctions._
import org.apache.spark.sql.delta.sources.DeltaSQLConf

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.ColumnImplicitsShim._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/** Trait for changing the data layout using a multi-dimensional clustering algorithm */
trait MultiDimClustering extends Logging {
  /** Repartition the given `df` into `approxNumPartitions` based on the provided `colNames`. */
  def cluster(
      df: DataFrame,
      colNames: Seq[String],
      approxNumPartitions: Int,
      randomizationExpressionOpt: Option[Column]
  ): DataFrame
}

object MultiDimClustering {
  /**
   * Repartition the given dataframe `df` based on the given `curve` type into
   * `approxNumPartitions` on the given `colNames`.
   */
  def cluster(
      df: DataFrame,
      approxNumPartitions: Int,
      colNames: Seq[String],
      curve: String): DataFrame = {
    assert(colNames.nonEmpty, "Cannot cluster by zero columns!")
    val clusteringImpl = curve match {
      case "hilbert" if colNames.size == 1 => ZOrderClustering
      case "hilbert" => HilbertClustering
      case "zorder" => ZOrderClustering
      case unknownCurve =>
        throw new SparkException(s"Unknown curve ($unknownCurve), unable to perform multi " +
          "dimensional clustering.")
    }
    clusteringImpl.cluster(df, colNames, approxNumPartitions, randomizationExpressionOpt = None)
  }
}

/** Base class for space filling curve based clustering e.g. ZOrder */
trait SpaceFillingCurveClustering extends MultiDimClustering {

  protected def getClusteringExpression(cols: Seq[Column], numRanges: Int): Column

  override def cluster(
      df: DataFrame,
      colNames: Seq[String],
      approxNumPartitions: Int,
      randomizationExpressionOpt: Option[Column]): DataFrame = {
    val conf = df.sparkSession.sessionState.conf
    val numRanges = conf.getConf(DeltaSQLConf.MDC_NUM_RANGE_IDS)
    val addNoise = conf.getConf(DeltaSQLConf.MDC_ADD_NOISE)

    val cols = colNames.map(df(_))
    val mdcCol = getClusteringExpression(cols, numRanges)
    val repartitionKeyColName = s"${UUID.randomUUID().toString}-rpKey1"

    var repartitionedDf = if (addNoise) {
      val randByteColName = s"${UUID.randomUUID().toString}-rpKey2"
      val randByteCol = randomizationExpressionOpt.getOrElse((rand() * 255 - 128).cast(ByteType))
      df.withColumn(repartitionKeyColName, mdcCol).withColumn(randByteColName, randByteCol)
        .repartitionByRange(approxNumPartitions, col(repartitionKeyColName), col(randByteColName))
        .drop(randByteColName)
    } else {
      df.withColumn(repartitionKeyColName, mdcCol)
        .repartitionByRange(approxNumPartitions, col(repartitionKeyColName))
    }

    repartitionedDf.drop(repartitionKeyColName)
  }
}

/** Implement Z-Order clustering */
object ZOrderClustering extends SpaceFillingCurveClustering {
  override protected[skipping] def getClusteringExpression(
      cols: Seq[Column], numRanges: Int): Column = {
    assert(cols.size >= 1, "Cannot do Z-Order clustering by zero columns!")
    val rangeIdCols = cols.map(range_partition_id(_, numRanges))
    interleave_bits(rangeIdCols: _*).cast(StringType)
  }
}

object HilbertClustering extends SpaceFillingCurveClustering with Logging {
  override protected def getClusteringExpression(cols: Seq[Column], numRanges: Int): Column = {
    assert(cols.size > 1, "Cannot do Hilbert clustering by zero or one column!")
    val rangeIdCols = cols.map(range_partition_id(_, numRanges))
    val numBits = Integer.numberOfTrailingZeros(Integer.highestOneBit(numRanges)) + 1
    hilbert_index(numBits, rangeIdCols: _*)
  }
}
