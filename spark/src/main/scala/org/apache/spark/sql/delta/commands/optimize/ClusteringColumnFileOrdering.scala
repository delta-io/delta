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

package org.apache.spark.sql.delta.commands.optimize

import java.util.{Arrays, Comparator}

import org.apache.spark.sql.delta.{DeltaErrors, Snapshot}
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.stats.DeltaStatistics.{MAX, MIN}

import org.apache.spark.sql.{Column, Encoders, SparkSession}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.functions.col

/**
 * Orders the files of a clustered table for lightweight compaction by the per-file min/max
 * statistics of a single clustering column.
 *
 * Compaction packs files into bins sequentially in the given order and writes each bin out as one
 * file without sorting rows. Ordering the files by the (min, max) values of a column makes each
 * bin cover a contiguous range of that column, so data skipping on the column keeps working after
 * compaction. Only one column is used: grouping whole files cannot keep several columns narrow at
 * once, and clustering takes care of all clustering columns later.
 */
object ClusteringColumnFileOrdering {

  /** The min and max values of a column in a file. Both are null without statistics. */
  case class ValueRange(min: Any, max: Any) {
    def isKnown: Boolean = min != null && max != null
  }

  /**
   * Returns the files ordered for lightweight compaction, and the clustering column used to
   * order them. If `column` is not set, the clustering column whose files cover the narrowest
   * part of its value range is used, provided that [[relativeFileRange]] is at most
   * `maxRelativeFileRange`. If no column qualifies, the files are ordered by size.
   */
  def order(
      spark: SparkSession,
      snapshot: Snapshot,
      files: Seq[AddFile],
      clusteringColumns: Seq[String],
      column: Option[String],
      maxRelativeFileRange: Double): (Seq[AddFile], Option[String]) = {
    val columns = column match {
      case Some(c) => Seq(resolve(c, clusteringColumns))
      case None => clusteringColumns
    }
    if (files.size < 2 || columns.isEmpty) return (files.sortBy(_.size), None)

    val ranges = loadRanges(spark, snapshot, files, columns.map(nameParts))
    def rangesOf(i: Int): Seq[ValueRange] =
      files.map(f => ranges.get(f.path).map(_(i)).getOrElse(ValueRange(null, null)))

    val chosen = if (column.isDefined) {
      Some(0)
    } else {
      columns.indices
        .flatMap(i => relativeFileRange(rangesOf(i)).map(i -> _))
        .filter(_._2 <= maxRelativeFileRange)
        .sortBy(_._2)
        .headOption
        .map(_._1)
    }
    chosen match {
      case Some(i) => (sort(files.zip(rangesOf(i))), Some(columns(i)))
      case None => (files.sortBy(_.size), None)
    }
  }

  /**
   * Returns how much of the value range of a column a file covers on average, from 0 when no two
   * files overlap to 1 when every file covers all values. Values are replaced by their rank among
   * the distinct min/max values of all files, so the measure works for any orderable type and
   * reflects how much files overlap rather than how values are distributed. Returns None if fewer
   * than two files have statistics or all values are equal.
   */
  private[delta] def relativeFileRange(ranges: Seq[ValueRange]): Option[Double] = {
    val known = ranges.filter(_.isKnown)
    if (known.size < 2) return None
    val values = known.flatMap(r => Seq(r.min, r.max)).map(_.asInstanceOf[AnyRef]).toArray
    Arrays.sort(values, valueComparator)
    val distinct = values.foldLeft(Vector.empty[AnyRef]) { (acc, v) =>
      if (acc.nonEmpty && valueComparator.compare(acc.last, v) == 0) acc else acc :+ v
    }.toArray
    if (distinct.length < 2) return None
    def rank(v: Any): Int = Arrays.binarySearch(distinct, v.asInstanceOf[AnyRef], valueComparator)
    val totalWidth = known.map(r => (rank(r.max) - rank(r.min)).toLong).sum
    Some(totalWidth.toDouble / known.size / (distinct.length - 1))
  }

  /** Orders files by (min, max), with files without statistics last, ordered by size. */
  private def sort(files: Seq[(AddFile, ValueRange)]): Seq[AddFile] = {
    files.sortWith { case ((f1, r1), (f2, r2)) =>
      val c = compareRanges(r1, r2)
      if (c != 0) c < 0 else if (f1.size != f2.size) f1.size < f2.size else f1.path < f2.path
    }.map(_._1)
  }

  private[delta] def compareRanges(r1: ValueRange, r2: ValueRange): Int = {
    if (!r1.isKnown || !r2.isKnown) return java.lang.Boolean.compare(!r1.isKnown, !r2.isKnown)
    val c = valueComparator.compare(r1.min.asInstanceOf[AnyRef], r2.min.asInstanceOf[AnyRef])
    if (c != 0) c else {
      valueComparator.compare(r1.max.asInstanceOf[AnyRef], r2.max.asInstanceOf[AnyRef])
    }
  }

  private val valueComparator: Comparator[AnyRef] = new Comparator[AnyRef] {
    override def compare(a: AnyRef, b: AnyRef): Int =
      a.asInstanceOf[Comparable[AnyRef]].compareTo(b)
  }

  /** Loads the min/max values of the given columns, by file path. */
  private def loadRanges(
      spark: SparkSession,
      snapshot: Snapshot,
      files: Seq[AddFile],
      columns: Seq[Seq[String]]): Map[String, IndexedSeq[ValueRange]] = {
    val statsColumns: Seq[Column] = columns.flatMap { parts =>
      Seq(MIN, MAX).map(snapshot.getStatsColumnOrNullLiteral(_, parts.reverse))
    }
    val paths = spark.createDataset(files.map(_.path).distinct)(Encoders.STRING).toDF("path")
    snapshot.withStats
      .join(paths, "path")
      .select(col("path") +: statsColumns: _*)
      .collect()
      .map { row =>
        row.getString(0) ->
          columns.indices.map(i => ValueRange(row.get(1 + 2 * i), row.get(2 + 2 * i)))
      }
      .toMap
  }

  private def nameParts(column: String): Seq[String] =
    UnresolvedAttribute.quotedString(column).nameParts

  private def resolve(column: String, clusteringColumns: Seq[String]): String = {
    val parts = nameParts(column)
    def matches(c: String): Boolean = {
      val other = nameParts(c)
      other.size == parts.size && other.zip(parts).forall { case (a, b) => a.equalsIgnoreCase(b) }
    }
    clusteringColumns.find(matches).getOrElse {
      throw DeltaErrors.invalidLightweightClusteringColumn(column, clusteringColumns)
    }
  }
}
