package org.apache.spark.sql.delta.stats

import org.apache.spark.sql.delta.actions.{AddFile, Metadata, SingleAction}
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.StateCache
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.{DataFrame, _}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.Literal.TrueLiteral
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, NumericType, StringType, StructType, TimestampType}
import org.apache.spark.unsafe.types.UTF8String

/**
 * Note: Please don't add any new constructor to this class. `jackson-module-scala` always picks up
 * the first constructor returned by `Class.getConstructors` but the order of the constructors list
 * is non-deterministic. (SC-13343)
 */
case class DataSize(
    @JsonDeserialize(contentAs = classOf[java.lang.Long])
    bytesCompressed: Option[Long] = None,
    @JsonDeserialize(contentAs = classOf[java.lang.Long])
    rows: Option[Long] = None)

object DataSize {
  def apply(a: ArrayAccumulator): DataSize = {
    DataSize(
      Option(a.value(0)).filterNot(_ == -1),
      Option(a.value(1)).filterNot(_ == -1))
  }
}

/**
 * Used to hold details the files and stats for a scan where we have already
 * applied filters and a limit.
 */
case class DeltaScan(
    version: Long,
    files: Seq[AddFile],
    total: DataSize,
    partition: DataSize,
    scanned: DataSize)(
    // Moved to separate argument list, to not be part of case class equals check -
    // expressions can differ by exprId or ordering, but as long as same files are scanned, the
    // PreparedDeltaFileIndex and HadoopFsRelation should be considered equal for reuse purposes.
    val partitionFilters: ExpressionSet,
    val dataFilters: ExpressionSet,
    val unusedFilters: ExpressionSet,
    val projection: AttributeSet) {
  def allFilters: ExpressionSet = partitionFilters ++ dataFilters ++ unusedFilters
}
