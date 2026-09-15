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

package org.apache.spark.sql.delta.amt

import org.apache.spark.sql.delta.ClassicColumnConversions._
import org.apache.spark.sql.delta.DeltaColumnMapping
import org.apache.spark.sql.delta.util.PartitionUtils

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.catalyst.expressions.Cast
import org.apache.spark.sql.execution.datasources.parquet.ParquetUtils
import org.apache.spark.sql.functions.{array, col, element_at, lit, map_from_arrays, struct, when}
import org.apache.spark.sql.types.{MapType, MetadataBuilder, StringType, StructType}

/**
 * Converts between Delta's physical-name string map and Iceberg's typed partition struct.
 *
 * TODO(v4amt): shrink this converter. It exists because the two sides disagree on
 *   representation: Delta stores partition values as `AddFile.partitionValues`, a physical-name to
 *   string map, while the Iceberg V4 `partition` field is a struct with one typed field per
 *   partition column.
 *
 *   https://github.com/delta-io/delta/issues/6953 proposes typing partitionValues in the Delta log
 *   itself. That removes [[forWrite]]'s per-field cast, since the source would already be typed,
 *   but it does not remove [[forRead]]'s render: log replay materializes an in-memory `AddFile`,
 *   whose `partitionValues` is a `Map[String, String]`, so something still has to turn the typed
 *   struct back into strings. Retiring the converter outright needs that field typed as well, not
 *   just the log format.
 */
private[amt] object AMTPartitionValues {

  /** Iceberg assigns partition-field ids from 1000, in partition-spec order. */
  private val PARTITION_FIELD_ID_START: Long = 1000L

  /**
   * The partition struct as persisted: every column keeps its real type and logical name, with
   * Iceberg partition-field ids stamped from 1000 in partition-spec order.
   */
  def persistedSchema(partitionSchema: StructType): StructType =
    StructType(partitionSchema.fields.zipWithIndex.map { case (field, ordinal) =>
      field.copy(
        nullable = true,
        metadata = new MetadataBuilder()
          .putLong(ParquetUtils.FIELD_ID_METADATA_KEY, PARTITION_FIELD_ID_START + ordinal)
          .build())
    })

  /**
   * Physical-name string map -> persisted partition struct.
   *
   * The struct's fields carry the partition columns' *logical* names, as Iceberg expects. That is
   * safe because an Iceberg reader resolves partition fields by field ID (stamped from 1000 in
   * partition-spec order, see `AMTSingleAction.persistedSchema`), not by name, so the names here
   * are informational and a column rename does not change how the struct is read.
   */
  def forWrite(df: DataFrame, partitionSchema: StructType): DataFrame = {
    if (partitionSchema.isEmpty) return df.drop("partition")
    val raw = col("partition")
    // String -> typed mirrors what UniForm's Iceberg conversion does when it parses partition
    // values out of a directory path: `PartitionUtils.parsePartitionValue` casts the string to the
    // partition column's type. The cast is ANSI here so an unparseable value fails the write rather
    // than silently writing a null partition value into the manifest.
    val typedFields = partitionSchema.map { field =>
      val physicalName = DeltaColumnMapping.getPhysicalName(field)
      Column(Cast(
        element_at(raw, lit(physicalName)).expr,
        field.dataType,
        ansiEnabled = true)).as(field.name)
    }
    val persistedPartitionSchema = AMTPartitionValues.persistedSchema(partitionSchema)
    val partition = when(raw.isNull, lit(null).cast(persistedPartitionSchema))
      .otherwise(struct(typedFields: _*))
    replacePartition(df, partition)
  }

  /** Persisted partition struct -> physical-name string map. */
  def forRead(df: DataFrame, partitionSchema: StructType): DataFrame = {
    val values = if (partitionSchema.isEmpty) {
      lit(null).cast(MapType(StringType, StringType, valueContainsNull = true))
    } else {
      val typedPartition = col("partition")
      val keys = array(partitionSchema.map(f => lit(DeltaColumnMapping.getPhysicalName(f))): _*)
      // typed -> String reuses the renderer UniForm writes its partition values with:
      // `ConvertUtils` calls `PartitionUtils.literalToNormalizedString` on each parsed literal.
      // `expressionToNormalizedString` is that function in expression form, so a value converted
      // here is byte-identical to what Delta would have logged for it.
      //
      // AMT always renders a timestamp UTC-normalized, rather than following
      // `write.utcTimestampPartitionValues`. That conf exists to preserve a legacy write format,
      // and it is a session conf the log does not record per file -- so honoring it here would make
      // reading a manifest depend on the session doing the read, and a manifest written by one
      // session unreadable by another. A UTC-normalized timestamp also needs no time zone to
      // interpret, which is why `timeZoneId` is left unset: it would only be consulted on the
      // branch this never takes.
      val stringValues = array(partitionSchema.map { field =>
        Column(PartitionUtils.expressionToNormalizedString(
          value = typedPartition.getField(field.name).expr,
          dataType = field.dataType,
          timeZoneId = None,
          useUtcNormalizedTimestamp = true))
      }: _*)
      when(typedPartition.isNull, lit(null)).otherwise(map_from_arrays(keys, stringValues))
    }
    if (df.columns.contains("partition")) {
      replacePartition(df, values)
    } else {
      df.withColumn("partition", values)
    }
  }

  private def replacePartition(df: DataFrame, partition: Column): DataFrame =
    df.select(df.columns.map { name =>
      if (name == "partition") partition.as(name) else col(name)
    }: _*)
}
