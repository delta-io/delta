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

package org.apache.spark.sql.delta.sources

import java.util.Locale
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.delta.TypeWideningMode
import org.apache.spark.sql.delta.schema.SchemaUtils
import org.apache.spark.sql.sources
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

object DeltaSourceUtils {
  val NAME = "delta"
  val ALT_NAME = "delta"

  // Batch relations don't pass partitioning columns to `CreatableRelationProvider`s, therefore
  // as a hack, we pass in the partitioning columns among the options.
  val PARTITIONING_COLUMNS_KEY = "__partition_columns"


  // The metadata key recording the generation expression in a generated column's `StructField`.
  val GENERATION_EXPRESSION_METADATA_KEY = "delta.generationExpression"


  val IDENTITY_INFO_ALLOW_EXPLICIT_INSERT = "delta.identity.allowExplicitInsert"
  val IDENTITY_INFO_START = "delta.identity.start"
  val IDENTITY_INFO_STEP = "delta.identity.step"
  val IDENTITY_INFO_HIGHWATERMARK = "delta.identity.highWaterMark"
  val IDENTITY_COMMITINFO_TAG = "delta.identity.schemaUpdate"

  def isDeltaDataSourceName(name: String): Boolean = {
    name.toLowerCase(Locale.ROOT) == NAME || name.toLowerCase(Locale.ROOT) == ALT_NAME
  }

  /** Check whether this table is a Delta table based on information from the Catalog. */
  def isDeltaTable(provider: Option[String]): Boolean = {
    provider.exists(isDeltaDataSourceName)
  }

  /** Creates Spark literals from a value exposed by the public Spark API. */
  private def createLiteral(value: Any): expressions.Literal = value match {
    case v: String => expressions.Literal.create(v)
    case v: Int => expressions.Literal.create(v)
    case v: Byte => expressions.Literal.create(v)
    case v: Short => expressions.Literal.create(v)
    case v: Long => expressions.Literal.create(v)
    case v: Double => expressions.Literal.create(v)
    case v: Float => expressions.Literal.create(v)
    case v: Boolean => expressions.Literal.create(v)
    case v: java.sql.Date => expressions.Literal.create(v)
    case v: java.sql.Timestamp => expressions.Literal.create(v)
    case v: java.time.Instant => expressions.Literal.create(v)
    case v: java.time.LocalDate => expressions.Literal.create(v)
    case v: BigDecimal => expressions.Literal.create(v)
  }

  /** Translates the public Spark Filter APIs into Spark internal expressions. */
  def translateFilters(filters: Array[Filter]): Expression = filters.map {
    case sources.EqualTo(attribute, value) =>
      expressions.EqualTo(UnresolvedAttribute(attribute), expressions.Literal.create(value))
    case sources.EqualNullSafe(attribute, value) =>
      expressions.EqualNullSafe(UnresolvedAttribute(attribute), expressions.Literal.create(value))
    case sources.GreaterThan(attribute, value) =>
      expressions.GreaterThan(UnresolvedAttribute(attribute), expressions.Literal.create(value))
    case sources.GreaterThanOrEqual(attribute, value) =>
      expressions.GreaterThanOrEqual(
        UnresolvedAttribute(attribute), expressions.Literal.create(value))
    case sources.LessThan(attribute, value) =>
      expressions.LessThan(UnresolvedAttribute(attribute), expressions.Literal.create(value))
    case sources.LessThanOrEqual(attribute, value) =>
      expressions.LessThanOrEqual(UnresolvedAttribute(attribute), expressions.Literal.create(value))
    case sources.In(attribute, values) =>
      expressions.In(UnresolvedAttribute(attribute), values.map(createLiteral))
    case sources.IsNull(attribute) => expressions.IsNull(UnresolvedAttribute(attribute))
    case sources.IsNotNull(attribute) => expressions.IsNotNull(UnresolvedAttribute(attribute))
    case sources.Not(otherFilter) => expressions.Not(translateFilters(Array(otherFilter)))
    case sources.And(filter1, filter2) =>
      expressions.And(translateFilters(Array(filter1)), translateFilters(Array(filter2)))
    case sources.Or(filter1, filter2) =>
      expressions.Or(translateFilters(Array(filter1)), translateFilters(Array(filter2)))
    case sources.StringStartsWith(attribute, value) =>
      new expressions.Like(
        UnresolvedAttribute(attribute), expressions.Literal.create(s"${value}%"))
    case sources.StringEndsWith(attribute, value) =>
      new expressions.Like(
        UnresolvedAttribute(attribute), expressions.Literal.create(s"%${value}"))
    case sources.StringContains(attribute, value) =>
      new expressions.Like(
        UnresolvedAttribute(attribute), expressions.Literal.create(s"%${value}%"))
    case sources.AlwaysTrue() => expressions.Literal.TrueLiteral
    case sources.AlwaysFalse() => expressions.Literal.FalseLiteral
  }.reduceOption(expressions.And).getOrElse(expressions.Literal.TrueLiteral)

  /**
   * Configuration options for schema compatibility validation during Delta streaming reads.
   *
   * This class encapsulates various flags and settings that control how Delta streaming handles
   * schema changes and compatibility checks.
   *
   * @param allowUnsafeStreamingReadOnColumnMappingSchemaChanges
   *        Flag that allows user to force enable unsafe streaming read on Delta table with
   *        column mapping enabled AND drop/rename actions.
   * @param allowUnsafeStreamingReadOnPartitionColumnChanges
   *        Flag that allows user to force enable unsafe streaming read on Delta table with
   *        column mapping enabled AND partition column changes.
   * @param forceEnableStreamingReadOnReadIncompatibleSchemaChangesDuringStreamStart
   *        Flag that allows user to disable the read-compatibility check during stream start which
   *        protects against a corner case in which verifyStreamHygiene could not detect.
   *        This is a bug fix but yet a potential behavior change, so we add a flag to fallback.
   * @param forceEnableUnsafeReadOnNullabilityChange
   *        Flag that allows user to fallback to the legacy behavior in which user can allow
   *        nullable=false schema to read nullable=true data, which is incorrect but a behavior
   *        change regardless.
   * @param isStreamingFromColumnMappingTable
   *        Whether we are streaming from a table with column mapping enabled.
   * @param typeWideningEnabled
   *        Whether we are streaming from a table that has the type widening table feature enabled.
   * @param enableSchemaTrackingForTypeWidening
   *        Whether we should track widening type changes to allow users to accept them and resume
   *        stream processing.
   */
  case class SchemaReadOptions(
      allowUnsafeStreamingReadOnColumnMappingSchemaChanges: Boolean,
      allowUnsafeStreamingReadOnPartitionColumnChanges: Boolean,
      forceEnableStreamingReadOnReadIncompatibleSchemaChangesDuringStreamStart: Boolean,
      forceEnableUnsafeReadOnNullabilityChange: Boolean,
      isStreamingFromColumnMappingTable: Boolean,
      typeWideningEnabled: Boolean,
      enableSchemaTrackingForTypeWidening: Boolean
  )

  /**
   * Validate schema compatibility between data schema and read schema. Checks for read
   * compatibility considering nullability, type widening, missing columns, and partition changes.
   *
   * Returns (isCompatible, isRetryable) where isRetryable is None if validation succeeded.
   */
  def validateBasicSchemaChanges(
      dataSchema: StructType,
      readSchema: StructType,
      newPartitionColumns: Seq[String],
      oldPartitionColumns: Seq[String],
      backfilling: Boolean,
      readOptions: SchemaReadOptions): (Boolean, Option[Boolean]) = {
    // We forbid the case when the the schemaChange is nullable while the read schema is NOT
    // nullable, or in other words, `schema` should not tighten nullability from `schemaChange`,
    // because we don't ever want to read back any nulls when the read schema is non-nullable.
    val shouldForbidTightenNullability = !readOptions.forceEnableUnsafeReadOnNullabilityChange
    // If schema tracking is disabled for type widening, we allow widening type changes to go
    // through without requiring the user to set `allowSourceColumnTypeChange`. The schema change
    // will cause the stream to fail with a retryable exception, and the stream will restart using
    // the new schema.
    val allowWideningTypeChanges = readOptions.typeWideningEnabled &&
        !readOptions.enableSchemaTrackingForTypeWidening
    // If a user is streaming from a column mapping table and enable the unsafe flag to ignore
    // column mapping schema changes, we can allow the standard check to allow missing columns
    // from the read schema in the schema change, because the only case that happens is when
    // user rename/drops column but they don't care so they enabled the flag to unblock.
    // This is only allowed when we are "backfilling", i.e. the stream progress is older than
    // the analyzed table version. Any schema change past the analysis should still throw
    // exception, because additive schema changes MUST be taken into account.
    val shouldAllowMissingColumns = readOptions.isStreamingFromColumnMappingTable &&
        readOptions.allowUnsafeStreamingReadOnColumnMappingSchemaChanges && backfilling

    if (!SchemaUtils.isReadCompatible(
      existingSchema = dataSchema,
      readSchema = readSchema,
      forbidTightenNullability = shouldForbidTightenNullability,
      allowMissingColumns = shouldAllowMissingColumns,
      // When backfilling after a type change, allow processing the data using the new, wider
      // type.
      typeWideningMode = if (allowWideningTypeChanges && backfilling) {
        TypeWideningMode.AllTypeWidening
      } else {
        TypeWideningMode.NoTypeWidening
      },
      newPartitionColumns = newPartitionColumns,
      oldPartitionColumns = oldPartitionColumns
    )) {
      // Only schema change later than the current read snapshot/schema can be retried, in other
      // words, backfills could never be retryable, because we have no way to refresh
      // the latest schema to "catch up" when the schema change happens before than current read
      // schema version.
      // If not backfilling, we do another check to determine retryability, in which we assume
      // we will be reading using this later `schemaChange` back on the current outdated `schema`,
      // and if it works (including that `schemaChange` should not tighten the nullability
      // constraint from `schema`), it is a retryable exception.
      val retryable = !backfilling && SchemaUtils.isReadCompatible(
        existingSchema = readSchema,
        readSchema = dataSchema,
        forbidTightenNullability = shouldForbidTightenNullability,
        // Check for widening type changes that would succeed on retry when we backfill batches.
        typeWideningMode = if (allowWideningTypeChanges) {
          TypeWideningMode.AllTypeWidening
        } else {
          TypeWideningMode.NoTypeWidening
        }
      )
      (false, Some(retryable))
    } else {
      (true, None)
    }
  }
}
