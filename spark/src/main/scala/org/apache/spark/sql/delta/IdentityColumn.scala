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

import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.sources.DeltaSourceUtils._
import org.apache.spark.sql.delta.stats.{DeltaFileStatistics, DeltaJobStatisticsTracker}
import org.apache.spark.sql.delta.util.JsonUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.execution.datasources.WriteTaskStats
import org.apache.spark.sql.functions.{array, max, min, to_json}
import org.apache.spark.sql.types.{MetadataBuilder, StructField, StructType}

/**
 * Provide utility methods related to IDENTITY column support for Delta.
 */
object IdentityColumn extends DeltaLogging {
  case class IdentityInfo(start: Long, step: Long, highWaterMark: Option[Long])
  // Default start and step configuration if not specified by user.
  val defaultStart = 1
  val defaultStep = 1
  // Operation types in usage logs.
  // When IDENTITY columns are defined.
  val opTypeDefinition = "delta.identityColumn.definition"
  // When table with IDENTITY columns are written into.
  val opTypeWrite = "delta.identityColumn.write"
  // When IDENTITY column update causes transaction to abort.
  val opTypeAbort = "delta.identityColumn.abort"

  // Return true if `field` is an identity column that allows explicit insert. Caller must ensure
  // `isIdentityColumn(field)` is true.
  def allowExplicitInsert(field: StructField): Boolean = {
    field.metadata.getBoolean(IDENTITY_INFO_ALLOW_EXPLICIT_INSERT)
  }

  // Return all the IDENTITY columns from `schema`.
  def getIdentityColumns(schema: StructType): Seq[StructField] = {
    schema.filter(ColumnWithDefaultExprUtils.isIdentityColumn)
  }

  // Return the number of IDENTITY columns in `schema`.
  private def getNumberOfIdentityColumns(schema: StructType): Int = {
    getIdentityColumns(schema).size
  }

  // Create expression to generate IDENTITY values for the column `field`.
  def createIdentityColumnGenerationExpr(field: StructField): Expression = {
    val info = IdentityColumn.getIdentityInfo(field)
    GenerateIdentityValues(info.start, info.step, info.highWaterMark)
  }

  // Create a column to generate IDENTITY values for the column `field`.
  def createIdentityColumnGenerationExprAsColumn(field: StructField): Column = {
    new Column(createIdentityColumnGenerationExpr(field)).alias(field.name)
  }

  /**
   * Create a stats tracker to collect IDENTITY column high water marks if its values are system
   * generated.
   *
   * @param spark The SparkSession associated with this query.
   * @param hadoopConf The Hadoop configuration object to use on an executor.
   * @param path Root Reservoir path
   * @param schema The schema of the table to be written into.
   * @param statsDataSchema The schema of the output data (this does not include partition columns).
   * @param trackHighWaterMarks Column names for which we should track high water marks.
   * @return The stats tracker.
   */
  def createIdentityColumnStatsTracker(
      spark: SparkSession,
      hadoopConf: Configuration,
      path: Path,
      schema: StructType,
      statsDataSchema: Seq[Attribute],
      trackHighWaterMarks: Set[String]
    ) : Option[DeltaIdentityColumnStatsTracker] = {
    if (trackHighWaterMarks.isEmpty) return None
    val identityColumnInfo = schema
      .filter(f => trackHighWaterMarks.contains(f.name))
      .map(f => DeltaColumnMapping.getPhysicalName(f) ->  // Get identity column physical names
        (f.metadata.getLong(IDENTITY_INFO_STEP) > 0L))
    // We should have found all IDENTITY columns to track high water marks.
    assert(identityColumnInfo.size == trackHighWaterMarks.size,
      s"expect: $trackHighWaterMarks, found (physical names): ${identityColumnInfo.map(_._1)}")
    // Build the expression to collect high water marks of all IDENTITY columns as a single
    // expression. It is essentially a json array containing one max or min aggregate expression
    // for each IDENTITY column.
    //
    // Example: for the following table
    //
    //   CREATE TABLE t1 (
    //     id1 BIGINT GENERATED BY DEFAULT AS IDENTITY (START WITH 1 INCREMENT BY 1),
    //     id2 BIGINT GENERATED BY DEFAULT AS IDENTITY (START WITH 1 INCREMENT BY -1),
    //     value STRING
    //   ) USING delta;
    //
    // The expression will be: to_json(array(max(id1), min(id2)))
    val aggregates = identityColumnInfo.map {
      case (name, positiveStep) =>
        val col = new Column(UnresolvedAttribute.quoted(name))
        if (positiveStep) max(col) else min(col)
    }
    val unresolvedExpr = to_json(array(aggregates: _*))
    // Resolve the collection expression by constructing a query to select the expression from a
    // table with the statsSchema and get the analyzed expression.
    val resolvedExpr = Dataset.ofRows(spark, LocalRelation(statsDataSchema))
      .select(unresolvedExpr).queryExecution.analyzed.expressions.head
    Some(new DeltaIdentityColumnStatsTracker(
      hadoopConf,
      path,
      statsDataSchema,
      resolvedExpr,
      identityColumnInfo
    ))
  }

  /**
   * Return a new schema with IDENTITY high water marks updated in the schema.
   * The new high watermarks are decided based on the `updatedIdentityHighWaterMarks` and old high
   * watermark values present in the passed `schema`.
   */
  def updateSchema(
      schema: StructType,
      updatedIdentityHighWaterMarks: Seq[(String, Long)]) : StructType = {
    val updatedIdentityHighWaterMarksGrouped =
      updatedIdentityHighWaterMarks.groupBy(_._1).mapValues(v => v.map(_._2))
    StructType(schema.map { f =>
      updatedIdentityHighWaterMarksGrouped.get(DeltaColumnMapping.getPhysicalName(f)) match {
        case Some(newWatermarks) if ColumnWithDefaultExprUtils.isIdentityColumn(f) =>
          val oldIdentityInfo = getIdentityInfo(f)
          val positiveStep = oldIdentityInfo.step > 0
          val newHighWaterMark = if (positiveStep) {
            oldIdentityInfo.highWaterMark.map(Math.max(_, newWatermarks.max))
              .getOrElse(newWatermarks.max)
          } else {
            oldIdentityInfo.highWaterMark.map(Math.min(_, newWatermarks.min))
              .getOrElse(newWatermarks.min)
          }
          val builder = new MetadataBuilder()
            .withMetadata(f.metadata)
            .putLong(IDENTITY_INFO_HIGHWATERMARK, newHighWaterMark)
          f.copy(metadata = builder.build())
        case _ =>
          f
      }
    })
  }

  // Block explicitly provided IDENTITY values if column definition does not allow so.
  def blockExplicitIdentityColumnInsert(
      schema: StructType,
      query: LogicalPlan): Unit = {
    val nonInsertableIdentityColumns = schema.filter { f =>
      ColumnWithDefaultExprUtils.isIdentityColumn(f) && !IdentityColumn.allowExplicitInsert(f)
    }.map(_.name)
    blockIdentityColumn(
      nonInsertableIdentityColumns,
      query.output.map(attr => Seq(attr.name)),
      isUpdate = false
    )
  }

  // Block explicitly provided IDENTITY values if column definition does not allow so.
  def blockExplicitIdentityColumnInsert(
      identityColumns: Seq[StructField],
      insertedColNameParts: Seq[Seq[String]]): Unit = {
    val nonInsertableIdentityColumns = identityColumns
      .filter(!allowExplicitInsert(_))
      .map(_.name)
    blockIdentityColumn(
      nonInsertableIdentityColumns,
      insertedColNameParts,
      isUpdate = false)
  }

  // Block updating IDENTITY columns.
  def blockIdentityColumnUpdate(
      schema: StructType,
      updatedColNameParts: Seq[Seq[String]]): Unit = {
    blockIdentityColumnUpdate(getIdentityColumns(schema), updatedColNameParts)
  }

  // Block updating IDENTITY columns.
  def blockIdentityColumnUpdate(
      identityColumns: Seq[StructField],
      updatedColNameParts: Seq[Seq[String]]): Unit = {
    blockIdentityColumn(
      identityColumns.map(_.name),
      updatedColNameParts,
      isUpdate = true)
  }

  def logTableCreation(deltaLog: DeltaLog, schema: StructType): Unit = {
    val numIdentityColumns = getNumberOfIdentityColumns(schema)
    if (numIdentityColumns != 0) {
      recordDeltaEvent(
        deltaLog,
        opTypeDefinition,
        data = Map(
          "numIdentityColumns" -> numIdentityColumns
        )
      )
    }
  }

  def logTableWrite(
      snapshot: Snapshot,
      generatedIdentityColumns: Set[String],
      numInsertedRowsOpt: Option[Long]): Unit = {
    val identityColumns = getIdentityColumns(snapshot.schema)
    if (identityColumns.nonEmpty) {
      val explicitIdentityColumns = identityColumns.filter {
        f => !generatedIdentityColumns.contains(f.name)
      }.map(_.name)
      recordDeltaEvent(
        snapshot.deltaLog,
        opTypeWrite,
        data = Map(
          "numInsertedRows" -> numInsertedRowsOpt,
          "generatedIdentityColumnNames" -> generatedIdentityColumns.mkString(","),
          "generatedIdentityColumnCount" -> generatedIdentityColumns.size,
          "explicitIdentityColumnNames" -> explicitIdentityColumns.mkString(","),
          "explicitIdentityColumnCount" -> explicitIdentityColumns.size
        )
      )
    }
  }

  def logTransactionAbort(deltaLog: DeltaLog): Unit = {
    recordDeltaEvent(deltaLog, opTypeAbort)
  }

  // Calculate the sync'ed IDENTITY high water mark based on actual data and returns a
  // potentially updated `StructField`.
  def syncIdentity(field: StructField, df: DataFrame): StructField = {
    // Round `value` to the next value that follows start and step configuration.
    def roundToNext(start: Long, step: Long, value: Long): Long = {
      if (Math.subtractExact(value, start) % step == 0) {
        value
      } else {
        // start + step * ((value - start) / step + 1)
        Math.addExact(
          Math.multiplyExact(Math.addExact(Math.subtractExact(value, start) / step, 1), step),
          start)
      }
    }

    assert(ColumnWithDefaultExprUtils.isIdentityColumn(field))
    // Run a query to get the actual high water mark (max or min value of the IDENTITY column) from
    // the actual data.
    val info = getIdentityInfo(field)
    val positiveStep = info.step > 0
    val expr = if (positiveStep) max(field.name) else min(field.name)
    val resultRow = df.select(expr).collect().head

    if (!resultRow.isNullAt(0)) {
      val result = resultRow.getLong(0)
      val isBeforeStart = if (positiveStep) result < info.start else result > info.start
      val newHighWaterMark = roundToNext(info.start, info.step, result)
      if (isBeforeStart || info.highWaterMark.contains(newHighWaterMark)) {
        field
      } else {
        val newMetadata = new MetadataBuilder().withMetadata(field.metadata)
          .putLong(IDENTITY_INFO_HIGHWATERMARK, newHighWaterMark)
          .build()
        field.copy(metadata = newMetadata)
      }
    } else {
      field
    }
  }

  /**
   * Returns a copy of `schemaToCopy` in which the high water marks of the identity columns have
   * been merged with the corresponding high water marks of `schemaWithHighWaterMarksToMerge`.
   */
  def copySchemaWithMergedHighWaterMarks(
      schemaToCopy: StructType, schemaWithHighWaterMarksToMerge: StructType): StructType = {
    val newHighWatermarks = getIdentityColumns(schemaWithHighWaterMarksToMerge).flatMap { f =>
      val info = getIdentityInfo(f)
      info.highWaterMark.map(waterMark => DeltaColumnMapping.getPhysicalName(f) -> waterMark)
    }
    updateSchema(schemaToCopy, newHighWatermarks)
  }


  // Check `colNameParts` does not contain any column from `columnNamesToBlock`.
  private def blockIdentityColumn(
      columnNamesToBlock: Seq[String],
      colNameParts: Seq[Seq[String]],
      isUpdate: Boolean): Unit = {
    if (columnNamesToBlock.nonEmpty) {
      val resolver = SparkSession.active.sessionState.analyzer.resolver
      for (namePart <- colNameParts) {
        // IDENTITY column cannot be nested columns, so we only need to check top level columns.
        if (namePart.size == 1) {
          val colName = namePart.head
          if (columnNamesToBlock.exists(resolver(_, colName))) {
            if (isUpdate) {
              throw DeltaErrors.identityColumnUpdateNotSupported(colName)
            } else {
              throw DeltaErrors.identityColumnExplicitInsertNotSupported(colName)
            }
          }
        }
      }
    }
  }

  // Return IDENTITY information of column `field`. Caller must ensure `isIdentityColumn(field)`
  // is true.
  def getIdentityInfo(field: StructField): IdentityInfo = {
    val md = field.metadata
    val start = md.getLong(IDENTITY_INFO_START)
    val step = md.getLong(IDENTITY_INFO_STEP)
    // If system hasn't generated IDENTITY values for this column (either it hasn't been
    // inserted into, or every inserts provided values for this IDENTITY column), high water mark
    // field will not present in column metadata. In this case, high water mark will be set to
    // (start - step) so that the first value generated is start (high water mark + step).
    val highWaterMark = if (md.contains(IDENTITY_INFO_HIGHWATERMARK)) {
        Some(md.getLong(IDENTITY_INFO_HIGHWATERMARK))
      } else {
        None
      }
    IdentityInfo(start, step, highWaterMark)
  }
}

/**
 * Stats tracker for IDENTITY column high water marks. The only difference between this class and
 * `DeltaJobStatisticsTracker` is how the stats are aggregated on the driver.
 *
 * @param hadoopConf The Hadoop configuration object to use on an executor.
 * @param path Root Reservoir path
 * @param dataCols Resolved data (i.e. non-partitionBy) columns of the dataframe to be written.
 * @param statsColExpr The expression to collect high water marks.
 * @param identityColumnInfo Information of IDENTITY columns. It contains a pair of column name
 *                           and whether it has a positive step for each IDENTITY column.
 */
class DeltaIdentityColumnStatsTracker(
    @transient private val hadoopConf: Configuration,
    @transient path: Path,
    dataCols: Seq[Attribute],
    statsColExpr: Expression,
    val identityColumnInfo: Seq[(String, Boolean)]
  )
  extends DeltaJobStatisticsTracker(
    hadoopConf,
    path,
    dataCols,
    statsColExpr
  ) {

  // Map of column name to its corresponding collected high water mark.
  var highWaterMarks = scala.collection.mutable.Map[String, Long]()

  // Process the stats on the driver. In `stats` we have a sequence of `DeltaFileStatistics`,
  // whose stats is a map of file path to its corresponding array of high water marks in json.
  override def processStats(stats: Seq[WriteTaskStats], jobCommitTime: Long): Unit = {
    stats.map(_.asInstanceOf[DeltaFileStatistics]).flatMap(_.stats).map {
      case (_, statsString) =>
        val fileHighWaterMarks = JsonUtils.fromJson[Array[Long]](statsString)
        // We must have high water marks collected for all IDENTITY columns and we have guaranteed
        // that their orders in the array follow the orders in `identityInfo` by aligning the
        // order of expression and `identityColumnInfo` in `createIdentityColumnStatsTracker`.
        require(fileHighWaterMarks.size == identityColumnInfo.size)
        identityColumnInfo.zip(fileHighWaterMarks).map {
          case ((name, positiveStep), value) =>
            val updated = highWaterMarks.get(name).map { v =>
              if (positiveStep) v.max(value) else v.min(value)
            }.getOrElse(value)
            highWaterMarks.update(name, updated)
        }
    }
  }
}
