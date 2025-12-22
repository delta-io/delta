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

package org.apache.spark.sql.delta.commands

// scalastyle:off import.ordering.noEmptyLine
import org.apache.spark.sql.delta.{DeltaErrors, DeltaTableIdentifier, DeltaTimeTravelSpec, Snapshot, UnresolvedDeltaPathOrIdentifier}
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.delta.metering.DeltaLogging

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{MultiInstanceRelation, ResolvedTable}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project, UnaryNode}
import org.apache.spark.sql.catalyst.types.DataTypeUtils.toAttributes
import org.apache.spark.sql.connector.catalog.V1Table
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object DescribeDeltaTable {
  val COMMAND_NAME = "DESCRIBE TABLE"

  /**
   * Schema for the DESCRIBE TABLE output.
   * Similar to Spark's DESCRIBE TABLE but with additional Delta-specific information.
   */
  val schema: StructType = StructType(Seq(
    StructField("col_name", StringType, nullable = false),
    StructField("data_type", StringType, nullable = false),
    StructField("comment", StringType, nullable = true)
  ))

  /**
   * Schema for the DESCRIBE TABLE EXTENDED output.
   */
  val extendedSchema: StructType = schema

  /**
   * Create an unresolved DescribeDeltaTable from path or table identifier.
   */
  def apply(
      path: Option[String],
      tableIdentifier: Option[TableIdentifier],
      isExtended: Boolean,
      timeTravelSpec: Option[DeltaTimeTravelSpec]): DescribeDeltaTable = {
    val plan = UnresolvedDeltaPathOrIdentifier(path, tableIdentifier, COMMAND_NAME)
    DescribeDeltaTable(plan, isExtended, timeTravelSpec)
  }
}

/**
 * A logical placeholder for describing a Delta table's schema at a specific version.
 * Replaced with `DescribeDeltaTableCommand` during planning.
 *
 * @param child The logical plan representing the table to describe.
 * @param isExtended Whether to show extended information (partitioning, properties, etc.).
 * @param timeTravelSpec The time travel specification for the version/timestamp to describe.
 */
case class DescribeDeltaTable(
    override val child: LogicalPlan,
    isExtended: Boolean,
    timeTravelSpec: Option[DeltaTimeTravelSpec],
    override val output: Seq[Attribute] = toAttributes(DescribeDeltaTable.schema))
  extends UnaryNode
    with MultiInstanceRelation
    with DeltaCommand {

  override def newInstance(): LogicalPlan = copy(output = output.map(_.newInstance()))

  override def withNewChildInternal(newChild: LogicalPlan): LogicalPlan = copy(child = newChild)

  /**
   * Define this operator as having no attributes provided by children in order to prevent column
   * pruning from trying to insert projections above the source relation.
   */
  override lazy val references: AttributeSet = AttributeSet.empty
  override def inputSet: AttributeSet = AttributeSet.empty
  assert(!child.isInstanceOf[Project],
    s"The child operator of DescribeDeltaTable must not contain any projection: $child")

  /** Converts this operator into an executable command. */
  def toCommand: DescribeDeltaTableCommand = {
    val deltaTableV2: DeltaTableV2 = child match {
      case ResolvedTable(_, _, d: DeltaTableV2, _) =>
        // Apply time travel if specified
        timeTravelSpec match {
          case Some(spec) => d.copy(timeTravelOpt = Some(spec))
          case None => d
        }
      case ResolvedTable(_, _, t: V1Table, _)
          if org.apache.spark.sql.delta.DeltaTableUtils.isDeltaTable(t.catalogTable) =>
        val spark = SparkSession.active
        val tableV2 = DeltaTableV2(
          spark,
          new org.apache.hadoop.fs.Path(t.v1Table.location),
          Some(t.v1Table))
        timeTravelSpec match {
          case Some(spec) => tableV2.copy(timeTravelOpt = Some(spec))
          case None => tableV2
        }
      case _ =>
        throw DeltaErrors.notADeltaTableException(DescribeDeltaTable.COMMAND_NAME)
    }
    DescribeDeltaTableCommand(
      table = deltaTableV2,
      isExtended = isExtended,
      timeTravelSpec = timeTravelSpec,
      output = output)
  }
}

/**
 * A command for describing the schema and metadata of a Delta table at a specific version.
 *
 * @param table The DeltaTableV2 to describe.
 * @param isExtended Whether to show extended information.
 * @param timeTravelSpec The time travel specification for the version/timestamp to describe.
 */
case class DescribeDeltaTableCommand(
    @transient table: DeltaTableV2,
    isExtended: Boolean,
    timeTravelSpec: Option[DeltaTimeTravelSpec],
    override val output: Seq[Attribute] = toAttributes(DescribeDeltaTable.schema))
  extends LeafRunnableCommand
    with MultiInstanceRelation
    with DeltaLogging {

  override def newInstance(): LogicalPlan = copy(output = output.map(_.newInstance()))

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val deltaLog = table.deltaLog
    recordDeltaOperation(deltaLog, "delta.ddl.describeTable") {
      if (!deltaLog.tableExists) {
        throw DeltaErrors.notADeltaTableException(
          DescribeDeltaTable.COMMAND_NAME,
          DeltaTableIdentifier(path = Some(table.path.toString))
        )
      }

      // Get the snapshot at the specified version/timestamp
      val snapshot = table.initialSnapshot
      val versionInfo = timeTravelSpec match {
        case Some(spec) =>
          val versionStr = spec.version.map(v => s"version $v")
            .orElse(spec.timestamp.map(t => s"timestamp ${t.toString}"))
            .getOrElse("")
          Some(s"Table version: $versionStr")
        case None => None
      }

      describeSchema(snapshot, versionInfo)
    }
  }

  /**
   * Describes the schema of a Delta table.
   */
  private def describeSchema(snapshot: Snapshot, versionInfo: Option[String]): Seq[Row] = {
    val schema = snapshot.schema
    val metadata = snapshot.metadata

    // Build rows for each column in the schema
    val columnRows = schema.fields.map { field =>
      Row(field.name, field.dataType.simpleString, field.getComment().orNull)
    }

    if (isExtended) {
      val partitionRows = if (metadata.partitionColumns.nonEmpty) {
        Seq(
          Row("", "", ""),
          Row("# Partitioning", "", ""),
          Row("Part 0", metadata.partitionColumns.mkString(", "), "")
        )
      } else {
        Seq.empty
      }

      val detailRows = Seq(
        Row("", "", ""),
        Row("# Detailed Table Information", "", ""),
        Row("Name", metadata.name, ""),
        Row("Description", Option(metadata.description).getOrElse(""), ""),
        Row("Location", table.path.toString, ""),
        Row("Provider", "delta", ""),
        Row("Table Version", snapshot.version.toString, "")
      )

      // Add version info if time-traveling
      val versionRow = versionInfo.map(info => Row("Time Travel", info, "")).toSeq

      // Add table properties
      val propertiesRows = if (metadata.configuration.nonEmpty) {
        Row("", "", "") +:
        Row("# Table Properties", "", "") +:
        metadata.configuration.toSeq.sortBy(_._1).map { case (key, value) =>
          Row(key, value, "")
        }
      } else {
        Seq.empty
      }

      columnRows ++ partitionRows ++ detailRows ++ versionRow ++ propertiesRows
    } else {
      columnRows.toSeq
    }
  }
}

