/*
 * Copyright (2026) The Delta Lake Project Authors.
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

package org.apache.spark.sql.delta.catalog

import java.util
import java.util.Locale

import scala.collection.JavaConverters._

import io.delta.kernel.data.Row
import io.delta.kernel.internal.actions.{AddFile => KernelAddFile, SingleAction}
import io.delta.kernel.internal.data.GenericRow
import io.delta.kernel.internal.util.VectorUtils
import io.delta.kernel.utils.CloseableIterable
import io.delta.spark.internal.v2.catalog.CreateTableCommitCoordinator
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.connector.catalog.{Identifier, StagedTable, SupportsWrite, TableCapability}
import org.apache.spark.sql.connector.catalog.TableCapability.{TRUNCATE, V1_BATCH_WRITE}
import org.apache.spark.sql.connector.catalog.TableCatalog
import org.apache.spark.sql.connector.expressions.{FieldReference, IdentityTransform, Transform}
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, SupportsTruncate, V1Write, WriteBuilder}
import org.apache.spark.sql.delta.DeltaErrors
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.DeltaOptions
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.commands.WriteIntoDelta
import org.apache.spark.sql.delta.DeltaTableUtils
import org.apache.spark.sql.sources.InsertableRelation
import org.apache.spark.sql.types.StructType

/**
 * Staged table used by the CTAS POC path:
 * - DSv1 writer generates parquet files and AddFile actions.
 * - AddFile actions are adapted to Kernel SingleAction rows.
 * - Final CREATE TABLE v0 commit happens via DSv2/Kernel coordinator.
 */
class KernelBackfilledStagedCreateTable(
    ident: Identifier,
    override val schema: StructType,
    partitions: Array[Transform],
    override val properties: util.Map[String, String],
    spark: SparkSession,
    catalogName: String,
    engineInfo: String,
    isPathIdentifier: Boolean) extends StagedTable with SupportsWrite {

  private var asSelectQuery: Option[DataFrame] = None
  private var writeOptions: Map[String, String] = Map.empty

  override def partitioning(): Array[Transform] = partitions

  override def commitStagedChanges(): Unit = {
    val (baseTableProperties, sqlWriteOptions) =
      KernelBackfilledStagedCreateTable.getTablePropsAndWriteOptions(properties)
    if (writeOptions.isEmpty && sqlWriteOptions.nonEmpty) {
      writeOptions = sqlWriteOptions
    }
    val tableProperties = KernelBackfilledStagedCreateTable.expandDeltaWriteOptionsToTableProps(
      baseTableProperties,
      writeOptions,
      spark)

    val dataActions = asSelectQuery match {
      case Some(df) =>
        KernelBackfilledStagedCreateTable.buildDataActionsWithDsv1Writer(
          spark,
          ident,
          schema,
          partitions,
          tableProperties,
          writeOptions,
          isPathIdentifier,
          df)
      case None =>
        CloseableIterable.emptyIterable[Row]()
    }

    CreateTableCommitCoordinator.commitCreateTableVersion0(
      ident,
      schema,
      partitions,
      tableProperties.asJava,
      spark,
      catalogName,
      engineInfo,
      isPathIdentifier,
      dataActions)
  }

  override def name(): String = ident.name()

  override def abortStagedChanges(): Unit = {}

  override def capabilities(): util.Set[TableCapability] =
    Set(V1_BATCH_WRITE, TRUNCATE).asJava

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    writeOptions = info.options().asCaseSensitiveMap().asScala.toMap
    new KernelBackfilledWriteBuilder
  }

  private class KernelBackfilledWriteBuilder extends WriteBuilder with SupportsTruncate {
    override def truncate(): this.type = this
    override def build(): V1Write = new V1Write {
      override def toInsertableRelation(): InsertableRelation = {
        new InsertableRelation {
          override def insert(data: DataFrame, overwrite: Boolean): Unit = {
            asSelectQuery = Option(data)
          }
        }
      }
    }
  }
}

object KernelBackfilledStagedCreateTable {

  private def getTablePropsAndWriteOptions(
      properties: util.Map[String, String]): (Map[String, String], Map[String, String]) = {
    val allProps = properties.asScala.toMap
    val optionsThroughProperties = allProps.collect {
      case (k, _) if k.startsWith(TableCatalog.OPTION_PREFIX) =>
        k.stripPrefix(TableCatalog.OPTION_PREFIX)
    }.toSet
    val tableProps = allProps.collect {
      case (k, v) if !k.startsWith(TableCatalog.OPTION_PREFIX) &&
          !optionsThroughProperties.contains(k) =>
        k -> v
    }
    val writeOptions = allProps.collect {
      case (k, v) if optionsThroughProperties.contains(k) => k -> v
    }
    (tableProps, writeOptions)
  }

  private def expandDeltaWriteOptionsToTableProps(
      tableProps: Map[String, String],
      writeOptions: Map[String, String],
      spark: SparkSession): Map[String, String] = {
    val expanded = scala.collection.mutable.Map.empty[String, String] ++ tableProps
    if (spark.sessionState.conf.getConf(
      org.apache.spark.sql.delta.sources.DeltaSQLConf.DELTA_LEGACY_STORE_WRITER_OPTIONS_AS_PROPS
    )) {
      writeOptions.foreach { case (key, value) => expanded.update(key, value) }
    } else {
      writeOptions.foreach { case (key, value) =>
        if (key.toLowerCase(Locale.ROOT).startsWith("delta.")) {
          expanded.update(key, value)
        }
      }
    }
    expanded.toMap
  }

  private def buildDataActionsWithDsv1Writer(
      spark: SparkSession,
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      tableProperties: Map[String, String],
      writeOptions: Map[String, String],
      isPathIdentifier: Boolean,
      sourceQuery: DataFrame): CloseableIterable[Row] = {
    val location = resolveLocation(ident, tableProperties, isPathIdentifier)
    val fsOptions = (tableProperties ++ writeOptions).filter { case (key, _) =>
      DeltaTableUtils.validDeltaTableHadoopPrefixes.exists(prefix => key.startsWith(prefix))
    }
    val deltaLog = DeltaLog.forTable(spark, new Path(location), fsOptions)
    val writer = WriteIntoDelta(
      deltaLog = deltaLog,
      mode = SaveMode.ErrorIfExists,
      options = new DeltaOptions(writeOptions, spark.sessionState.conf),
      partitionColumns = toPartitionColumns(partitions),
      configuration = tableProperties,
      data = sourceQuery,
      catalogTableOpt = None,
      schemaInCatalog = Some(schema))

    val addFiles = deltaLog.withNewTransaction(catalogTableOpt = None) { txn =>
      val stagedActions = writer.writeAndReturnCommitData(txn, spark).actions
      stagedActions.collect { case add: AddFile => add }
    }

    val kernelActions = addFiles.map(addFile => toKernelAddFileSingleAction(addFile))
    CloseableIterable.inMemoryIterable(
      io.delta.kernel.internal.util.Utils.toCloseableIterator(kernelActions.iterator.asJava))
  }

  private def resolveLocation(
      ident: Identifier,
      tableProperties: Map[String, String],
      isPathIdentifier: Boolean): String = {
    tableProperties
      .get(TableCatalog.PROP_LOCATION)
      .orElse(tableProperties.get("location"))
      .orElse(if (isPathIdentifier) Some(ident.name()) else None)
      .getOrElse {
        throw new IllegalArgumentException(s"Unable to resolve location for CREATE TABLE $ident")
      }
  }

  private def toPartitionColumns(partitions: Array[Transform]): Seq[String] = {
    partitions.toSeq.map {
      case IdentityTransform(FieldReference(Seq(column))) => column
      case _ =>
        throw DeltaErrors.operationNotSupportedException("Partitioning by expressions")
    }
  }

  private def toKernelAddFileSingleAction(addFile: AddFile): Row = {
    val addValueMap = new util.HashMap[Integer, AnyRef]()
    addValueMap.put(KernelAddFile.FULL_SCHEMA.indexOf("path"), addFile.path)
    addValueMap.put(
      KernelAddFile.FULL_SCHEMA.indexOf("partitionValues"),
      VectorUtils.stringStringMapValue(Option(addFile.partitionValues).getOrElse(Map.empty).asJava))
    addValueMap.put(KernelAddFile.FULL_SCHEMA.indexOf("size"), Long.box(addFile.size))
    addValueMap.put(
      KernelAddFile.FULL_SCHEMA.indexOf("modificationTime"),
      Long.box(addFile.modificationTime))
    addValueMap.put(
      KernelAddFile.FULL_SCHEMA.indexOf("dataChange"),
      Boolean.box(addFile.dataChange))
    Option(addFile.stats).foreach { stats =>
      addValueMap.put(KernelAddFile.FULL_SCHEMA.indexOf("stats"), stats)
    }
    Option(addFile.tags).foreach { tags =>
      addValueMap.put(
        KernelAddFile.FULL_SCHEMA.indexOf("tags"),
        VectorUtils.stringStringMapValue(tags.asJava))
    }
    addFile.baseRowId.foreach { rowId =>
      addValueMap.put(KernelAddFile.FULL_SCHEMA.indexOf("baseRowId"), Long.box(rowId))
    }
    addFile.defaultRowCommitVersion.foreach { commitVersion =>
      addValueMap.put(
        KernelAddFile.FULL_SCHEMA.indexOf("defaultRowCommitVersion"),
        Long.box(commitVersion))
    }
    val addRow = new GenericRow(KernelAddFile.FULL_SCHEMA, addValueMap)
    SingleAction.createAddFileSingleAction(addRow)
  }
}
