/*
 * Copyright (2025) The Delta Lake Project Authors.
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

package io.delta.spark.internal.v2.catalog

import java.util.Arrays

import io.delta.kernel.engine.{Engine => KernelEngine}
import io.delta.spark.internal.v2.utils.ScalaUtils
import io.delta.spark.internal.v2.utils.ScalaUtils.toJavaOptional
import io.delta.spark.internal.v2.write.DeltaMetadataOnlyDeleteExecutor

import org.apache.spark.sql.delta.{ColumnTypeChangeSupport, Snapshot}
import org.apache.spark.sql.delta.v2.interop.{AbstractMetadata, AbstractProtocol}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.connector.catalog.{SupportsDeleteV2, SupportsSchemaEvolution, TableCapability, TableChange}
import org.apache.spark.sql.connector.expressions.filter.{AlwaysTrue, PartitionPredicate, Predicate}

/**
 * Shim to build the DSv2 Delta table connector against different Spark versions.
 * This is the shim for Spark 4.2:
 *  - Schema evolution support in DSV2 (add columns, change type) was added in Spark 4.2.
 *  - Partition-aware metadata-only DELETE support depends on Spark 4.2's PartitionPredicate.
 */
private[v2] abstract class DeltaV2TableShims extends SupportsSchemaEvolution with SupportsDeleteV2 {

  /** Implemented in DeltaV2Table to provide access to the table protocol/metadata. */
  protected def protocol(): AbstractProtocol
  protected def metadata(): AbstractMetadata
  protected def kernelEngine(): KernelEngine
  protected def initialSnapshot(): Snapshot
  protected def catalogTable(): Option[CatalogTable]

  /**
   * Whether an analyzer-proposed schema-evolution column change (adding a column or widening a
   * column type) can be applied to this Delta table. Adding a column is always allowed; changing a
   * column type delegates to the connector-agnostic [[ColumnTypeChangeSupport]].
   */
  override def supportsColumnChange(change: TableChange.ColumnChange): Boolean = change match {
    case _: TableChange.AddColumn => true
    case typeChange: TableChange.UpdateColumnType =>
      ColumnTypeChangeSupport.supportsTypeChange(
        SparkSession.active,
        protocol(),
        metadata(),
        ScalaUtils.toScalaList(typeChange.fieldNames()),
        typeChange.newDataType())
    case _ => false
  }

  /**
   * A DELETE qualifies for metadata-only execution when every predicate can be evaluated against
   * partition values alone: either a whole-table [[AlwaysTrue]], or a
   * [[PartitionPredicate]] from a partition-column condition.
   */
  override def canDeleteWhere(predicates: Array[Predicate]): Boolean =
    Arrays.stream(predicates).allMatch {
      case _: AlwaysTrue | _: PartitionPredicate => true
      case _ => false
    }

  override def deleteWhere(predicates: Array[Predicate]): Unit =
    DeltaMetadataOnlyDeleteExecutor
      .deleteWhere(kernelEngine(), initialSnapshot(), toJavaOptional(catalogTable()), predicates)
}

object DeltaV2TableShims {

  /** The schema evolution capability to advertise. */
  def schemaEvolutionCapability(): Option[TableCapability] =
    Some(TableCapability.AUTOMATIC_SCHEMA_EVOLUTION)
}
