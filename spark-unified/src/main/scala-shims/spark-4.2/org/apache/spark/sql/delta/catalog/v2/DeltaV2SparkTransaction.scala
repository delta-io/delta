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

package org.apache.spark.sql.delta.catalog.v2

import java.util

import org.apache.spark.sql.delta.catalog.{ChangelogSupport, DeltaCatalogLike}

import org.apache.spark.sql.connector.catalog.{CatalogPlugin, Changelog, ChangelogContext, Column, Identifier, NamespaceChange, StagedTable, Table, TableCatalog, TableCatalogCapability, TableChange}
import org.apache.spark.sql.connector.catalog.TableInfo
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction
import org.apache.spark.sql.connector.catalog.transactions.{Transaction => SparkTransaction, TransactionInfo => SparkTransactionInfo}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.metric.CustomMetric
import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * Catalog-scoped v2 [[SparkTransaction]]. V2 transactions are initialized by catalogs that
 * implement the [[TransactionalCatalogPlugin]] and are managed by Spark.
 */
private[catalog] class DeltaV2SparkTransaction(
    delegate: DeltaCatalogLike,
    val info: SparkTransactionInfo)
  extends SparkTransaction {

  // The transaction-scoped catalog is created along with the transaction.
  private val txnCatalog: DeltaV2SparkTransactionCatalog =
    new DeltaV2SparkTransactionCatalog(delegate)

  override def catalog(): CatalogPlugin = txnCatalog

  override def commit(): Unit = {}
  override def abort(): Unit = {}
  override def close(): Unit = {}

  override def registerScans(scans: Array[Scan]): Boolean = false
}

/**
 * A transaction-scoped catalog. This is the Delta implementation of a core pattern of the v2
 * transaction machinery. The main idea is to create a dedicated catalog instance for each Spark
 * transaction, tied to the lifecycle of that transaction. It is implemented as a decorator of the
 * actual catalog. For now it only provides the wiring: it forwards every call to the delegate.
 * Tracking the tables loaded during the transaction (e.g. table pinning) is going to be added in
 * follow up work.
 *
 * It extends [[DeltaCatalogLike]] (the shared Delta catalog contract) rather than the concrete
 * catalog, so it inherits none of that catalog's instance state. The contract bundles the full
 * catalog surface the wrapper must expose. Thus, the decorator stays type-compatible with the real
 * catalog. A type check the engine performs on a catalog (e.g. `case c: SupportsNamespaces`)
 * resolves the same on the wrapper as on the real catalog. This matters because the transaction
 * machinery substitutes the wrapper for the session catalog by name, so a namespace, function,
 * or staging check must not start failing on it.
 *
 * The contract alone does NOT force the wrapper to forward every call: a method backed by a Java
 * default is silently satisfied by that default. We therefore exhaustively override every method
 * to forward it to `delegate`; the reflection test in DeltaV2CatalogTransactionSuite enforces
 * that none is missed.
 *
 * [[ChangelogSupport]] is mixed in directly (as [[DeltaCatalog]] does), rather than via
 * [[DeltaCatalogLike]]: it is a per-catalog capability the base catalog does not carry.
 * `ResolveTableChangesV2` type-checks the catalog for this interface, so the wrapper must expose
 * it for CDF (`table_changes`) reads to resolve while a transaction's catalog is substituted in.
 */
private[catalog] class DeltaV2SparkTransactionCatalog(delegate: DeltaCatalogLike)
  extends DeltaCatalogLike with TableCatalog with ChangelogSupport {

  override def name(): String = delegate.name()

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit =
    delegate.initialize(name, options)

  override def setDelegateCatalog(sessionCatalog: CatalogPlugin): Unit =
    delegate.setDelegateCatalog(sessionCatalog)

  override def loadTable(ident: Identifier): Table = delegate.loadTable(ident)

  override def loadTable(ident: Identifier, timestamp: Long): Table =
    delegate.loadTable(ident, timestamp)

  override def loadTable(ident: Identifier, version: String): Table =
    delegate.loadTable(ident, version)

  override def createTable(
      ident: Identifier,
      columns: Array[Column],
      partitions: Array[Transform],
      properties: util.Map[String, String]): Table =
    delegate.createTable(ident, columns, partitions, properties)

  override def createTable(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: util.Map[String, String]): Table =
    delegate.createTable(ident, schema, partitions, properties)

  override def createTableLike(
      ident: Identifier,
      tableInfo: TableInfo,
      sourceTable: Table): Table =
    delegate.createTableLike(ident, tableInfo, sourceTable)

  override def stageCreate(
      ident: Identifier,
      columns: Array[Column],
      partitions: Array[Transform],
      properties: util.Map[String, String]): StagedTable =
    delegate.stageCreate(ident, columns, partitions, properties)

  override def stageCreate(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: util.Map[String, String]): StagedTable =
    delegate.stageCreate(ident, schema, partitions, properties)

  override def stageReplace(
      ident: Identifier,
      columns: Array[Column],
      partitions: Array[Transform],
      properties: util.Map[String, String]): StagedTable =
    delegate.stageReplace(ident, columns, partitions, properties)

  override def stageReplace(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: util.Map[String, String]): StagedTable =
    delegate.stageReplace(ident, schema, partitions, properties)

  override def stageCreateOrReplace(
      ident: Identifier,
      columns: Array[Column],
      partitions: Array[Transform],
      properties: util.Map[String, String]): StagedTable =
    delegate.stageCreateOrReplace(ident, columns, partitions, properties)

  override def stageCreateOrReplace(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: util.Map[String, String]): StagedTable =
    delegate.stageCreateOrReplace(ident, schema, partitions, properties)

  override def alterTable(ident: Identifier, changes: TableChange*): Table =
    delegate.alterTable(ident, changes: _*)

  override def tableExists(ident: Identifier): Boolean = delegate.tableExists(ident)

  override def loadChangelog(
      ident: Identifier,
      changelogContext: ChangelogContext,
      options: CaseInsensitiveStringMap): Changelog =
    delegate.loadChangelog(ident, changelogContext, options)

  override def defaultNamespace(): Array[String] = delegate.defaultNamespace()

  override def capabilities(): util.Set[TableCatalogCapability] = delegate.capabilities()

  override def supportedCustomMetrics(): Array[CustomMetric] = delegate.supportedCustomMetrics()

  override def listTables(namespace: Array[String]): Array[Identifier] =
    delegate.listTables(namespace)

  override def invalidateTable(ident: Identifier): Unit = delegate.invalidateTable(ident)

  override def dropTable(ident: Identifier): Boolean = delegate.dropTable(ident)

  override def purgeTable(ident: Identifier): Boolean = delegate.purgeTable(ident)

  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit =
    delegate.renameTable(oldIdent, newIdent)

  override def listNamespaces(): Array[Array[String]] = delegate.listNamespaces()

  override def listNamespaces(namespace: Array[String]): Array[Array[String]] =
    delegate.listNamespaces(namespace)

  override def namespaceExists(namespace: Array[String]): Boolean =
    delegate.namespaceExists(namespace)

  override def loadNamespaceMetadata(namespace: Array[String]): util.Map[String, String] =
    delegate.loadNamespaceMetadata(namespace)

  override def createNamespace(
      namespace: Array[String],
      metadata: util.Map[String, String]): Unit =
    delegate.createNamespace(namespace, metadata)

  override def alterNamespace(namespace: Array[String], changes: NamespaceChange*): Unit =
    delegate.alterNamespace(namespace, changes: _*)

  override def dropNamespace(namespace: Array[String], cascade: Boolean): Boolean =
    delegate.dropNamespace(namespace, cascade)

  override def loadFunction(ident: Identifier): UnboundFunction = delegate.loadFunction(ident)

  override def listFunctions(namespace: Array[String]): Array[Identifier] =
    delegate.listFunctions(namespace)

  override def functionExists(ident: Identifier): Boolean = delegate.functionExists(ident)

}
