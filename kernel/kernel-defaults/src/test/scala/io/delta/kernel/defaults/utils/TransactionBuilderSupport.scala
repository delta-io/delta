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
package io.delta.kernel.defaults.utils

import scala.collection.JavaConverters._
import scala.collection.immutable.Seq

import io.delta.kernel.{Operation, Table, TableManager, Transaction, TransactionBuilder}
import io.delta.kernel.engine.Engine
import io.delta.kernel.expressions.Column
import io.delta.kernel.internal.{SnapshotImpl, TableImpl}
import io.delta.kernel.internal.tablefeatures.TableFeatures
import io.delta.kernel.internal.util.Clock
import io.delta.kernel.transaction.DataLayoutSpec
import io.delta.kernel.types.StructType

/**
 * Test helper contract for constructing and configuring Delta Kernel transactions.
 */
trait TransactionBuilderSupport {

  // scalastyle:off argcount
  def getCreateTxn(
      engine: Engine,
      tablePath: String,
      schema: StructType,
      partCols: Seq[String] = null,
      tableProperties: Map[String, String] = null,
      clock: Clock = () => System.currentTimeMillis,
      withDomainMetadataSupported: Boolean = false,
      maxRetries: Int = -1,
      clusteringColsOpt: Option[List[Column]] = None): Transaction

  def getUpdateTxn(
      engine: Engine,
      tablePath: String,
      schema: StructType = null,
      tableProperties: Map[String, String] = null,
      clock: Clock = () => System.currentTimeMillis,
      withDomainMetadataSupported: Boolean = false,
      maxRetries: Int = -1,
      clusteringColsOpt: Option[List[Column]] = None,
      logCompactionInterval: Int = 10,
      txnId: Option[(String, Long)] = None,
      tablePropertiesRemoved: Set[String] = null): Transaction
  // scalastyle:on argcount

  def getReplaceTxn(
      engine: Engine,
      tablePath: String,
      schema: StructType,
      partCols: Seq[String] = null,
      clusteringColsOpt: Option[Seq[Column]] = None,
      tableProperties: Map[String, String] = null,
      withDomainMetadataSupported: Boolean = false,
      maxRetries: Int = -1): Transaction
}

/** An implementation of [[TransactionBuilderSupport]] that uses the V1 transaction builder. */
trait TransactionBuilderV1Support extends TransactionBuilderSupport with TestUtils {

  // scalastyle:off argcount
  override def getCreateTxn(
      engine: Engine,
      tablePath: String,
      schema: StructType,
      partCols: Seq[String] = null,
      tableProperties: Map[String, String] = null,
      clock: Clock = () => System.currentTimeMillis,
      withDomainMetadataSupported: Boolean = false,
      maxRetries: Int = -1,
      clusteringColsOpt: Option[List[Column]] = None): Transaction = {
    // scalastyle:on argcount
    var txnBuilder = TableImpl.forPath(engine, tablePath, clock)
      .createTransactionBuilder(engine, "test-engine", Operation.CREATE_TABLE)
      .withSchema(engine, schema)
    if (partCols != null) {
      txnBuilder = txnBuilder.withPartitionColumns(engine, partCols.asJava)
    }
    if (tableProperties != null) {
      txnBuilder = txnBuilder.withTableProperties(engine, tableProperties.asJava)
    }
    if (withDomainMetadataSupported) {
      txnBuilder = txnBuilder.withDomainMetadataSupported()
    }
    if (maxRetries >= 0) {
      txnBuilder = txnBuilder.withMaxRetries(maxRetries)
    }
    if (clusteringColsOpt.isDefined) {
      txnBuilder = txnBuilder.withClusteringColumns(engine, clusteringColsOpt.get.asJava)
    }
    txnBuilder.build(engine)
  }

  // scalastyle:off argcount
  override def getUpdateTxn(
      engine: Engine,
      tablePath: String,
      schema: StructType = null,
      tableProperties: Map[String, String] = null,
      clock: Clock = () => System.currentTimeMillis,
      withDomainMetadataSupported: Boolean = false,
      maxRetries: Int = -1,
      clusteringColsOpt: Option[List[Column]] = None,
      logCompactionInterval: Int = 10,
      txnId: Option[(String, Long)] = None,
      tablePropertiesRemoved: Set[String] = null): Transaction = {
    // scalastyle:on argcount
    var txnBuilder = TableImpl.forPath(engine, tablePath, clock)
      .createTransactionBuilder(engine, "test-engine", Operation.WRITE)
    if (schema != null) {
      txnBuilder = txnBuilder.withSchema(engine, schema)
    }
    if (tableProperties != null) {
      txnBuilder = txnBuilder.withTableProperties(engine, tableProperties.asJava)
    }
    if (withDomainMetadataSupported) {
      txnBuilder = txnBuilder.withDomainMetadataSupported()
    }
    if (maxRetries >= 0) {
      txnBuilder = txnBuilder.withMaxRetries(maxRetries)
    }
    if (clusteringColsOpt.isDefined) {
      txnBuilder = txnBuilder.withClusteringColumns(engine, clusteringColsOpt.get.asJava)
    }
    txnBuilder = txnBuilder.withLogCompactionInverval(logCompactionInterval)
    txnId.foreach { case (appId, txnVer) =>
      txnBuilder = txnBuilder.withTransactionId(engine, appId, txnVer)
    }
    if (tablePropertiesRemoved != null) {
      txnBuilder = txnBuilder.withTablePropertiesRemoved(tablePropertiesRemoved.asJava)
    }
    txnBuilder.build(engine)
  }

  override def getReplaceTxn(
      engine: Engine,
      tablePath: String,
      schema: StructType,
      partCols: Seq[String] = null,
      clusteringColsOpt: Option[Seq[Column]] = None,
      tableProperties: Map[String, String] = null,
      withDomainMetadataSupported: Boolean = false,
      maxRetries: Int = -1): Transaction = {
    var txnBuilder = Table.forPath(engine, tablePath).asInstanceOf[TableImpl]
      .createReplaceTableTransactionBuilder(engine, "test-engine")
      .withSchema(engine, schema)
    if (partCols != null) {
      txnBuilder = txnBuilder.withPartitionColumns(engine, partCols.asJava)
    }
    if (tableProperties != null) {
      txnBuilder = txnBuilder.withTableProperties(engine, tableProperties.asJava)
    }
    if (withDomainMetadataSupported) {
      txnBuilder = txnBuilder.withDomainMetadataSupported()
    }
    clusteringColsOpt.foreach { cols =>
      txnBuilder = txnBuilder.withClusteringColumns(engine, cols.asJava)
    }
    if (maxRetries >= 0) {
      txnBuilder = txnBuilder.withMaxRetries(maxRetries)
    }
    txnBuilder.build(engine)
  }
}

/** An implementation of [[TransactionBuilderSupport]] that uses the V2 transaction builder. */
trait TransactionBuilderV2Support extends TransactionBuilderSupport with TestUtils {

  // scalastyle:off argcount
  override def getCreateTxn(
      engine: Engine,
      tablePath: String,
      schema: StructType,
      partCols: Seq[String] = null,
      tableProperties: Map[String, String] = null,
      clock: Clock = () => System.currentTimeMillis,
      withDomainMetadataSupported: Boolean = false,
      maxRetries: Int = -1,
      clusteringColsOpt: Option[List[Column]] = None): Transaction = {
    // scalastyle:on argcount
    var txnBuilder = TableManager.buildCreateTableTransaction(
      tablePath,
      schema,
      "test-engine")
    if (partCols != null) {
      txnBuilder = txnBuilder.withDataLayoutSpec(
        DataLayoutSpec.partitioned(partCols.map(new Column(_)).asJava))
    }
    val completeTblProps =
      tblPropertiesWithDomainMetadata(tableProperties, withDomainMetadataSupported)
    if (completeTblProps != null) {
      txnBuilder = txnBuilder.withTableProperties(completeTblProps.asJava)
    }
    if (clusteringColsOpt.nonEmpty) {
      txnBuilder = txnBuilder.withDataLayoutSpec(
        DataLayoutSpec.clustered(clusteringColsOpt.get.asJava))
    }
    if (maxRetries >= 0) {
      txnBuilder = txnBuilder.withMaxRetries(maxRetries)
    }
    txnBuilder.build(engine)
  }

  // scalastyle:off argcount
  override def getUpdateTxn(
      engine: Engine,
      tablePath: String,
      schema: StructType = null,
      tableProperties: Map[String, String] = null,
      clock: Clock = () => System.currentTimeMillis,
      withDomainMetadataSupported: Boolean = false,
      maxRetries: Int = -1,
      clusteringColsOpt: Option[List[Column]] = None,
      logCompactionInterval: Int = 10,
      txnId: Option[(String, Long)] = None,
      tablePropertiesRemoved: Set[String] = null): Transaction = {
    // scalastyle:on argcount
    var txnBuilder = TableManager.loadSnapshot(tablePath)
      .build(engine)
      .buildUpdateTableTransaction("test-engine", Operation.WRITE)
    if (schema != null) {
      txnBuilder = txnBuilder.withUpdatedSchema(schema)
    }
    clusteringColsOpt.foreach { clusteringCols =>
      txnBuilder = txnBuilder.withClusteringColumns(clusteringCols.asJava)
    }
    val completeTblProps =
      tblPropertiesWithDomainMetadata(tableProperties, withDomainMetadataSupported)
    if (completeTblProps != null) {
      txnBuilder = txnBuilder.withTablePropertiesAdded(completeTblProps.asJava)
    }
    if (maxRetries >= 0) {
      txnBuilder = txnBuilder.withMaxRetries(maxRetries)
    }
    txnBuilder = txnBuilder.withLogCompactionInterval(logCompactionInterval)
    txnId.foreach { case (appId, txnVer) =>
      txnBuilder = txnBuilder.withTransactionId(appId, txnVer)
    }
    if (tablePropertiesRemoved != null) {
      txnBuilder = txnBuilder.withTablePropertiesRemoved(tablePropertiesRemoved.asJava)
    }
    txnBuilder.build(engine)
  }

  override def getReplaceTxn(
      engine: Engine,
      tablePath: String,
      schema: StructType,
      partCols: Seq[String] = null,
      clusteringColsOpt: Option[Seq[Column]] = None,
      tableProperties: Map[String, String] = null,
      withDomainMetadataSupported: Boolean = false,
      maxRetries: Int = -1): Transaction = {
    var txnBuilder = TableManager.loadSnapshot(tablePath)
      .build(engine).asInstanceOf[SnapshotImpl]
      .buildReplaceTableTransaction(schema, "test-engine")
    if (partCols != null) {
      txnBuilder = txnBuilder.withDataLayoutSpec(
        DataLayoutSpec.partitioned(partCols.map(new Column(_)).asJava))
    }
    val completeTblProps =
      tblPropertiesWithDomainMetadata(tableProperties, withDomainMetadataSupported)
    if (completeTblProps != null) {
      txnBuilder = txnBuilder.withTableProperties(completeTblProps.asJava)
    }
    if (clusteringColsOpt.nonEmpty) {
      txnBuilder = txnBuilder.withDataLayoutSpec(
        DataLayoutSpec.clustered(clusteringColsOpt.get.asJava))
    }
    if (maxRetries >= 0) {
      txnBuilder = txnBuilder.withMaxRetries(maxRetries)
    }
    txnBuilder.build(engine)
  }

  private def tblPropertiesWithDomainMetadata(
      tableProperties: Map[String, String],
      withDomainMetadataSupported: Boolean): Map[String, String] = {
    if (tableProperties == null && !withDomainMetadataSupported) {
      null
    } else {
      val origTblProps = if (tableProperties != null) tableProperties else Map()
      val dmTblProps = if (withDomainMetadataSupported) {
        Map(TableFeatures.SET_TABLE_FEATURE_SUPPORTED_PREFIX + "domainMetadata" -> "supported")
      } else {
        Map()
      }
      origTblProps ++ dmTblProps
    }
  }
}
