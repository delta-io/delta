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
import io.delta.kernel.internal.TableImpl
import io.delta.kernel.internal.table.SnapshotBuilderImpl
import io.delta.kernel.internal.tablefeatures.TableFeatures
import io.delta.kernel.internal.util.Clock
import io.delta.kernel.transaction.DataLayoutSpec
import io.delta.kernel.types.StructType

/**
 * Test helper contract for constructing and configuring Delta Kernel transactions.
 */
trait TransactionBuilderSupport {

  // TODO: we should standardize on using ONLY `createTxn` and remove this eventually
  def createWriteTxnBuilder(
      table: Table,
      operation: Operation = Operation.WRITE): TransactionBuilder

  // scalastyle:off argcount
  def createTxn(
      engine: Engine,
      tablePath: String,
      isNewTable: Boolean = false,
      schema: StructType = null,
      partCols: Seq[String] = null,
      tableProperties: Map[String, String] = null,
      clock: Clock = () => System.currentTimeMillis,
      withDomainMetadataSupported: Boolean = false,
      maxRetries: Int = -1,
      clusteringColsOpt: Option[List[Column]] = None,
      logCompactionInterval: Int = 10,
      txnId: Option[(String, Long)] = None,
      tablePropertiesRemoved: Set[String] = null): Transaction
  // scalastyle:on argcount
}

/** An implementation of [[TransactionBuilderSupport]] that uses the V1 transaction builder. */
trait TransactionBuilderV1Support extends TransactionBuilderSupport with TestUtils {

  // scalastyle:off argcount
  override def createTxn(
      engine: Engine,
      tablePath: String,
      isNewTable: Boolean = false,
      schema: StructType = null,
      partCols: Seq[String] = null,
      tableProperties: Map[String, String] = null,
      clock: Clock = () => System.currentTimeMillis,
      withDomainMetadataSupported: Boolean = false,
      maxRetries: Int = -1,
      clusteringColsOpt: Option[List[Column]] = None,
      logCompactionInterval: Int = 10,
      txnId: Option[(String, Long)] = None,
      tablePropertiesRemoved: Set[String] = null): Transaction = {
    // scalastyle:on argcount
    val operation = if (isNewTable) Operation.CREATE_TABLE else Operation.WRITE

    var txnBuilder = createWriteTxnBuilder(
      TableImpl.forPath(engine, tablePath, clock),
      operation)

    if (isNewTable) {
      txnBuilder = txnBuilder.withSchema(engine, schema)
      if (partCols != null) {
        txnBuilder = txnBuilder.withPartitionColumns(engine, partCols.asJava)
      }
    } else if (schema != null) {
      txnBuilder = txnBuilder.withSchema(engine, schema)
    }

    if (clusteringColsOpt.isDefined) {
      txnBuilder = txnBuilder.withClusteringColumns(engine, clusteringColsOpt.get.asJava)
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

    txnBuilder = txnBuilder.withLogCompactionInverval(logCompactionInterval)

    txnId.foreach { case (appId, txnVer) =>
      txnBuilder = txnBuilder.withTransactionId(engine, appId, txnVer)
    }
    if (tablePropertiesRemoved != null) {
      txnBuilder = txnBuilder.withTablePropertiesRemoved(tablePropertiesRemoved.asJava)
    }
    txnBuilder.build(engine)
  }

  override def createWriteTxnBuilder(
      table: Table,
      operation: Operation = Operation.WRITE): TransactionBuilder = {
    table.createTransactionBuilder(defaultEngine, "test-engine", operation)
  }
}

/** An implementation of [[TransactionBuilderSupport]] that uses the V2 transaction builder. */
trait TransactionBuilderV2Support extends TransactionBuilderSupport with TestUtils {

  // scalastyle:off argcount
  override def createTxn(
      engine: Engine,
      tablePath: String,
      isNewTable: Boolean = false,
      schema: StructType = null,
      partCols: Seq[String] = null,
      tableProperties: Map[String, String] = null,
      clock: Clock = () => System.currentTimeMillis,
      withDomainMetadataSupported: Boolean = false,
      maxRetries: Int = -1,
      clusteringColsOpt: Option[List[Column]] = None,
      logCompactionInterval: Int = 10,
      txnId: Option[(String, Long)] = None,
      tablePropertiesRemoved: Set[String] = null): Transaction = {
    // scalastyle:on argcount
    def tblPropertiesWithDomainMetadata = {
      val origTblProps = if (tableProperties != null) tableProperties else Map()
      val dmTblProps = if (withDomainMetadataSupported) {
        Map(TableFeatures.SET_TABLE_FEATURE_SUPPORTED_PREFIX + "domainMetadata" -> "supported")
      } else {
        Map()
      }
      origTblProps ++ dmTblProps
    }

    if (isNewTable) {
      var txnBuilder = TableManager.buildCreateTableTransaction(
        tablePath,
        schema,
        "test-engine")
      if (partCols != null) {
        txnBuilder = txnBuilder.withDataLayoutSpec(
          DataLayoutSpec.partitioned(partCols.map(new Column(_)).asJava))
      }
      if (tableProperties != null || withDomainMetadataSupported) {
        txnBuilder = txnBuilder.withTableProperties(tblPropertiesWithDomainMetadata.asJava)
      }
      if (clusteringColsOpt.nonEmpty) {
        txnBuilder = txnBuilder.withDataLayoutSpec(
          DataLayoutSpec.clustered(clusteringColsOpt.get.asJava))
      }
      if (maxRetries >= 0) {
        txnBuilder = txnBuilder.withMaxRetries(maxRetries)
      }
      txnBuilder.build(engine)
    } else {
      var txnBuilder = TableManager.loadSnapshot(tablePath)
        .build(engine)
        .buildUpdateTableTransaction("test-engine", Operation.WRITE)
      /*
      TODO: in a separate PR clean up usage of createTxn such that schema != null ONLY when it
        should either be updated for schema evolution or for a new table
      if (schema != null) {
        txnBuilder = txnBuilder.withUpdatedSchema(schema)
      }
       */
      if (clusteringColsOpt.nonEmpty) {
        txnBuilder = txnBuilder.withClusteringColumns(clusteringColsOpt.get.asJava)
      }
      if (tableProperties != null || withDomainMetadataSupported) {
        txnBuilder = txnBuilder.withTablePropertiesAdded(tblPropertiesWithDomainMetadata.asJava)
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
  }

  override def createWriteTxnBuilder(
      table: Table,
      operation: Operation = Operation.WRITE): TransactionBuilder = {
    throw new UnsupportedOperationException(
      "This is unsupported for V2 builders, instead use createTxn")
  }
}
