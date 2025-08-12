package io.delta.kernel.defaults

import scala.collection.JavaConverters._
import scala.collection.immutable.Seq

import io.delta.kernel.{Operation, TableManager, Transaction}
import io.delta.kernel.engine.Engine
import io.delta.kernel.exceptions.KernelException
import io.delta.kernel.expressions.Column
import io.delta.kernel.internal.table.SnapshotBuilderImpl
import io.delta.kernel.internal.tablefeatures.TableFeatures
import io.delta.kernel.internal.util.Clock
import io.delta.kernel.transaction.DataLayoutSpec
import io.delta.kernel.types.StructType

trait UseTransactionBuilderV2 extends DeltaTableWriteSuiteBase {

  // scalastyle:off argcount
  override def createTxn(
      engine: Engine = defaultEngine,
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
        testEngineInfo)
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
        .asInstanceOf[SnapshotBuilderImpl].build(engine)
        .buildUpdateTableTransaction(testEngineInfo, Operation.WRITE)
      if (schema != null) {
        txnBuilder = txnBuilder.withUpdatedSchema(schema)
      }
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
}

/**
 * Tests for the V2 transaction builders [[CreateTableTransactionBuilder]] and
 * [[UpdateTableTransactionBuilder]]. We don't cover the full scope of tests we have for
 * TransactionBuilderV1 (to do so, we would have to duplicate many, many of the existing suites).
 * Instead, we selectively run everything in [[DeltaTableWritesSuite]] as well as some additional
 * white-box-tests for the logic specific to the new builders. The main metadata validation +
 * update logic is shared by both V1 + V2 builders in
 * [[io.delta.kernel.internal.TransactionMetadataFactory]] and thus is covered by all the existing
 * tests we have for the V1 builder (ideally we would have unit tests for just
 * TransactionMetadataFactory in the future).
 *
 * <p>In the future, when we deprecate the V1 builder, we should switch all the existing suites
 * to use the V2 builders -- and maintain something similar to this for the V1 builder.
 */
class TransactionBuildersV2Suite extends DeltaTableWritesSuite with UseTransactionBuilderV2 {

  // Tested in DeltaTableWritesSuite: setTxnOpt (covered by idempotent writes test)
  // Tested in DeltaTableWritesSuite: validateKernelCanWriteToTable (covered by unsupported
  // writer feature test)

  test("Cannot add clustering columns to a partitioned table") {
    withTempDirAndEngine { (tablePath, engine) =>
      createEmptyTable(
        engine,
        tablePath,
        testPartitionSchema,
        partCols = testPartitionColumns)
      val e = intercept[KernelException] {
        TableManager.loadSnapshot(tablePath)
          .asInstanceOf[SnapshotBuilderImpl].build(engine)
          .buildUpdateTableTransaction(testEngineInfo, Operation.WRITE)
          .withClusteringColumns(List(new Column("part1")).asJava)
      }
      assert(e.getMessage.contains("Cannot enable clustering on a partitioned table"))

    }
  }

  test("Cannot use UpdateTableTransactionBuilder with incompatible operations") {
    Seq(Operation.CREATE_TABLE, Operation.REPLACE_TABLE).foreach { op =>
      withTempDirAndEngine { (tablePath, engine) =>
        createEmptyTable(
          engine,
          tablePath,
          testSchema)
        val e = intercept[IllegalArgumentException] {
          TableManager.loadSnapshot(tablePath)
            .asInstanceOf[SnapshotBuilderImpl].build(engine)
            .buildUpdateTableTransaction(testEngineInfo, op)
        }
        assert(e.getMessage.contains(
          s"Operation $op is not compatible with Snapshot::buildUpdateTableTransaction"))
      }
    }
  }
}

// This suite tests table property validation, normalization, and unset/set overlap for
// Create + Update
class TablePropertiesTransactionBuilderV2Suite extends TablePropertiesSuiteBase
    with UseTransactionBuilderV2 {}
