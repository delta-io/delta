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
package io.delta.kernel.defaults

import scala.collection.JavaConverters._
import scala.collection.immutable.Seq

import io.delta.kernel.TransactionCommitResult
import io.delta.kernel.data.FilteredColumnarBatch
import io.delta.kernel.defaults.utils.AbstractWriteUtils
import io.delta.kernel.engine.Engine
import io.delta.kernel.expressions.{Column, Literal}
import io.delta.kernel.expressions.Literal.{ofInt, ofString}
import io.delta.kernel.internal.SnapshotImpl
import io.delta.kernel.internal.clustering.ClusteringMetadataDomain
import io.delta.kernel.internal.tablefeatures.{TableFeature, TableFeatures}
import io.delta.kernel.types.{IntegerType, StringType, StructType}

import org.scalatest.funsuite.AnyFunSuite

trait DeltaReplaceTableSuiteBase extends AnyFunSuite with AbstractWriteUtils {

  /* -------- Test values to use -------- */

  case class SchemaDef(
      schema: StructType,
      partitionColumns: Seq[String] = Seq.empty,
      clusteringColumns: Option[List[Column]] = None) {
    override def toString: String = {
      s"Schema=$schema, partCols=$partitionColumns, " +
        s"clusteringColumns=${clusteringColumns.map(_.toString).getOrElse(List.empty)}"
    }
  }

  val schemaA = new StructType()
    .add("col1", IntegerType.INTEGER)
    .add("col2", IntegerType.INTEGER)

  val schemaB = new StructType()
    .add("col4", StringType.STRING)
    .add("col5", StringType.STRING)

  val unpartitionedSchemaDefA = SchemaDef(schemaA)
  val unpartitionedSchemaDefB = SchemaDef(schemaB)
  val unpartitionedSchemaDefA_dataBatches = generateData(schemaA, Seq.empty, Map.empty, 200, 3)
  val unpartitionedSchemaDefB_dataBatches = generateData(schemaB, Seq.empty, Map.empty, 200, 3)

  val partitionedSchemaDefA_1 = SchemaDef(schemaA, partitionColumns = Seq("col1"))
  val partitionedSchemaDefA_2 = SchemaDef(schemaA, partitionColumns = Seq("col2"))
  val partitionedSchemaDefA_1_dataBatches = generateData(
    schemaA,
    partitionedSchemaDefA_1.partitionColumns,
    Map("col1" -> ofInt(1)),
    batchSize = 237,
    numBatches = 3)
  val partitionedSchemaDefA_2_dataBatches = generateData(
    schemaA,
    partitionedSchemaDefA_2.partitionColumns,
    Map("col2" -> ofInt(5)),
    batchSize = 400,
    numBatches = 1)

  val partitionedSchemaDefB = SchemaDef(schemaB, partitionColumns = Seq("col4"))
  val partitionedSchemaDefB_dataBatches = generateData(
    schemaB,
    partitionedSchemaDefB.partitionColumns,
    Map("col4" -> ofString("foo")),
    batchSize = 100,
    numBatches = 1)

  val clusteredSchemaDefA_1 = SchemaDef(
    schemaA,
    clusteringColumns = Some(List(new Column("col1"))))
  val clusteredSchemaDefA_2 = SchemaDef(
    schemaA,
    clusteringColumns = Some(List(new Column("col2"))))
  val clusteredSchemaDefA_1_dataBatches = generateData(
    schemaA,
    partitionCols = Seq.empty,
    partitionValues = Map.empty,
    batchSize = 237,
    numBatches = 3)
  val clusteredSchemaDefA_2_dataBatches = generateData(
    schemaA,
    partitionCols = Seq.empty,
    partitionValues = Map.empty,
    batchSize = 100,
    numBatches = 3)

  val clusteredSchemaDefB = SchemaDef(
    schemaB,
    clusteringColumns = Some(List(new Column("col4"))))
  val clusteredSchemaDefB_dataBatches = generateData(
    schemaB,
    partitionCols = Seq.empty,
    partitionValues = Map.empty,
    batchSize = 2,
    numBatches = 1)

  val validSchemaDefs = Map(
    unpartitionedSchemaDefA ->
      Seq(Map.empty[String, Literal] -> unpartitionedSchemaDefA_dataBatches),
    unpartitionedSchemaDefB ->
      Seq(Map.empty[String, Literal] -> unpartitionedSchemaDefB_dataBatches),
    partitionedSchemaDefA_1 ->
      Seq(Map("col1" -> ofInt(1)) -> partitionedSchemaDefA_1_dataBatches),
    partitionedSchemaDefA_2 ->
      Seq(Map("col2" -> ofInt(5)) -> partitionedSchemaDefA_2_dataBatches),
    partitionedSchemaDefB ->
      Seq(Map("col4" -> ofString("foo")) -> partitionedSchemaDefB_dataBatches),
    clusteredSchemaDefA_1 ->
      Seq(Map.empty[String, Literal] -> clusteredSchemaDefA_1_dataBatches),
    clusteredSchemaDefA_2 ->
      Seq(Map.empty[String, Literal] -> clusteredSchemaDefA_2_dataBatches),
    clusteredSchemaDefB ->
      Seq(Map.empty[String, Literal] -> clusteredSchemaDefB_dataBatches))

  /* -------- Test methods -------- */

  protected def createInitialTable(
      engine: Engine,
      tablePath: String,
      schema: StructType = testSchema,
      partitionColumns: Seq[String] = Seq.empty,
      clusteringColumns: Option[List[Column]] = None,
      tableProperties: Map[String, String] = null,
      includeData: Boolean = true,
      data: Seq[(Map[String, Literal], Seq[FilteredColumnarBatch])] =
        Seq(Map.empty[String, Literal] -> (dataBatches1))): Unit = {
    val dataToWrite = if (includeData) {
      data
    } else {
      Seq.empty
    }

    appendData(
      engine,
      tablePath,
      isNewTable = true,
      schema,
      partCols = partitionColumns,
      clusteringColsOpt = clusteringColumns,
      tableProperties = tableProperties,
      data = dataToWrite)
    checkTable(tablePath, dataToWrite.flatMap(_._2).flatMap(_.toTestRows))
  }

  protected def commitReplaceTable(
      engine: Engine,
      tablePath: String,
      schema: StructType = testSchema,
      partitionColumns: Seq[String] = Seq.empty,
      clusteringColumns: Option[Seq[Column]] = None,
      tableProperties: Map[String, String] = Map.empty,
      domainsToAdd: Seq[(String, String)] = Seq.empty,
      data: Seq[(Map[String, Literal], Seq[FilteredColumnarBatch])] = Seq.empty)
      : TransactionCommitResult = {

    val txn = getReplaceTxn(
      engine,
      tablePath,
      schema,
      partitionColumns,
      clusteringColumns,
      tableProperties,
      domainsToAdd.nonEmpty)
    domainsToAdd.foreach { case (domainName, config) =>
      txn.addDomainMetadata(domainName, config)
    }

    commitTransaction(txn, engine, getAppendActions(txn, data))
  }

  // scalastyle:off argcount
  protected def checkReplaceTable(
      engine: Engine,
      tablePath: String,
      schema: StructType = testSchema,
      partitionColumns: Seq[String] = Seq.empty,
      clusteringColumns: Option[Seq[Column]] = None,
      transactionId: Option[(String, Long)] = None,
      tableProperties: Map[String, String] = Map.empty,
      domainsToAdd: Seq[(String, String)] = Seq.empty,
      data: Seq[(Map[String, Literal], Seq[FilteredColumnarBatch])] = Seq.empty,
      expectedTableProperties: Option[Map[String, String]] = None,
      expectedTableFeaturesSupported: Seq[TableFeature] = Seq.empty): Unit = {
    // scalastyle:on argcount
    val oldProtocol = getProtocol(engine, tablePath)
    val wasClusteredTable = oldProtocol.supportsFeature(TableFeatures.CLUSTERING_W_FEATURE)

    val commitResult = commitReplaceTable(
      engine,
      tablePath,
      schema,
      partitionColumns,
      clusteringColumns,
      tableProperties,
      domainsToAdd,
      data)
    assertCommitResultHasClusteringCols(commitResult, clusteringColumns.getOrElse(Seq.empty))

    verifyWrittenContent(
      tablePath,
      schema,
      data.flatMap(_._2).flatMap(_.toTestRows))

    val snapshot = latestSnapshot(tablePath).asInstanceOf[SnapshotImpl]
    assert(snapshot.getPartitionColumnNames.asScala == partitionColumns)

    clusteringColumns match {
      case Some(clusteringCols) =>
        // Check clustering table feature is supported
        assertHasWriterFeature(snapshot, "clustering")
        assertHasWriterFeature(snapshot, "domainMetadata")
        // Validate clustering columns are correct
        // TODO when we support column mapping we will need to convert to physical-name here
        assert(snapshot.getPhysicalClusteringColumns.toScala
          .exists(_.asScala == clusteringCols))
      case None =>
        if (wasClusteredTable) {
          // If the table was previously clustered we expect the table feature to remain and for
          // there to be a clustering domain metadata with clusteringColumns=[]
          assertHasWriterFeature(snapshot, "clustering")
          assert(snapshot.getPhysicalClusteringColumns.toScala
            .exists(_.isEmpty))
        } else {
          // Otherwise there should be no table feature and no clustering domain metadata
          assertHasNoWriterFeature(snapshot, "clustering")
          assert(!ClusteringMetadataDomain.fromSnapshot(snapshot).isPresent)
        }
    }

    assert(snapshot.getMetadata.getConfiguration.asScala ==
      expectedTableProperties.getOrElse(tableProperties))

    val nonClusteringActiveDomains = snapshot.getActiveDomainMetadataMap.asScala
      .filter { case (domainName, _) =>
        domainName != ClusteringMetadataDomain.DOMAIN_NAME
      }.map { case (domainName, domainMetadata) => (domainName, domainMetadata.getConfiguration) }
    assert(nonClusteringActiveDomains.toSet == domainsToAdd.toSet)

    val newProtocol = getProtocol(engine, tablePath)
    // Check that we never downgrade the protocol
    assert(oldProtocol.canUpgradeTo(newProtocol))
    assert(expectedTableFeaturesSupported.forall(newProtocol.supportsFeature))

    val row = spark.sql(s"DESCRIBE HISTORY delta.`$tablePath`")
      .filter(s"version = ${snapshot.getVersion}")
      .select("operation")
      .collect().last
    assert(row.getAs[String]("operation") == "REPLACE TABLE")
  }
}
