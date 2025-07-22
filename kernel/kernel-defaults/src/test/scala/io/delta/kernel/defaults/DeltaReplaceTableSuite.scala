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

import io.delta.kernel.{Operation, Table, Transaction, TransactionBuilder, TransactionCommitResult}
import io.delta.kernel.data.FilteredColumnarBatch
import io.delta.kernel.defaults.utils.TestRow
import io.delta.kernel.engine.Engine
import io.delta.kernel.exceptions.{ConcurrentTransactionException, ConcurrentWriteException, KernelException, TableNotFoundException}
import io.delta.kernel.expressions.{Column, Literal}
import io.delta.kernel.expressions.Literal.{ofInt, ofString}
import io.delta.kernel.internal.{SnapshotImpl, TableConfig, TableImpl}
import io.delta.kernel.internal.clustering.{ClusteringMetadataDomain, ClusteringUtils}
import io.delta.kernel.internal.tablefeatures.{TableFeature, TableFeatures}
import io.delta.kernel.types.{IntegerType, StringType, StructType}
import io.delta.kernel.utils.CloseableIterable.emptyIterable

trait DeltaReplaceTableSuiteBase extends DeltaTableWriteSuiteBase {

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

  protected def getReplaceTxnBuilder(engine: Engine, tablePath: String): TransactionBuilder = {
    Table.forPath(engine, tablePath).asInstanceOf[TableImpl]
      .createReplaceTableTransactionBuilder(engine, testEngineInfo)
  }

  protected def getReplaceTransaction(
      engine: Engine,
      tablePath: String,
      schema: StructType = testSchema,
      partitionColumns: Seq[String] = Seq.empty,
      clusteringColumns: Option[Seq[Column]] = None,
      transactionId: Option[(String, Long)] = None,
      tableProperties: Map[String, String] = Map.empty,
      domainsToAdd: Seq[(String, String)] = Seq.empty): Transaction = {
    var txnBuilder = getReplaceTxnBuilder(engine, tablePath)
      .withSchema(engine, schema)
      .withPartitionColumns(engine, partitionColumns.asJava)
      .withTableProperties(engine, tableProperties.asJava)

    clusteringColumns.foreach { cols =>
      txnBuilder = txnBuilder.withClusteringColumns(engine, cols.asJava)
    }

    transactionId.foreach { case (id, version) =>
      txnBuilder = txnBuilder.withTransactionId(engine, id, version)
    }

    if (domainsToAdd.nonEmpty) {
      txnBuilder = txnBuilder.withDomainMetadataSupported()
    }

    val txn = txnBuilder.build(engine)
    domainsToAdd.foreach { case (domainName, config) =>
      txn.addDomainMetadata(domainName, config)
    }
    txn
  }

  protected def commitReplaceTable(
      engine: Engine,
      tablePath: String,
      schema: StructType = testSchema,
      partitionColumns: Seq[String] = Seq.empty,
      clusteringColumns: Option[Seq[Column]] = None,
      transactionId: Option[(String, Long)] = None,
      tableProperties: Map[String, String] = Map.empty,
      domainsToAdd: Seq[(String, String)] = Seq.empty,
      data: Seq[(Map[String, Literal], Seq[FilteredColumnarBatch])] = Seq.empty)
      : TransactionCommitResult = {

    val txn = getReplaceTransaction(
      engine,
      tablePath,
      schema,
      partitionColumns,
      clusteringColumns,
      transactionId,
      tableProperties,
      domainsToAdd)

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
      transactionId,
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
        assert(ClusteringUtils.getClusteringColumnsOptional(snapshot).toScala
          .exists(_.asScala == clusteringCols))
      case None =>
        if (wasClusteredTable) {
          // If the table was previously clustered we expect the table feature to remain and for
          // there to be a clustering domain metadata with clusteringColumns=[]
          assertHasWriterFeature(snapshot, "clustering")
          assert(ClusteringUtils.getClusteringColumnsOptional(snapshot).toScala
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

class DeltaReplaceTableSuite extends DeltaReplaceTableSuiteBase {

  /* ----- ERROR CASES ------ */

  test("Conflict resolution is disabled for replace table") {
    withTempDirAndEngine { (tablePath, engine) =>
      createInitialTable(engine, tablePath)
      // Start replace transaction
      val txn = getReplaceTransaction(engine, tablePath)
      // Commit a simple blind append as a conflicting txn
      appendData(
        engine,
        tablePath,
        data = Seq(Map.empty[String, Literal] -> (dataBatches2)))
      // Try to commit replace table and intercept conflicting txn (no conflict resolution)
      intercept[ConcurrentWriteException] {
        commitTransaction(txn, engine, emptyIterable())
      }
    }
  }

  test("Table::createTransactionBuilder does not allow REPLACE TABLE") {
    withTempDirAndEngine { (tablePath, engine) =>
      createInitialTable(engine, tablePath)
      assert(intercept[UnsupportedOperationException] {
        Table.forPath(engine, tablePath)
          .createTransactionBuilder(engine, testEngineInfo, Operation.REPLACE_TABLE)
          .build(engine)
      }.getMessage.contains("REPLACE TABLE is not yet supported"))
    }
  }

  test("Cannot replace a table that does not exist") {
    withTempDirAndEngine { (tablePath, engine) =>
      assert(
        intercept[TableNotFoundException] {
          commitReplaceTable(engine, tablePath)
        }.getMessage.contains("Trying to replace a table that does not exist"))
    }
  }

  test("Cannot enable a feature that Kernel does not support") {
    withTempDirAndEngine { (tablePath, engine) =>
      createInitialTable(engine, tablePath)
      assert(
        intercept[KernelException] {
          commitReplaceTable(
            engine,
            tablePath,
            tableProperties = Map(TableConfig.CHANGE_DATA_FEED_ENABLED.getKey -> "true"))
        }.getMessage.contains("Unsupported Delta writer feature"))
    }
  }

  test("Transaction identifier is used to preserve idempotent writes") {
    withTempDirAndEngine { (tablePath, engine) =>
      createInitialTable(engine, tablePath)
      commitReplaceTable(
        engine,
        tablePath,
        transactionId = Some(("foo", 0)))
      intercept[ConcurrentTransactionException] {
        commitReplaceTable(
          engine,
          tablePath,
          transactionId = Some(("foo", 0)))
      }
    }
  }

  test("Cannot replace a table with a protocol Kernel does not support") {
    withTempDirAndEngine { (tablePath, engine) =>
      spark.sql(
        s"""
          |CREATE TABLE delta.`$tablePath` (id INT) USING DELTA
          |TBLPROPERTIES('delta.enableChangeDataFeed' = 'true')
          |""".stripMargin)
      assert(
        intercept[KernelException] {
          commitReplaceTable(
            engine,
            tablePath)
        }.getMessage.contains("Unsupported Delta writer feature"))
    }
  }

  test("Must provide a schema for replace table transaction") {
    withTempDirAndEngine { (tablePath, engine) =>
      createInitialTable(engine, tablePath)
      assert(intercept[KernelException] {
        getReplaceTxnBuilder(engine, tablePath)
          .build(engine)
      }.getMessage.contains("Must provide a new schema for REPLACE TABLE"))
    }
  }

  test("Cannot define both partition and clustering columns at the same time") {
    withTempDirAndEngine { (tablePath, engine) =>
      createInitialTable(engine, tablePath)
      assert(intercept[IllegalArgumentException] {
        getReplaceTransaction(
          engine,
          tablePath,
          schema = testPartitionSchema,
          partitionColumns = testPartitionColumns,
          clusteringColumns = Some(testClusteringColumns))
      }.getMessage.contains(
        "Partition Columns and Clustering Columns cannot be set at the same time"))
    }
  }

  test("Schema provided must be valid") {
    withTempDirAndEngine { (tablePath, engine) =>
      createInitialTable(engine, tablePath)
      assert(intercept[KernelException] {
        getReplaceTransaction(
          engine,
          tablePath,
          schema = new StructType().add("col", IntegerType.INTEGER).add("col", IntegerType.INTEGER))
      }.getMessage.contains(
        "Schema contains duplicate columns"))
    }
  }

  test("Partition columns provided must be valid") {
    withTempDirAndEngine { (tablePath, engine) =>
      createInitialTable(engine, tablePath)
      assert(intercept[IllegalArgumentException] {
        getReplaceTransaction(
          engine,
          tablePath,
          partitionColumns = Seq("foo"))
      }.getMessage.contains(
        "Partition column foo not found in the schema"))
    }
  }

  test("Clustering columns provided must be valid") {
    withTempDirAndEngine { (tablePath, engine) =>
      createInitialTable(engine, tablePath)
      assert(intercept[KernelException] {
        getReplaceTransaction(
          engine,
          tablePath,
          clusteringColumns = Some(Seq(new Column("foo"))))
      }.getMessage.contains(
        "Column 'column(`foo`)' was not found in the table schema"))
    }
  }

  test("icebergWriterCompatV1 checks are enforced") {
    withTempDirAndEngine { (tablePath, engine) =>
      createInitialTable(engine, tablePath)
      assert(
        intercept[KernelException] {
          commitReplaceTable(
            engine,
            tablePath,
            tableProperties = Map(
              TableConfig.ICEBERG_WRITER_COMPAT_V1_ENABLED.getKey -> "true",
              TableConfig.COLUMN_MAPPING_MODE.getKey -> "name"))
        }.getMessage.contains("The value 'name' for the property 'delta.columnMapping.mode' is " +
          "not compatible with icebergWriterCompatV1 requirements"))
    }
  }

  test("icebergCompatV2 checks are enforced") {
    withTempDirAndEngine { (tablePath, engine) =>
      createInitialTable(engine, tablePath)
      assert(
        intercept[KernelException] {
          commitReplaceTable(
            engine,
            tablePath,
            tableProperties = Map(
              TableConfig.ICEBERG_COMPAT_V2_ENABLED.getKey -> "true",
              TableConfig.DELETION_VECTORS_CREATION_ENABLED.getKey -> "true"))
        }.getMessage.contains(
          "Table features [deletionVectors] are incompatible with icebergCompatV2"))
    }
  }

  test("REPLACE is not supported on existing table with icebergCompatV3 feature") {
    withTempDirAndEngine { (tablePath, engine) =>
      createInitialTable(
        engine,
        tablePath,
        tableProperties = Map(TableConfig.ICEBERG_COMPAT_V3_ENABLED.getKey -> "true"),
        includeData = false // To avoid writing data with correct CM schema
      )
      assert(
        intercept[UnsupportedOperationException] {
          commitReplaceTable(engine, tablePath)
        }.getMessage.contains("REPLACE TABLE is not yet supported on IcebergCompatV3 tables"))
    }
  }

  test("REPLACE is not supported when enabling icebergCompatV3 feature") {
    withTempDirAndEngine { (tablePath, engine) =>
      createInitialTable(
        engine,
        tablePath,
        tableProperties = Map(
          TableConfig.COLUMN_MAPPING_MODE.getKey -> "name"),
        includeData = false // To avoid writing data with correct CM schema
      )
      assert(
        intercept[UnsupportedOperationException] {
          commitReplaceTable(
            engine,
            tablePath,
            tableProperties = Map(
              TableConfig.ICEBERG_COMPAT_V3_ENABLED.getKey -> "true",
              TableConfig.COLUMN_MAPPING_MODE.getKey -> "name"))
        }.getMessage.contains("REPLACE TABLE is not yet supported on IcebergCompatV3 tables"))
    }
  }

  /* ----------------- POSITIVE CASES ----------------- */

  // TODO can we refactor other suites to run with both create + replace?

  Seq(Seq(), Seq(Map.empty[String, Literal] -> (dataBatches1))).foreach { replaceData =>
    test(s"Basic case with no metadata changes, insertData=${replaceData.nonEmpty}") {
      withTempDirAndEngine { (tablePath, engine) =>
        createInitialTable(engine, tablePath)
        checkReplaceTable(engine, tablePath, data = replaceData)
      }
    }

    test(s"Basic case with initial empty table, insertData=${replaceData.nonEmpty}") {
      withTempDirAndEngine { (tablePath, engine) =>
        createInitialTable(engine, tablePath)
        checkReplaceTable(engine, tablePath, data = replaceData)
      }
    }
  }

  // Note, these tests cover things like transitioning between unpartitioned, partitioned, and
  // clustered tables. This means it includes removing existing clustering domains when the initial
  // table was clustered.
  validSchemaDefs.foreach { case (initialSchemaDef, initialData) =>
    validSchemaDefs.foreach { case (replaceSchemaDef, replaceData) =>
      Seq(true, false).foreach { initialTableEmpty =>
        Seq(true, false).foreach { insertDataInReplace =>
          test(s"Schema change from $initialSchemaDef to $replaceSchemaDef; " +
            s"initialTableEmpty=$initialTableEmpty, insertDataInReplace=$insertDataInReplace") {
            withTempDirAndEngine { (tablePath, engine) =>
              createInitialTable(
                engine,
                tablePath,
                schema = initialSchemaDef.schema,
                partitionColumns = initialSchemaDef.partitionColumns,
                clusteringColumns = initialSchemaDef.clusteringColumns,
                includeData = !initialTableEmpty,
                data = initialData)
              checkReplaceTable(
                engine,
                tablePath,
                schema = replaceSchemaDef.schema,
                partitionColumns = replaceSchemaDef.partitionColumns,
                clusteringColumns = replaceSchemaDef.clusteringColumns,
                data = if (insertDataInReplace) replaceData else Seq.empty)
            }
          }

        }

      }

      test(s"Schema change from $initialSchemaDef to $replaceSchemaDef") {
        withTempDirAndEngine { (tablePath, engine) =>
          createInitialTable(
            engine,
            tablePath,
            schema = initialSchemaDef.schema,
            partitionColumns = initialSchemaDef.partitionColumns,
            clusteringColumns = initialSchemaDef.clusteringColumns,
            includeData = false)
          checkReplaceTable(
            engine,
            tablePath,
            schema = replaceSchemaDef.schema,
            partitionColumns = replaceSchemaDef.partitionColumns,
            clusteringColumns = replaceSchemaDef.clusteringColumns)
        }
      }
    }
  }

  test("Case with DVs in the initial table") {
    withTempDirAndEngine { (tablePath, engine) =>
      spark.sql(
        s"""
           |CREATE TABLE delta.`$tablePath` (id INT) USING DELTA
           |TBLPROPERTIES('delta.enableDeletionVectors' = 'true')
           |""".stripMargin)
      spark.sql(
        s"""
           |INSERT INTO delta.`$tablePath` VALUES (0), (1), (2), (3)
           |""".stripMargin)
      spark.sql(
        s"""
           |DELETE FROM delta.`$tablePath` WHERE id > 1
           |""".stripMargin)
      checkTable(tablePath, Seq(TestRow(0), TestRow(1)))
      checkReplaceTable(engine, tablePath) // check it is empty after (also DVs no longer enabled)
    }
  }

  test("Existing table properties are removed") {
    withTempDirAndEngine { (tablePath, engine) =>
      createInitialTable(
        engine,
        tablePath,
        tableProperties = Map(
          TableConfig.APPEND_ONLY_ENABLED.getKey -> "true",
          "user.facing.prop" -> "existing_prop"))
      checkReplaceTable(engine, tablePath)
    }
  }

  test("New table features are correctly enabled") {
    // This also validates that withDomainMetadataSupported works
    withTempDirAndEngine { (tablePath, engine) =>
      createInitialTable(engine, tablePath)
      checkReplaceTable(
        engine,
        tablePath,
        tableProperties = Map(
          TableConfig.DELETION_VECTORS_CREATION_ENABLED.getKey -> "true"),
        domainsToAdd = Seq(("domain-name", "some-config")),
        expectedTableFeaturesSupported =
          Seq(TableFeatures.DELETION_VECTORS_RW_FEATURE, TableFeatures.DOMAIN_METADATA_W_FEATURE))
    }
  }

  test("Domain metadata are reset (user-facing)") {
    // (1) checks that we correctly override an existing domain with the new config if set in the
    //     replace txn
    // (2) checks we remove stale ones that are not set in the replace txn
    withTempDirAndEngine { (tablePath, engine) =>
      // Create initial table with 2 domains
      val txn = Table.forPath(engine, tablePath).asInstanceOf[TableImpl]
        .createTransactionBuilder(engine, "test-info", Operation.CREATE_TABLE)
        .withSchema(engine, testSchema)
        .withDomainMetadataSupported()
        .build(engine)
      txn.addDomainMetadata("domainToOverride", "check1")
      txn.addDomainMetadata("domainToRemove", "check2")
      commitTransaction(txn, engine, emptyIterable())

      // Validate the 2 domains are present
      val snapshot = Table.forPath(engine, tablePath).getLatestSnapshot(engine)
      assert(snapshot.getDomainMetadata("domainToOverride").toScala.contains("check1"))
      assert(snapshot.getDomainMetadata("domainToRemove").toScala.contains("check2"))

      // Replace table and override 1/2 of the domains
      checkReplaceTable(
        engine,
        tablePath,
        domainsToAdd = Seq(("domainToOverride", "overridden-config")))
    }
  }

  test("Column mapping maxFieldId is preserved during REPLACE TABLE") {
    // We should preserve maxFieldId regardless of column mapping mode (if a future replace
    // operation re-enables id mode we should not start our fieldIds from 0)
    withTempDirAndEngine { (tablePath, engine) =>
      createInitialTable(
        engine,
        tablePath,
        tableProperties = Map(
          TableConfig.COLUMN_MAPPING_MODE.getKey -> "id"),
        includeData = false // To avoid writing data with correct CM schema
      )
      checkReplaceTable(
        engine,
        tablePath,
        expectedTableProperties = Some(Map(TableConfig.COLUMN_MAPPING_MAX_COLUMN_ID.getKey -> "1")))
    }
  }

  test("icebergCompatV2 checks are executed and properties updated/auto-enabled") {
    withTempDirAndEngine { (tablePath, engine) =>
      createInitialTable(engine, tablePath)
      // TODO once we support column mapping update this test
      intercept[UnsupportedOperationException] {
        checkReplaceTable(
          engine,
          tablePath,
          tableProperties = Map(TableConfig.ICEBERG_COMPAT_V2_ENABLED.getKey -> "true"),
          expectedTableProperties = Some(Map(
            TableConfig.COLUMN_MAPPING_MODE.getKey -> "name",
            TableConfig.ICEBERG_COMPAT_V2_ENABLED.getKey -> "true")),
          expectedTableFeaturesSupported = Seq(
            TableFeatures.ICEBERG_COMPAT_V2_W_FEATURE,
            TableFeatures.COLUMN_MAPPING_RW_FEATURE))
      }
    }
  }

  // TODO - can we reuse the tests in IcebergWriterCompatV1Suite to run with both create table and
  //  replace table?
  test("icebergWriterCompatV1 checks are executed and properties updated/auto-enabled") {
    // This also validates you can enable icebergWriterCompatV1 on an existing table during replace
    withTempDirAndEngine { (tablePath, engine) =>
      createInitialTable(engine, tablePath)
      // TODO once we support column mapping update this test
      intercept[UnsupportedOperationException] {
        checkReplaceTable(
          engine,
          tablePath,
          tableProperties = Map(TableConfig.ICEBERG_WRITER_COMPAT_V1_ENABLED.getKey -> "true"),
          expectedTableProperties = Some(Map(
            TableConfig.ICEBERG_WRITER_COMPAT_V1_ENABLED.getKey -> "true",
            TableConfig.COLUMN_MAPPING_MODE.getKey -> "id",
            TableConfig.ICEBERG_COMPAT_V2_ENABLED.getKey -> "true")),
          expectedTableFeaturesSupported = Seq(
            TableFeatures.ICEBERG_COMPAT_V2_W_FEATURE,
            TableFeatures.ICEBERG_WRITER_COMPAT_V1,
            TableFeatures.COLUMN_MAPPING_RW_FEATURE))
      }
    }
  }

  test("When cmMode=None it is possible to have column with same name different type") {
    withTempDirAndEngine { (tablePath, engine) =>
      createInitialTable(
        engine,
        tablePath,
        schema = new StructType()
          .add("col1", StringType.STRING),
        includeData = false)
      checkReplaceTable(
        engine,
        tablePath,
        schema = new StructType()
          .add("col1", IntegerType.INTEGER))
    }
  }
}
