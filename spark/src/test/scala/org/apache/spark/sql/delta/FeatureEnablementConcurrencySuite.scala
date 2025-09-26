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

import org.apache.spark.sql.delta.DeltaTestUtils.BOOLEAN_DOMAIN
import org.apache.spark.sql.delta.actions.{AddFile, Format, Metadata}
import org.apache.spark.sql.delta.fuzzer.{OptimisticTransactionPhases, PhaseLockingTransactionExecutionObserver => TransactionObserver}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.util.{DeltaFileOperations, FileNames}
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.metadata.ParquetMetadata

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.execution.datasources.parquet.ParquetReadSupport
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.ThreadUtils

class FeatureEnablementConcurrencySuite
    extends QueryTest
    with SharedSparkSession
    with DeltaSQLCommandTest
    with ConflictResolutionTestUtils {

  val testTableName = "test_feature_enablement_table"

  /** Represents a transaction that alters a table property. */
  case class AlterTableProperty(
      property: String,
      value: String) extends TestTransaction(Map.empty) {
    override val name: String = s"ALTER TABLE($property $value)"
    override def dataChange: Boolean = false
    override def toSQL(tableName: String): String = {
      s"ALTER TABLE $tableName SET TBLPROPERTIES ('$property' = '$value')"
    }
  }

  /** Represents a transaction that unsets a table property. */
  case class UnsetTableProperty(property: String) extends TestTransaction(Map.empty) {
    override val name: String = s"UNSET PROPERTY($property)"
    override def dataChange: Boolean = false
    override def toSQL(tableName: String): String = {
      s"ALTER TABLE $tableName UNSET TBLPROPERTIES ('$property')"
    }
  }

  private def createTestTable(
      properties: Seq[String] = Seq.empty,
      numPartitions: Int = 2): (DeltaLog, CatalogTable) = {
    sql(s"DROP TABLE IF EXISTS $testTableName")
    val propertiesString = if (properties.nonEmpty) properties.mkString(",") + "," else ""
    sql(
      s"""CREATE TABLE $testTableName (idCol bigint)
         |USING delta
         |TBLPROPERTIES (
         |$propertiesString
         |'${DeltaConfigs.ROW_TRACKING_ENABLED.key}' = 'false'
         |)""".stripMargin)

    spark.range(start = 0, end = 100, step = 1, numPartitions)
      .withColumnRenamed("id", "idCol")
      .write
      .format("delta")
      .mode("append")
      .saveAsTable(testTableName)

    val catalogTable = spark.sessionState.catalog.getTableMetadata(
      TableIdentifier(testTableName))
    (DeltaLog.forTable(spark, catalogTable), catalogTable)
  }

  private def getParquetFooter(deltaLog: DeltaLog, file: AddFile): ParquetMetadata = {
    val dataPath = deltaLog.dataPath.toString
    val filePath = file.path
    val hadoopConf = new Configuration()

    val path = DeltaFileOperations.absolutePath(dataPath, filePath)
    val fileSystem = path.getFileSystem(hadoopConf)
    val fileStatus = fileSystem.listStatus(path).head
    ParquetFileReader.readFooter(hadoopConf, fileStatus)
  }

  private def validateFooter(footer: ParquetMetadata, expected: Boolean): Unit = {
    val footerMetadata = footer.getFileMetaData.getKeyValueMetaData
    assert(footerMetadata.containsKey(ParquetReadSupport.SPARK_METADATA_KEY))
    val fieldMetadata = footerMetadata.get(ParquetReadSupport.SPARK_METADATA_KEY)

    assert(
      fieldMetadata.contains(DeltaColumnMapping.PARQUET_FIELD_ID_METADATA_KEY) === expected &&
        fieldMetadata.contains(DeltaColumnMapping.COLUMN_MAPPING_METADATA_ID_KEY) === expected &&
        fieldMetadata.contains(DeltaColumnMapping.COLUMN_MAPPING_PHYSICAL_NAME_KEY) === expected)
  }

  test("Validate Metadata diff") {
    val metadataA = Metadata(
      id = "idA",
      name = "nameA",
      description = "descriptionA",
      format = Format(options = Map("OptionA" -> "A")),
      schemaString = "schemaA",
      partitionColumns = Seq("colA1", "colA2"),
      configuration = Map("propA1" -> "valueA1", "propA2" -> "valueA2"),
      createdTime = Some(1L))

    val metadataB = Metadata(
      id = "idB",
      name = "nameB",
      description = "descriptionB",
      format = Format(),
      schemaString = "schemaB",
      partitionColumns = Seq("colB1"),
      configuration = Map("propB1" -> "valueB1"),
      createdTime = Some(2L))

    // Diff should output all properties in correct order.
    val expectedDiff = Set(
      "id", "name", "description", "format", "schemaString",
      "partitionColumns", "configuration", "createdTime")
    assert(metadataA.diffFieldNames(metadataB) === expectedDiff,
      """The Metadata properties do not match the expected diff.
        |If you are extending Metadata please check Metadata.diff as well as
        |ConflictChecker.attemptToResolveMetadataConflicts""".stripMargin)

    val metadataA_1 = Metadata(id = "idA")
    val metadataB_1 = Metadata(id = "idB")
    assert(metadataA_1.diffFieldNames(metadataB_1) === Set("id"))

    val metadataA_2 = Metadata(id = "id", name = "nameA")
    val metadataB_2 = Metadata(id = "id", name = "nameB")
    assert(metadataA_2.diffFieldNames(metadataB_2) === Set("name"))

    val metadataA_3 = Metadata(id = "id", description = "descriptionA")
    val metadataB_3 = Metadata(id = "id", description = "descriptionB")
    assert(metadataA_3.diffFieldNames(metadataB_3) === Set("description"))

    val metadataA_4 = Metadata(id = "id", format = Format(options = Map("OptionA" -> "A")))
    val metadataB_4 = Metadata(id = "id", format = Format())
    assert(metadataA_4.diffFieldNames(metadataB_4) === Set("format"))

    val metadataA_5 = Metadata(id = "id", schemaString = "schemaA")
    val metadataB_5 = Metadata(id = "id", schemaString = "schemaB")
    assert(metadataA_5.diffFieldNames(metadataB_5) === Set("schemaString"))

    val metadataA_6 = Metadata(id = "id", partitionColumns = Seq("colA1"))
    val metadataB_6 = Metadata(id = "id", partitionColumns = Seq("colB1"))
    assert(metadataA_6.diffFieldNames(metadataB_6) === Set("partitionColumns"))

    val metadataA_7 = Metadata(id = "id", configuration = Map.empty)
    val metadataB_7 = Metadata(id = "id", configuration = Map("propB1" -> "valueB1"))
    assert(metadataA_7.diffFieldNames(metadataB_7) === Set("configuration"))

    val metadataA_8 = Metadata(id = "id", createdTime = Some(1L))
    val metadataB_8 = Metadata(id = "id", createdTime = Some(2L))
    assert(metadataA_8.diffFieldNames(metadataB_8) === Set("createdTime"))

    val metadataA_9 = Metadata(id = "idA", createdTime = Some(1L))
    val metadataB_9 = Metadata(id = "idB", createdTime = Some(2L))
    assert(metadataA_9.diffFieldNames(metadataB_9) === Set("id", "createdTime"))
  }

  test("checkConfigurationChangesForConflicts") {
    val (deltaLog, catalogTable) = createTestTable()
    val snapshot = deltaLog.update(catalogTableOpt = Some(catalogTable))
    val dummyTransactionInfo = CurrentTransactionInfo(
      txnId = "txn 1",
      readPredicates = Vector.empty,
      readFiles = Set.empty,
      readWholeTable = false,
      readAppIds = Set.empty,
      metadata = Metadata(),
      protocol = snapshot.protocol,
      actions = Seq.empty[AddFile],
      readSnapshot = snapshot,
      commitInfo = None,
      readRowIdHighWatermark = 0L,
      catalogTable = Some(catalogTable),
      domainMetadata = Seq.empty,
      op = DeltaOperations.ManualUpdate)

    val lastVersion = snapshot.version
    val dummyCommit = deltaLog
      .getChangeLogFiles(
        startVersion = lastVersion,
        endVersion = lastVersion,
        catalogTableOpt = Some(catalogTable),
        failOnDataLoss = false)
      .map { case (_, file) => file }
      .filter(FileNames.isDeltaFile)
      .take(1)
      .toList
      .last
    val dummySummary = WinningCommitSummary.createFromFileStatus(deltaLog, dummyCommit)

    val conflictChecker = new ConflictChecker(
      spark,
      initialCurrentTransactionInfo = dummyTransactionInfo,
      winningCommitSummary = dummySummary,
      isolationLevel = WriteSerializable)

    // Test 1: Change 2 configs. One is allowed, the other is not.
    val current = Metadata(configuration = Map("prop1" -> "value1", "prop2" -> "value2"))
    val winning = Metadata(configuration = Map("prop1" -> "newValue1", "prop2" -> "newValue2"))

    val result1 = conflictChecker.checkConfigurationChangesForConflicts(
      current,
      winning,
      allowList = Set("prop1"))
    val expected1 = conflictChecker.ConfigurationChanges(
      areValid = false,
      changed = Map("prop1" -> "newValue1", "prop2" -> "newValue2"))
    assert(result1 === expected1)

    // Test 2: Change 2 configs. Both allowed.
    val result2 = conflictChecker.checkConfigurationChangesForConflicts(
      current,
      winning,
      allowList = Set("prop1", "prop2"))
    val expected2 = conflictChecker.ConfigurationChanges(
      areValid = true,
      changed = Map("prop1" -> "newValue1", "prop2" -> "newValue2"))
    assert(result2 === expected2)

    // Test 3: Same as previous but one property is added instead of changed.
    val result3 = conflictChecker.checkConfigurationChangesForConflicts(
      currentMetadata = Metadata(configuration = Map("prop1" -> "value1")),
      winningMetadata =
        Metadata(configuration = Map("prop1" -> "newValue1", "prop2" -> "newValue2")),
      allowList = Set("prop1", "prop2"))
    val expected3 = conflictChecker.ConfigurationChanges(
      areValid = true,
      added = Map("prop2" -> "newValue2"),
      changed = Map("prop1" -> "newValue1"))
    assert(result3 === expected3)

    // Test 4: Removals are not allowed.
    val result4 = conflictChecker.checkConfigurationChangesForConflicts(
      currentMetadata = Metadata(configuration = Map("prop1" -> "value1")),
      winningMetadata = Metadata(configuration = Map("prop2" -> "newValue2")),
      allowList = Set("prop1", "prop2"))
    val expected4 = conflictChecker.ConfigurationChanges(
      areValid = false,
      removed = Set("prop1"),
      added = Map("prop2" -> "newValue2"))
    assert(result4 === expected4)

  }

  /*
   * -------------------------------------------> TIME ------------------------------------------->
   *
   * Row tracking Enablement:
   *                 protocol ---------- Unbackfill ------------------------ Metadata ------------
   *                 Upgrade             Batch 1                             Update
   *                 prep+commit         prep+commit                         prep+commit
   *
   *
   * Concurrent Txn                                                    prep                commit
   *
   * -------------------------------------------> TIME ------------------------------------------->
   */
  for {
    concurrentTxnName <- Seq("alterTableProperty", "delete")
  } test("Enable row tracking feature " +
      s"concurrent txn: $concurrentTxnName") {
    val (deltaLog, catalogTable) = createTestTable(
      properties = Seq(s"'${DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.key}' = 'false'")
    )
    val ctx = new TestContext(deltaLog)

    val enableFeatureFn = () => {
      AlterTableProperty(property = DeltaConfigs.ROW_TRACKING_ENABLED.key, value = "true")
        .execute(ctx)
      Array.empty[Row]
    }

    val Seq(enableFuture) = runFunctionsWithOrderingFromObserver(Seq(enableFeatureFn)) {
      case (protocolUpgradeObserver :: Nil) =>
        val backfillObserver = new TransactionObserver(
          OptimisticTransactionPhases.forName("Backfill"))
        val metadataUpdateObserver = new TransactionObserver(
          OptimisticTransactionPhases.forName("Metadata Update"))
        protocolUpgradeObserver.setNextObserver(backfillObserver, autoAdvance = true)
        backfillObserver.setNextObserver(metadataUpdateObserver, autoAdvance = true)

        prepareAndCommitWithNextObserverSet(protocolUpgradeObserver)
        prepareAndCommitWithNextObserverSet(backfillObserver)

        val concurrentTxn = if (concurrentTxnName == "alterTableProperty") {
          AlterTableProperty(
            property = DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.key, value = "true")
        } else {
          Delete(rows = Seq(90L))
        }
        concurrentTxn.start(ctx)
        concurrentTxn
          .observer
          .foreach(o => busyWaitFor(o.phases.commitPhase.hasReached, timeout))
        prepareAndCommit(metadataUpdateObserver)

        val expectException = concurrentTxnName == "alterTableProperty"
        if (expectException) {
          val e = intercept[org.apache.spark.SparkException] {
            concurrentTxn.commit(ctx)
          }.getCause.asInstanceOf[DeltaThrowable]
          assert(e.getErrorClass() === "DELTA_METADATA_CHANGED")
        } else {
          concurrentTxn.commit(ctx)
        }
    }
    ThreadUtils.awaitResult(enableFuture, timeout)
    assert(DeltaConfigs.ROW_TRACKING_ENABLED.fromMetaData(
        deltaLog.update(catalogTableOpt = Some(catalogTable)).metadata))
  }

  for (withUnset <- BOOLEAN_DOMAIN)
  test(s"Disable row tracking feature - withUnset: $withUnset") {
    val (deltaLog, _) = createTestTable()
    val ctx = new TestContext(deltaLog)
    AlterTableProperty(property = DeltaConfigs.ROW_TRACKING_ENABLED.key, value = "true")
      .execute(ctx)

    val businessTxn = Delete(rows = Seq(90L))
    val disableTxn = if (withUnset) {
      UnsetTableProperty(property = DeltaConfigs.ROW_TRACKING_ENABLED.key)
    } else {
      AlterTableProperty(property = DeltaConfigs.ROW_TRACKING_ENABLED.key, value = "false")
    }

    businessTxn.start(ctx)
    disableTxn.execute(ctx)
    val e = intercept[org.apache.spark.SparkException] {
      businessTxn.commit(ctx)
    }
    assert(e.getCause.asInstanceOf[DeltaThrowable].getErrorClass() === "DELTA_METADATA_CHANGED")
  }

  test("Validate column metadata schema") {
    val (deltaLog, catalogTable) = createTestTable()
    val schema = deltaLog.update(catalogTableOpt = Some(catalogTable)).metadata.schema
    assert(schema.fields.head.productArity === 4,
      """
        |Got a non expected field column arity.
        |If extending the StructField schema please check validateSchemaChanges.
        |""".stripMargin)
  }

  for (mode <- Seq(NameMapping, IdMapping))
  test(s"Create table with column mapping - mode: ${mode.name}") {
    val (deltaLog, catalogTable) = createTestTable(
      properties = Seq(s"'${DeltaConfigs.COLUMN_MAPPING_MODE.key}' = '${mode.name}'"))

    deltaLog.update(catalogTableOpt = Some(catalogTable)).allFiles.collect().foreach { addFile =>
      val footer = getParquetFooter(deltaLog, addFile)
      validateFooter(footer, expected = true)
    }
  }

  for (txnInterleaved <- BOOLEAN_DOMAIN)
  test(s"Enable column mapping feature - txnInterleaved: $txnInterleaved") {
    val (deltaLog, catalogTable) = createTestTable()
    val ctx = new TestContext(deltaLog)

    val columnMappingEnablementTxn = AlterTableProperty(
      property = DeltaConfigs.COLUMN_MAPPING_MODE.key, value = NameMapping.name)
    val businessTxn = Update(rows = Seq(90L), setValue = -1L)

    if (txnInterleaved) {
      businessTxn.interleave(ctx) {
        columnMappingEnablementTxn.execute(ctx)
      }
    } else {
      columnMappingEnablementTxn.execute(ctx)
    }

    val metadata = deltaLog.update(catalogTableOpt = Some(catalogTable)).metadata
    assert(metadata.columnMappingMode === NameMapping)
    assert(metadata.schema.fields.map(_.metadata).forall { m =>
      m.contains("delta.columnMapping.id") && m.contains("delta.columnMapping.physicalName")
    })

    val tableDf = io.delta.tables.DeltaTable.forName(testTableName).toDF
    val expectedResult = if (txnInterleaved) {
      Seq.range(0L, 100L).filterNot(_ == 90L) :+ -1L
    } else {
      Seq.range(0L, 100L)
    }
    assert(tableDf.orderBy("idCol").collect() === expectedResult.sorted.map(Row(_)))

    // When column mapping is enabled on an existing table we do not expect any metadata in the
    // parquet footer.
    deltaLog.update(catalogTableOpt = Some(catalogTable)).allFiles.collect().foreach { addFile =>
      val footer = getParquetFooter(deltaLog, addFile)
      validateFooter(footer, expected = false)
    }
  }

  for (startMode <- Seq(NameMapping, IdMapping, NoMapping))
  test(s"Verify invalid column mapping transitions - startMode: ${startMode.name}") {
    val (deltaLog, _) = createTestTable(
      properties = Seq(s"'${DeltaConfigs.COLUMN_MAPPING_MODE.key}' = '${startMode.name}'"))
    val ctx = new TestContext(deltaLog)

    // Not allowed transitions.
    val newMode = startMode match {
      case NameMapping => IdMapping
      case IdMapping => NameMapping
      case NoMapping => IdMapping
    }
    val e = intercept[DeltaColumnMappingUnsupportedException] {
      AlterTableProperty(property = DeltaConfigs.COLUMN_MAPPING_MODE.key, value = newMode.name)
        .execute(ctx)
    }
    checkError(
      e,
      "DELTA_UNSUPPORTED_COLUMN_MAPPING_MODE_CHANGE",
      parameters = Map("oldMode" -> startMode.name, "newMode" -> newMode.name))
  }

  for (startMode <- Seq(NameMapping, IdMapping))
  test(s"Removing column mapping mode produces conflict - startMode: ${startMode.name}") {
    val (deltaLog, catalogTable) = createTestTable(
      properties = Seq(s"'${DeltaConfigs.COLUMN_MAPPING_MODE.key}' = '${startMode.name}'"))
    val ctx = new TestContext(deltaLog)

    val columnMappingDisablementTxn = AlterTableProperty(
      property = DeltaConfigs.COLUMN_MAPPING_MODE.key, value = NoMapping.name)
    val businessTxn = Delete(rows = Seq(90L))

    businessTxn.start(ctx)
    columnMappingDisablementTxn.execute(ctx)
    val e = intercept[org.apache.spark.SparkException] {
      businessTxn.commit(ctx)
    }
    assert(e.getCause.asInstanceOf[DeltaThrowable].getErrorClass() === "DELTA_METADATA_CHANGED")
    assert(deltaLog.update(
      catalogTableOpt = Some(catalogTable)).metadata.columnMappingMode === NoMapping)
  }

  test("Column mapping enablement with RESTORE") {
    val (deltaLog, catalogTable) = createTestTable(
      properties = Seq(s"'${DeltaConfigs.COLUMN_MAPPING_MODE.key}' = '${IdMapping.name}'"))
    val ctx = new TestContext(deltaLog)

    val columnMappingEnabledVersion = deltaLog.update(catalogTableOpt = Some(catalogTable)).version

    // Disable column mapping.
    AlterTableProperty(property = DeltaConfigs.COLUMN_MAPPING_MODE.key, value = NoMapping.name)
      .execute(ctx)

    // Cannot re-enable column mapping with RESTORE.
    val e = intercept[DeltaColumnMappingUnsupportedException] {
      sql(s"RESTORE TABLE $testTableName TO VERSION AS OF $columnMappingEnabledVersion")
    }
    checkError(
      e,
      "DELTA_UNSUPPORTED_COLUMN_MAPPING_MODE_CHANGE",
      parameters = Map("oldMode" -> NoMapping.name, "newMode" -> IdMapping.name))
  }
}
