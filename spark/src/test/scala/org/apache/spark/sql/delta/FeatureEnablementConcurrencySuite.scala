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
import org.apache.spark.sql.delta.actions.{AddFile, Format, Metadata, Protocol}
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
         |'${DeltaConfigs.ROW_TRACKING_ENABLED.key}' = 'false',
         |'${DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.key}' = 'false'
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

    // Tests 5-6: V2Checkpoint per-key validator.
    // In the real conflict resolution flow, checkProtocolCompatibility() runs first and updates
    // currentTransactionInfo.protocol to the winning protocol (which includes V2Checkpoint).
    // We simulate that here by constructing a conflict checker with the upgraded protocol.
    val v2CkptProtocol = Protocol.forTableFeature(V2CheckpointTableFeature)
    val conflictCheckerWithV2Ckpt = new ConflictChecker(
      spark,
      initialCurrentTransactionInfo = dummyTransactionInfo.copy(protocol = v2CkptProtocol),
      winningCommitSummary = dummySummary,
      isolationLevel = WriteSerializable)

    // Test 5: delta.checkpointPolicy=v2 is valid when the protocol supports V2Checkpoint.
    val result5 = conflictCheckerWithV2Ckpt.checkConfigurationChangesForConflicts(
      currentMetadata = Metadata(configuration = Map.empty),
      winningMetadata = Metadata(configuration =
        Map(DeltaConfigs.CHECKPOINT_POLICY.key -> CheckpointPolicy.V2.name)),
      allowList = Set(DeltaConfigs.CHECKPOINT_POLICY.key))
    assert(result5.areValid)

    // Test 6: An unknown checkpointPolicy value is rejected by the per-key validator.
    val result6 = conflictCheckerWithV2Ckpt.checkConfigurationChangesForConflicts(
      currentMetadata = Metadata(configuration = Map.empty),
      winningMetadata = Metadata(configuration =
        Map(DeltaConfigs.CHECKPOINT_POLICY.key -> "unknown-policy")),
      allowList = Set(DeltaConfigs.CHECKPOINT_POLICY.key))
    assert(!result6.areValid)

    // Tests 7-13: ICT per-key validators.
    val ictProtocol = Protocol.forTableFeature(InCommitTimestampTableFeature)
    val conflictCheckerWithICT = new ConflictChecker(
      spark,
      initialCurrentTransactionInfo = dummyTransactionInfo.copy(protocol = ictProtocol),
      winningCommitSummary = dummySummary,
      isolationLevel = WriteSerializable)
    val ictAllowList = InCommitTimestampUtils.TABLE_PROPERTY_KEYS.toSet

    // Test 7: Enabling ICT is valid when the protocol supports InCommitTimestampTableFeature.
    val result7 = conflictCheckerWithICT.checkConfigurationChangesForConflicts(
      currentMetadata = Metadata(configuration = Map.empty),
      winningMetadata = Metadata(configuration =
        Map(DeltaConfigs.IN_COMMIT_TIMESTAMPS_ENABLED.key -> "true")),
      allowList = ictAllowList)
    assert(result7.areValid)

    // Test 8: Enabling ICT is invalid when the protocol does not support
    // InCommitTimestampTableFeature.
    val result8 = conflictChecker.checkConfigurationChangesForConflicts(
      currentMetadata = Metadata(configuration = Map.empty),
      winningMetadata = Metadata(configuration =
        Map(DeltaConfigs.IN_COMMIT_TIMESTAMPS_ENABLED.key -> "true")),
      allowList = ictAllowList)
    assert(!result8.areValid)

    // Test 9: Disabling ICT (value = false) is invalid -- validator only permits enablement.
    val result9 = conflictCheckerWithICT.checkConfigurationChangesForConflicts(
      currentMetadata = Metadata(configuration = Map.empty),
      winningMetadata = Metadata(configuration =
        Map(DeltaConfigs.IN_COMMIT_TIMESTAMPS_ENABLED.key -> "false")),
      allowList = ictAllowList)
    assert(!result9.areValid)

    // Test 10: Adding enablementVersion is valid (isNew = true).
    val result10 = conflictCheckerWithICT.checkConfigurationChangesForConflicts(
      currentMetadata = Metadata(configuration = Map.empty),
      winningMetadata = Metadata(configuration =
        Map(DeltaConfigs.IN_COMMIT_TIMESTAMP_ENABLEMENT_VERSION.key -> "5")),
      allowList = ictAllowList)
    assert(result10.areValid)

    // Test 11: Changing enablementVersion is invalid -- the add-only validator rejects overwrites.
    val result11 = conflictCheckerWithICT.checkConfigurationChangesForConflicts(
      currentMetadata = Metadata(configuration =
        Map(DeltaConfigs.IN_COMMIT_TIMESTAMP_ENABLEMENT_VERSION.key -> "3")),
      winningMetadata = Metadata(configuration =
        Map(DeltaConfigs.IN_COMMIT_TIMESTAMP_ENABLEMENT_VERSION.key -> "5")),
      allowList = ictAllowList)
    assert(!result11.areValid)

    // Test 12: Adding enablementTimestamp is valid (isNew = true).
    val result12 = conflictCheckerWithICT.checkConfigurationChangesForConflicts(
      currentMetadata = Metadata(configuration = Map.empty),
      winningMetadata = Metadata(configuration =
        Map(DeltaConfigs.IN_COMMIT_TIMESTAMP_ENABLEMENT_TIMESTAMP.key -> "1000")),
      allowList = ictAllowList)
    assert(result12.areValid)

    // Test 13: Changing enablementTimestamp is invalid -- the add-only validator rejects
    // overwrites.
    val result13 = conflictCheckerWithICT.checkConfigurationChangesForConflicts(
      currentMetadata = Metadata(configuration =
        Map(DeltaConfigs.IN_COMMIT_TIMESTAMP_ENABLEMENT_TIMESTAMP.key -> "500")),
      winningMetadata = Metadata(configuration =
        Map(DeltaConfigs.IN_COMMIT_TIMESTAMP_ENABLEMENT_TIMESTAMP.key -> "1000")),
      allowList = ictAllowList)
    assert(!result13.areValid)

  }

  def testFeatureDisablement(property: String, withUnset: Boolean): Unit = {
    val (deltaLog, _) = createTestTable()
    val ctx = new TestContext(deltaLog)
    AlterTableProperty(property, value = "true")
      .execute(ctx)

    val businessTxn = Delete(rows = Seq(90L))
    val disableTxn = if (withUnset) {
      UnsetTableProperty(property)
    } else {
      AlterTableProperty(property, value = "false")
    }

    businessTxn.start(ctx)
    disableTxn.execute(ctx)
    val e = intercept[org.apache.spark.SparkException] {
      businessTxn.commit(ctx)
    }
    assert(e.getCause.asInstanceOf[DeltaThrowable].getErrorClass() === "DELTA_METADATA_CHANGED")
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
    val (deltaLog, catalogTable) = createTestTable()
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
    testFeatureDisablement(DeltaConfigs.ROW_TRACKING_ENABLED.key, withUnset)
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

  private def assertCheckpointFormat(
      deltaLog: DeltaLog, version: Long, isV2: Boolean): Unit = {
    val files = deltaLog.listFrom(version)
      .filter(FileNames.isCheckpointFile)
      .filter(f => CheckpointInstance(f.getPath).version == version)
      .toList
    assert(files.nonEmpty)
    if (isV2) {
      assert(files.forall(f =>
        CheckpointInstance(f.getPath).format == CheckpointInstance.Format.V2))
    } else {
      assert(files.forall { f =>
        val fmt = CheckpointInstance(f.getPath).format
        fmt == CheckpointInstance.Format.SINGLE || fmt == CheckpointInstance.Format.WITH_PARTS
      })
    }
  }

  ///////////////////////////////////////////////////////////////////
  // V2CheckpointTableFeature enablement: conflict resolution tests.//
  ///////////////////////////////////////////////////////////////////
  test("V2CheckpointTableFeature allows concurrent txns at upgrade") {
    assert(!V2CheckpointTableFeature.failConcurrentTransactionsAtUpgrade)
  }

  test("v2Checkpoint enablement win: concurrent DML rebases successfully " +
       "and writes V2 checkpoint") {
    val (deltaLog, _) = createTestTable(
      properties = Seq(s"'${DeltaConfigs.CHECKPOINT_INTERVAL.key}' = '1'"))
    val ctx = new TestContext(deltaLog)
    val v2CkptEnablementTxn = AlterTableProperty(
      property = DeltaConfigs.CHECKPOINT_POLICY.key, value = CheckpointPolicy.V2.name)
    val businessTxn = Delete(rows = Seq(90L))
    businessTxn.interleave(ctx) { v2CkptEnablementTxn.execute(ctx) }

    val snapshot = deltaLog.update()
    assert(snapshot.version === 3)
    checkAnswer(
      spark.table(testTableName).select("idCol"),
      (0L until 100L).filter(_ != 90L).map(Row(_)))
    assert(DeltaConfigs.CHECKPOINT_POLICY.fromMetaData(snapshot.metadata).needsV2CheckpointSupport)
    assertCheckpointFormat(deltaLog, snapshot.version, isV2 = true)
  }

  test("Checkpoint format invariant (a): all paths write V1 before v2Checkpoint enablement") {
    val (deltaLog, _) = createTestTable(
      properties = Seq(s"'${DeltaConfigs.CHECKPOINT_INTERVAL.key}' = '1'"))
    sql(s"DELETE FROM $testTableName WHERE idCol = 90")
    val preEnablementVersion = deltaLog.update().version
    assert(preEnablementVersion === 2)
    checkAnswer(
      spark.table(testTableName).select("idCol"),
      (0L until 100L).filter(_ != 90L).map(Row(_)))
    deltaLog.createCheckpointAtVersion(preEnablementVersion)
    deltaLog.checkpoint(deltaLog.update())
    assertCheckpointFormat(deltaLog, preEnablementVersion, isV2 = false)
  }

  test("Checkpoint format invariant (b): all paths write V2 at v2Checkpoint enablement version") {
    val (deltaLog, _) = createTestTable(
      properties = Seq(s"'${DeltaConfigs.CHECKPOINT_INTERVAL.key}' = '1'"))
    sql(s"ALTER TABLE $testTableName SET TBLPROPERTIES " +
      s"('${DeltaConfigs.CHECKPOINT_POLICY.key}' = '${CheckpointPolicy.V2.name}')")
    val enablementVersion = deltaLog.update().version
    assert(enablementVersion === 2)
    deltaLog.createCheckpointAtVersion(enablementVersion)
    deltaLog.checkpoint(deltaLog.update())
    assertCheckpointFormat(deltaLog, enablementVersion, isV2 = true)
  }

  test("Checkpoint format invariant (c): all paths write V2 after v2Checkpoint enablement " +
      "(including rebased concurrent DML)") {
    val (deltaLog, _) = createTestTable(
      properties = Seq(s"'${DeltaConfigs.CHECKPOINT_INTERVAL.key}' = '1'"))
    val ctx = new TestContext(deltaLog)
    val v2CkptEnablementTxn = AlterTableProperty(
      property = DeltaConfigs.CHECKPOINT_POLICY.key, value = CheckpointPolicy.V2.name)
    val businessTxn = Delete(rows = Seq(90L))
    // v2Checkpoint enablement commits first; DML rebases and commits at enablementVersion+1.
    businessTxn.interleave(ctx) { v2CkptEnablementTxn.execute(ctx) }
    val dmlVersion = deltaLog.update().version
    assert(dmlVersion === 3)
    checkAnswer(
      spark.table(testTableName).select("idCol"),
      (0L until 100L).filter(_ != 90L).map(Row(_)))
    deltaLog.createCheckpointAtVersion(dmlVersion)
    deltaLog.checkpoint(deltaLog.update())
    assertCheckpointFormat(deltaLog, dmlVersion, isV2 = true)
  }

  for (withUnset <- BOOLEAN_DOMAIN)
  test(s"Disable v2Checkpoint feature - withUnset: $withUnset") {
    val (deltaLog, _) = createTestTable()
    val ctx = new TestContext(deltaLog)
    AlterTableProperty(
      property = DeltaConfigs.CHECKPOINT_POLICY.key,
      value = CheckpointPolicy.V2.name).execute(ctx)

    val businessTxn = Delete(rows = Seq(90L))
    val disableTxn = if (withUnset) {
      UnsetTableProperty(DeltaConfigs.CHECKPOINT_POLICY.key)
    } else {
      AlterTableProperty(
        property = DeltaConfigs.CHECKPOINT_POLICY.key,
        value = CheckpointPolicy.Classic.name)
    }

    businessTxn.start(ctx)
    disableTxn.execute(ctx)
    val e = intercept[org.apache.spark.SparkException] {
      businessTxn.commit(ctx)
    }
    assert(e.getCause.asInstanceOf[DeltaThrowable].getErrorClass() === "DELTA_METADATA_CHANGED")
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

  test("Enable deletion vectors feature") {
    val (deltaLog, _) = createTestTable()
    val ctx = new TestContext(deltaLog)

    val dvEnablementTxn = AlterTableProperty(
      property = DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.key, value = "true")
    val businessTxn = Delete(rows = Seq(90L))

    businessTxn.interleave(ctx) {
      dvEnablementTxn.execute(ctx)
    }

    assert(DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.fromMetaData(deltaLog.update().metadata))
  }

  for (withUnset <- BOOLEAN_DOMAIN)
  test(s"Disable Deletion Vectors feature - withUnset: $withUnset") {
    testFeatureDisablement(DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.key, withUnset)
  }
  ///////////////////////////////////////////////////////////////////
  // ICT enablement: end-to-end conflict resolution. Positive cases//
  // verify DML rebases over ICT enablement; negative cases        //
  // verify unresolvable conflicts still fail.                     //
  ///////////////////////////////////////////////////////////////////
  test("Enable ICT feature - DML succeeds when interleaved with ICT enablement") {
    withSQLConf(
        DeltaConfigs.IN_COMMIT_TIMESTAMPS_ENABLED.defaultTablePropertyKey -> "false") {
      val (deltaLog, _) = createTestTable()
      val ctx = new TestContext(deltaLog)

      val ictEnablementTxn = AlterTableProperty(
        property = DeltaConfigs.IN_COMMIT_TIMESTAMPS_ENABLED.key, value = "true")
      val businessTxn = Delete(rows = Seq(90L))

        businessTxn.interleave(ctx) {
          ictEnablementTxn.execute(ctx)
        }

      val snapshot = deltaLog.update()
      assert(DeltaConfigs.IN_COMMIT_TIMESTAMPS_ENABLED.fromMetaData(snapshot.metadata))
      // The DML must have conflicted and retried.
      val dmlVersion = snapshot.version
      val enablementVersion = dmlVersion - 1
      // ICT timestamps must be strictly monotone across the enablement boundary (Fix 2 + Fix 3).
      assert(InCommitTimestampTestUtils.getInCommitTimestamp(deltaLog, dmlVersion) >
          InCommitTimestampTestUtils.getInCommitTimestamp(deltaLog, enablementVersion))
      // Provenance written by the enablement commit must not be overwritten by the rebased
      // DML commit (Fix 4).
      val observedEnablementVersion =
        DeltaConfigs.IN_COMMIT_TIMESTAMP_ENABLEMENT_VERSION.fromMetaData(snapshot.metadata)
      val observedEnablementTimestamp =
        DeltaConfigs.IN_COMMIT_TIMESTAMP_ENABLEMENT_TIMESTAMP.fromMetaData(snapshot.metadata)
      assert(observedEnablementVersion.contains(enablementVersion))
      assert(observedEnablementTimestamp.contains(
        InCommitTimestampTestUtils.getInCommitTimestamp(deltaLog, enablementVersion)))
    }
  }


  for (withUnset <- BOOLEAN_DOMAIN)
  test(s"Disable ICT feature - withUnset: $withUnset") {
    withSQLConf(
        DeltaConfigs.IN_COMMIT_TIMESTAMPS_ENABLED.defaultTablePropertyKey -> "false") {
      val (deltaLog, _) = createTestTable()
      val ctx = new TestContext(deltaLog)
      AlterTableProperty(DeltaConfigs.IN_COMMIT_TIMESTAMPS_ENABLED.key, value = "true")
        .execute(ctx)

      val businessTxn = Delete(rows = Seq(90L))
      val disableTxn = if (withUnset) {
        UnsetTableProperty(DeltaConfigs.IN_COMMIT_TIMESTAMPS_ENABLED.key)
      } else {
        AlterTableProperty(DeltaConfigs.IN_COMMIT_TIMESTAMPS_ENABLED.key, value = "false")
      }

      businessTxn.start(ctx)
      disableTxn.execute(ctx)
      val e = intercept[org.apache.spark.SparkException] {
        businessTxn.commit(ctx)
      }
      assert(e.getCause.asInstanceOf[DeltaThrowable].getErrorClass() === "DELTA_METADATA_CHANGED")
    }
  }

  // Unit test for the ICT-enablement detection predicate.
  test("didCurrentTransactionEnableICT - MetadataWithVersion consecutive-version pairs") {
    val ictOn = Metadata(configuration =
      Map(DeltaConfigs.IN_COMMIT_TIMESTAMPS_ENABLED.key -> "true"))
    val ictOff = Metadata(configuration = Map.empty)

    // Core of Fix 4: ICT enabled at both N and N+1 -> false.
    // This is the shape ConflictChecker passes after copying the winning commit's metadata:
    // both positions carry the same ICT-enabled metadata, so the DML did not enable ICT.
    assert(!InCommitTimestampUtils.didCurrentTransactionEnableICT(
      InCommitTimestampUtils.MetadataWithVersion(5, ictOn),
      InCommitTimestampUtils.MetadataWithVersion(4, ictOn)))

    // Normal enablement: ICT enabled at N+1, disabled at N -> true.
    assert(InCommitTimestampUtils.didCurrentTransactionEnableICT(
      InCommitTimestampUtils.MetadataWithVersion(5, ictOn),
      InCommitTimestampUtils.MetadataWithVersion(4, ictOff)))

    // ICT not enabled at either version -> false.
    assert(!InCommitTimestampUtils.didCurrentTransactionEnableICT(
      InCommitTimestampUtils.MetadataWithVersion(5, ictOff),
      InCommitTimestampUtils.MetadataWithVersion(4, ictOff)))

    // version = -1 (InitialSnapshot sentinel): treated as not previously enabled regardless
    // of metadata content, so a first-commit enablement returns true.
    assert(InCommitTimestampUtils.didCurrentTransactionEnableICT(
      InCommitTimestampUtils.MetadataWithVersion(0, ictOn),
      InCommitTimestampUtils.MetadataWithVersion(-1, ictOn)))
  }

  // A blind-append Insert started before ICT is enabled must receive a valid ICT after
  // rebasing over the enablement commit, even though it has no metadata conflict.
  test("Enable ICT feature - Insert (blind append) interleaved with ICT enablement") {
    withSQLConf(
        DeltaConfigs.IN_COMMIT_TIMESTAMPS_ENABLED.defaultTablePropertyKey -> "false") {
      val (deltaLog, _) = createTestTable()
      val ctx = new TestContext(deltaLog)

      val ictEnablementTxn = AlterTableProperty(
        property = DeltaConfigs.IN_COMMIT_TIMESTAMPS_ENABLED.key, value = "true")
      val insertTxn = Insert(rows = Seq.range(100, 110), partitionColumn = None)

      insertTxn.interleave(ctx) {
        ictEnablementTxn.execute(ctx)
      }

      val snapshot = deltaLog.update()
      assert(DeltaConfigs.IN_COMMIT_TIMESTAMPS_ENABLED.fromMetaData(snapshot.metadata))
      val insertVersion = snapshot.version
      val enablementVersion = insertVersion - 1
      assert(InCommitTimestampTestUtils.getInCommitTimestamp(deltaLog, insertVersion) >
          InCommitTimestampTestUtils.getInCommitTimestamp(deltaLog, enablementVersion))
    }
  }

  // Same as the Delete variant ("Enable ICT feature - DML succeeds when interleaved with ICT
  // enablement") but exercises the Update code path.
  test("Enable ICT feature - Update DML interleaved with ICT enablement") {
    withSQLConf(
        DeltaConfigs.IN_COMMIT_TIMESTAMPS_ENABLED.defaultTablePropertyKey -> "false") {
      val (deltaLog, _) = createTestTable()
      val ctx = new TestContext(deltaLog)

      val ictEnablementTxn = AlterTableProperty(
        property = DeltaConfigs.IN_COMMIT_TIMESTAMPS_ENABLED.key, value = "true")
      val businessTxn = Update(rows = Seq(90L), setValue = -1L)

        businessTxn.interleave(ctx) {
          ictEnablementTxn.execute(ctx)
        }

      val snapshot = deltaLog.update()
      assert(DeltaConfigs.IN_COMMIT_TIMESTAMPS_ENABLED.fromMetaData(snapshot.metadata))
      val dmlVersion = snapshot.version
      val enablementVersion = dmlVersion - 1
      assert(InCommitTimestampTestUtils.getInCommitTimestamp(deltaLog, dmlVersion) >
          InCommitTimestampTestUtils.getInCommitTimestamp(deltaLog, enablementVersion))
      val observedEnablementVersion =
        DeltaConfigs.IN_COMMIT_TIMESTAMP_ENABLEMENT_VERSION.fromMetaData(snapshot.metadata)
      val observedEnablementTimestamp =
        DeltaConfigs.IN_COMMIT_TIMESTAMP_ENABLEMENT_TIMESTAMP.fromMetaData(snapshot.metadata)
      assert(observedEnablementVersion.contains(enablementVersion))
      assert(observedEnablementTimestamp.contains(
        InCommitTimestampTestUtils.getInCommitTimestamp(deltaLog, enablementVersion)))
    }
  }

  // Both ICT and DV enablement carry a Protocol change. Whichever loses gets
  // ProtocolChangedException; the ICT enablement does not override protocol-conflict abort.
  test("ICT enablement aborted by concurrent DV enablement: ProtocolChangedException") {
    withSQLConf(
        DeltaConfigs.IN_COMMIT_TIMESTAMPS_ENABLED.defaultTablePropertyKey -> "false") {
      val (deltaLog, _) = createTestTable()
      val ctx = new TestContext(deltaLog)

      val ictEnablementTxn = AlterTableProperty(
        property = DeltaConfigs.IN_COMMIT_TIMESTAMPS_ENABLED.key, value = "true")
      val dvEnablementTxn = AlterTableProperty(
        property = DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.key, value = "true")

      ictEnablementTxn.start(ctx)
      dvEnablementTxn.execute(ctx)
      val e = intercept[org.apache.spark.SparkException] {
        ictEnablementTxn.commit(ctx)
      }
      assert(e.getCause.isInstanceOf[io.delta.exceptions.ProtocolChangedException])
    }
  }

  // The ICT enablement conflict resolver only allows rebasing over ICT-only metadata changes.
  // When the winning commit also changes a non-allowlisted config key, the DML must still
  // fail with DELTA_METADATA_CHANGED.
  test("Enable ICT feature - DML fails when ICT enablement also modifies non-allowlisted config") {
    withSQLConf(
        DeltaConfigs.IN_COMMIT_TIMESTAMPS_ENABLED.defaultTablePropertyKey -> "false") {
      val (deltaLog, _) = createTestTable()
      val ctx = new TestContext(deltaLog)
      val businessTxn = Delete(rows = Seq(90L))

      businessTxn.start(ctx)
      // The winning commit enables ICT together with a non-ICT config change.
      sql(s"""ALTER TABLE $testTableName SET TBLPROPERTIES (
             |  '${DeltaConfigs.IN_COMMIT_TIMESTAMPS_ENABLED.key}' = 'true',
             |  '${DeltaConfigs.CHECKPOINT_INTERVAL.key}' = '20'
             |)""".stripMargin)
      val e = intercept[org.apache.spark.SparkException] {
        businessTxn.commit(ctx)
      }
      assert(e.getCause.asInstanceOf[DeltaThrowable].getErrorClass() === "DELTA_METADATA_CHANGED")
    }
  }

  // After the DML retries and commits over the ICT enablement, the data change must be
  // correctly applied: no rows double-deleted or silently dropped.
  test("Enable ICT feature - data integrity preserved after DML retries over ICT enablement") {
    withSQLConf(
        DeltaConfigs.IN_COMMIT_TIMESTAMPS_ENABLED.defaultTablePropertyKey -> "false") {
      val (deltaLog, _) = createTestTable()
      val ctx = new TestContext(deltaLog)

      val ictEnablementTxn = AlterTableProperty(
        property = DeltaConfigs.IN_COMMIT_TIMESTAMPS_ENABLED.key, value = "true")
      val deleteTxn = Delete(rows = Seq(42L))

        deleteTxn.interleave(ctx) {
          ictEnablementTxn.execute(ctx)
        }

      val remaining = spark.table(testTableName).collect().map(_.getLong(0)).toSet
      assert(!remaining.contains(42L), "deleted row 42 must not appear after retry")
      assert((0L until 100L).filterNot(_ == 42L).forall(remaining.contains),
        "all other rows 0-99 (except 42) must still be present")
    }
  }

  // Both the loser and winner have metadata updates, so attemptToResolveMetadataConflicts
  // rejects the loser immediately before the ICT allowlist check runs.
  test("Enable ICT feature - metadata-changing loser fails with DELTA_METADATA_CHANGED") {
    withSQLConf(
        DeltaConfigs.IN_COMMIT_TIMESTAMPS_ENABLED.defaultTablePropertyKey -> "false") {
      val (deltaLog, _) = createTestTable()
      val ctx = new TestContext(deltaLog)

      val ictEnablementTxn = AlterTableProperty(
        property = DeltaConfigs.IN_COMMIT_TIMESTAMPS_ENABLED.key, value = "true")
      val metadataLoserTxn = AlterTableProperty(
        property = DeltaConfigs.CHECKPOINT_INTERVAL.key, value = "20")

      metadataLoserTxn.start(ctx)
      ictEnablementTxn.execute(ctx)
      val e = intercept[org.apache.spark.SparkException] {
        metadataLoserTxn.commit(ctx)
      }
      assert(e.getCause.asInstanceOf[DeltaThrowable].getErrorClass() === "DELTA_METADATA_CHANGED")
    }
  }

  // Both the loser and winner carry Protocol actions; ProtocolChangedException fires before
  // the ICT metadata allowlist is consulted.
  test("Enable ICT feature - protocol-changing loser fails with ProtocolChangedException") {
    withSQLConf(
        DeltaConfigs.IN_COMMIT_TIMESTAMPS_ENABLED.defaultTablePropertyKey -> "false") {
      val (deltaLog, _) = createTestTable()
      val ctx = new TestContext(deltaLog)

      val ictEnablementTxn = AlterTableProperty(
        property = DeltaConfigs.IN_COMMIT_TIMESTAMPS_ENABLED.key, value = "true")
      val dvEnablementTxn = AlterTableProperty(
        property = DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.key, value = "true")

      dvEnablementTxn.start(ctx)
      ictEnablementTxn.execute(ctx)
      val e = intercept[org.apache.spark.SparkException] {
        dvEnablementTxn.commit(ctx)
      }
      assert(e.getCause.isInstanceOf[io.delta.exceptions.ProtocolChangedException])
    }
  }

  test("Enable ICT feature - concurrent enablements: loser fails with ProtocolChangedException") {
    withSQLConf(
        DeltaConfigs.IN_COMMIT_TIMESTAMPS_ENABLED.defaultTablePropertyKey -> "false") {
      val (deltaLog, _) = createTestTable()
      val ctx = new TestContext(deltaLog)

      val userEnablementTxn = AlterTableProperty(
        property = DeltaConfigs.IN_COMMIT_TIMESTAMPS_ENABLED.key, value = "true")
      val afeEnablementTxn = AlterTableProperty(
        property = DeltaConfigs.IN_COMMIT_TIMESTAMPS_ENABLED.key, value = "true")

      userEnablementTxn.start(ctx)
      afeEnablementTxn.execute(ctx) // enablement wins at N+1
      val e = intercept[org.apache.spark.SparkException] {
        userEnablementTxn.commit(ctx)
      }
      // Both loser and winner carry a Protocol action -> checkProtocolCompatibility throws
      // before failConcurrentTransactionsAtUpgrade or the ICT allowlist is consulted.
      assert(e.getCause.isInstanceOf[io.delta.exceptions.ProtocolChangedException])

      // Winner's commit stands: ICT is correctly enabled with provenance intact.
      val snapshot = deltaLog.update()
      assert(DeltaConfigs.IN_COMMIT_TIMESTAMPS_ENABLED.fromMetaData(snapshot.metadata))
      assert(DeltaConfigs.IN_COMMIT_TIMESTAMP_ENABLEMENT_VERSION
        .fromMetaData(snapshot.metadata).isDefined)
      assert(DeltaConfigs.IN_COMMIT_TIMESTAMP_ENABLEMENT_TIMESTAMP
        .fromMetaData(snapshot.metadata).isDefined)
    }
  }

}
