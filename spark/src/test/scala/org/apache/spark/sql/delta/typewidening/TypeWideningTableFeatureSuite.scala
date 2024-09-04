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

package org.apache.spark.sql.delta.typewidening

import java.io.{File, PrintWriter}
import java.util.concurrent.TimeUnit

import com.databricks.spark.util.Log4jUsageLogger
import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.DeltaOperations.ManualUpdate
import org.apache.spark.sql.delta.actions.TableFeatureProtocolUtils.propertyKey
import org.apache.spark.sql.delta.rowtracking.RowTrackingTestUtils
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.JsonUtils
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkException
import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.util.ManualClock

/**
 * Test suite covering adding and removing the type widening table feature. Dropping the table
 * feature also includes rewriting data files with the old type and removing type widening metadata.
 */
class TypeWideningTableFeatureSuite
  extends QueryTest
    with TypeWideningTestMixin
    with TypeWideningDropFeatureTestMixin
    with TypeWideningTableFeatureTests

trait TypeWideningTableFeatureTests extends RowTrackingTestUtils with TypeWideningTestCases {
  self: QueryTest
    with TypeWideningTestMixin
    with TypeWideningDropFeatureTestMixin =>

  import testImplicits._

  /** Clock used to advance past the retention period when dropping the table feature. */
  var clock: ManualClock = _

  protected def setupManualClock(): Unit = {
    clock = new ManualClock(System.currentTimeMillis())
    // Override the (cached) delta log with one using our manual clock.
    DeltaLog.clearCache()
    deltaLog = DeltaLog.forTable(spark, new Path(tempPath), clock)
  }

  /**
   * Use this after dropping the table feature to artificially move the current time to after
   * the table retention period.
   */
  def advancePastRetentionPeriod(): Unit = {
    assert(clock != null, "Must call setupManualClock in tests that are using this method.")
    clock.advance(
      deltaLog.deltaRetentionMillis(deltaLog.update().metadata) +
        TimeUnit.MINUTES.toMillis(5))
  }

  test("enable type widening at table creation then disable it") {
    sql(s"CREATE TABLE delta.`$tempPath` (a int) USING DELTA " +
      s"TBLPROPERTIES ('${DeltaConfigs.ENABLE_TYPE_WIDENING.key}' = 'true')")
    assert(isTypeWideningSupported)
    assert(isTypeWideningEnabled)
    enableTypeWidening(tempPath, enabled = false)
    assert(isTypeWideningSupported)
    assert(!isTypeWideningEnabled)
  }

  test("enable type widening after table creation then disable it") {
    sql(s"CREATE TABLE delta.`$tempPath` (a int) USING DELTA " +
      s"TBLPROPERTIES ('${DeltaConfigs.ENABLE_TYPE_WIDENING.key}' = 'false')")
    assert(!isTypeWideningSupported)
    assert(!isTypeWideningEnabled)
    // Setting the property to false shouldn't add the table feature if it's not present.
    enableTypeWidening(tempPath, enabled = false)
    assert(!isTypeWideningSupported)
    assert(!isTypeWideningEnabled)

    enableTypeWidening(tempPath)
    assert(isTypeWideningSupported)
    assert(isTypeWideningEnabled)
    enableTypeWidening(tempPath, enabled = false)
    assert(isTypeWideningSupported)
    assert(!isTypeWideningEnabled)
  }

  test("set table property to incorrect value") {
    val ex = intercept[IllegalArgumentException] {
      sql(s"CREATE TABLE delta.`$tempPath` (a int) USING DELTA " +
        s"TBLPROPERTIES ('${DeltaConfigs.ENABLE_TYPE_WIDENING.key}' = 'bla')")
    }
    assert(ex.getMessage.contains("For input string: \"bla\""))
    sql(s"CREATE TABLE delta.`$tempPath` (a int) USING DELTA " +
       s"TBLPROPERTIES ('${DeltaConfigs.ENABLE_TYPE_WIDENING.key}' = 'false')")
    checkError(
      exception = intercept[SparkException] {
        sql(s"ALTER TABLE delta.`$tempPath` " +
          s"SET TBLPROPERTIES ('${DeltaConfigs.ENABLE_TYPE_WIDENING.key}' = 'bla')")
      },
      errorClass = "_LEGACY_ERROR_TEMP_2045",
      parameters = Map(
        "message" -> "For input string: \"bla\""
      )
    )
    assert(!isTypeWideningSupported)
    assert(!isTypeWideningEnabled)
  }

  test("change column type without table feature") {
    sql(s"CREATE TABLE delta.`$tempPath` (a TINYINT) USING DELTA " +
      s"TBLPROPERTIES ('${DeltaConfigs.ENABLE_TYPE_WIDENING.key}' = 'false')")

    checkError(
      exception = intercept[AnalysisException] {
        sql(s"ALTER TABLE delta.`$tempPath` CHANGE COLUMN a TYPE SMALLINT")
      },
      errorClass = "DELTA_UNSUPPORTED_ALTER_TABLE_CHANGE_COL_OP",
      parameters = Map(
        "fieldPath" -> "a",
        "oldField" -> "TINYINT",
        "newField" -> "SMALLINT"
      )
    )
  }

  test("change column type with type widening table feature supported but table property set to " +
    "false") {
    sql(s"CREATE TABLE delta.`$tempPath` (a SMALLINT) USING DELTA")
    sql(s"ALTER TABLE delta.`$tempPath` " +
      s"SET TBLPROPERTIES ('${DeltaConfigs.ENABLE_TYPE_WIDENING.key}' = 'false')")

    checkError(
      exception = intercept[AnalysisException] {
        sql(s"ALTER TABLE delta.`$tempPath` CHANGE COLUMN a TYPE INT")
      },
      errorClass = "DELTA_UNSUPPORTED_ALTER_TABLE_CHANGE_COL_OP",
      parameters = Map(
        "fieldPath" -> "a",
        "oldField" -> "SMALLINT",
        "newField" -> "INT"
      )
    )
  }

  test("no-op type changes are always allowed") {
    sql(s"CREATE TABLE delta.`$tempPath` (a int) USING DELTA " +
      s"TBLPROPERTIES ('${DeltaConfigs.ENABLE_TYPE_WIDENING.key}' = 'false')")
    sql(s"ALTER TABLE delta.`$tempPath` CHANGE COLUMN a TYPE INT")
    enableTypeWidening(tempPath, enabled = true)
    sql(s"ALTER TABLE delta.`$tempPath` CHANGE COLUMN a TYPE INT")
    enableTypeWidening(tempPath, enabled = false)
    sql(s"ALTER TABLE delta.`$tempPath` CHANGE COLUMN a TYPE INT")
  }

  test("drop unused table feature on empty table") {
    sql(s"CREATE TABLE delta.`$tempPath` (a byte) USING DELTA")
    dropTableFeature(
      expectedOutcome = ExpectedOutcome.SUCCESS,
      expectedNumFilesRewritten = 0,
      expectedColumnTypes = Map("a" -> ByteType)
    )
    checkAnswer(readDeltaTable(tempPath), Seq.empty)
  }

  test("drop feature using sql with multipart identifier") {
    withTempDatabase { databaseName =>
      val tableName = "test_table"
      withTable(tableName) {
        sql(s"CREATE TABLE $databaseName.$tableName (a byte) USING DELTA " +
          s"TBLPROPERTIES ('${DeltaConfigs.ENABLE_TYPE_WIDENING.key}' = 'true')")
        sql(s"INSERT INTO  $databaseName.$tableName VALUES (1)")
        sql(s"ALTER TABLE $databaseName.$tableName CHANGE COLUMN a TYPE INT")
        sql(s"INSERT INTO  $databaseName.$tableName VALUES (2)")

        val deltaLog = DeltaLog.forTable(spark, TableIdentifier(tableName, Some(databaseName)))

        checkError(
          exception = intercept[DeltaTableFeatureException] {
            sql(s"ALTER TABLE $databaseName.$tableName " +
              s"DROP FEATURE '${TypeWideningPreviewTableFeature.name}'"
            ).collect()
          },
          errorClass = "DELTA_FEATURE_DROP_WAIT_FOR_RETENTION_PERIOD",
          parameters = Map(
            "feature" -> TypeWideningPreviewTableFeature.name,
            "logRetentionPeriodKey" -> DeltaConfigs.LOG_RETENTION.key,
            "logRetentionPeriod" -> DeltaConfigs.LOG_RETENTION
              .fromMetaData(deltaLog.unsafeVolatileMetadata).toString,
            "truncateHistoryLogRetentionPeriod" ->
              DeltaConfigs.TABLE_FEATURE_DROP_TRUNCATE_HISTORY_LOG_RETENTION
                .fromMetaData(deltaLog.unsafeVolatileMetadata).toString)
        )
      }
    }
  }

  // Rewriting the data when dropping the table feature relies on the default row commit version
  // being set even when row tracking isn't enabled.
  for(rowTrackingEnabled <- BOOLEAN_DOMAIN) {
    test(s"drop unused table feature on table with data, rowTrackingEnabled=$rowTrackingEnabled") {
      sql(s"CREATE TABLE delta.`$tempPath` (a byte) USING DELTA")
      addSingleFile(Seq(1, 2, 3), ByteType)
      dropTableFeature(
        expectedOutcome = ExpectedOutcome.SUCCESS,
        expectedNumFilesRewritten = 0,
        expectedColumnTypes = Map("a" -> ByteType)
      )
      checkAnswer(readDeltaTable(tempPath), Seq(Row(1), Row(2), Row(3)))
    }

    test(s"drop unused table feature on table with data inserted before adding the table feature," +
      s"rowTrackingEnabled=$rowTrackingEnabled") {
      setupManualClock()
      sql(s"CREATE TABLE delta.`$tempPath` (a byte) USING DELTA " +
        s"TBLPROPERTIES ('${DeltaConfigs.ENABLE_TYPE_WIDENING.key}' = 'false')")
      addSingleFile(Seq(1, 2, 3), ByteType)
      enableTypeWidening(tempPath)
      sql(s"ALTER TABLE delta.`$tempPath` CHANGE COLUMN a TYPE int")

      dropTableFeature(
        expectedOutcome = ExpectedOutcome.FAIL_CURRENT_VERSION_USES_FEATURE,
        expectedNumFilesRewritten = 1,
        expectedColumnTypes = Map("a" -> IntegerType)
      )
      dropTableFeature(
        expectedOutcome = ExpectedOutcome.FAIL_HISTORICAL_VERSION_USES_FEATURE,
        expectedNumFilesRewritten = 0,
        expectedColumnTypes = Map("a" -> IntegerType)
      )

      advancePastRetentionPeriod()
      dropTableFeature(
        expectedOutcome = ExpectedOutcome.SUCCESS,
        expectedNumFilesRewritten = 0,
        expectedColumnTypes = Map("a" -> IntegerType)
      )
    }

    test(s"drop table feature on table with data added only after type change, " +
      s"rowTrackingEnabled=$rowTrackingEnabled") {
      setupManualClock()
      sql(s"CREATE TABLE delta.`$tempPath` (a byte) USING DELTA")
      sql(s"ALTER TABLE delta.`$tempPath` CHANGE COLUMN a TYPE int")
      addSingleFile(Seq(1, 2, 3), IntegerType)

      // We could actually drop the table feature directly here instead of failing by checking that
      // there were no files added before the type change. This may be an expensive check for a rare
      // scenario so we don't do it.
      dropTableFeature(
        expectedOutcome = ExpectedOutcome.FAIL_CURRENT_VERSION_USES_FEATURE,
        expectedNumFilesRewritten = 0,
        expectedColumnTypes = Map("a" -> IntegerType)
      )
      dropTableFeature(
        expectedOutcome = ExpectedOutcome.FAIL_HISTORICAL_VERSION_USES_FEATURE,
        expectedNumFilesRewritten = 0,
        expectedColumnTypes = Map("a" -> IntegerType)
      )

      advancePastRetentionPeriod()
      dropTableFeature(
        expectedOutcome = ExpectedOutcome.SUCCESS,
        expectedNumFilesRewritten = 0,
        expectedColumnTypes = Map("a" -> IntegerType)
      )
      checkAnswer(readDeltaTable(tempPath), Seq(Row(1), Row(2), Row(3)))
    }

    test(s"drop table feature on table with data added before type change, " +
      s"rowTrackingEnabled=$rowTrackingEnabled") {
      setupManualClock()
      sql(s"CREATE TABLE delta.`$tempDir` (a byte) USING DELTA")
      addSingleFile(Seq(1, 2, 3), ByteType)
      sql(s"ALTER TABLE delta.`$tempDir` CHANGE COLUMN a TYPE int")

      dropTableFeature(
        expectedOutcome = ExpectedOutcome.FAIL_CURRENT_VERSION_USES_FEATURE,
        expectedNumFilesRewritten = 1,
        expectedColumnTypes = Map("a" -> IntegerType)
      )

      dropTableFeature(
        expectedOutcome = ExpectedOutcome.FAIL_HISTORICAL_VERSION_USES_FEATURE,
        expectedNumFilesRewritten = 0,
        expectedColumnTypes = Map("a" -> IntegerType)
      )

      advancePastRetentionPeriod()
      dropTableFeature(
        expectedOutcome = ExpectedOutcome.SUCCESS,
        expectedNumFilesRewritten = 0,
        expectedColumnTypes = Map("a" -> IntegerType)
      )
      checkAnswer(readDeltaTable(tempPath), Seq(Row(1), Row(2), Row(3)))
    }

    test(s"drop table feature on table with data added before type change and fully rewritten " +
      s"after, rowTrackingEnabled=$rowTrackingEnabled") {
      setupManualClock()
      sql(s"CREATE TABLE delta.`$tempDir` (a byte) USING DELTA")
      addSingleFile(Seq(1, 2, 3), ByteType)
      sql(s"ALTER TABLE delta.`$tempDir` CHANGE COLUMN a TYPE int")
      sql(s"UPDATE delta.`$tempDir` SET a = a + 10")

      dropTableFeature(
        expectedOutcome = ExpectedOutcome.FAIL_CURRENT_VERSION_USES_FEATURE,
        // The file was already rewritten in UPDATE.
        expectedNumFilesRewritten = 0,
        expectedColumnTypes = Map("a" -> IntegerType)
      )

      dropTableFeature(
        expectedOutcome = ExpectedOutcome.FAIL_HISTORICAL_VERSION_USES_FEATURE,
        expectedNumFilesRewritten = 0,
        expectedColumnTypes = Map("a" -> IntegerType)
      )

      advancePastRetentionPeriod()
      dropTableFeature(
        expectedOutcome = ExpectedOutcome.SUCCESS,
        expectedNumFilesRewritten = 0,
        expectedColumnTypes = Map("a" -> IntegerType)
      )
      checkAnswer(readDeltaTable(tempPath), Seq(Row(11), Row(12), Row(13)))
    }

    test(s"drop table feature on table with data added before type change and partially " +
      s"rewritten after, rowTrackingEnabled=$rowTrackingEnabled") {
      setupManualClock()
      withRowTrackingEnabled(rowTrackingEnabled) {
        sql(s"CREATE TABLE delta.`$tempDir` (a byte) USING DELTA")
        addSingleFile(Seq(1, 2, 3), ByteType)
        addSingleFile(Seq(4, 5, 6), ByteType)
        sql(s"ALTER TABLE delta.`$tempDir` CHANGE COLUMN a TYPE int")
        sql(s"UPDATE delta.`$tempDir` SET a = a + 10 WHERE a < 4")

        dropTableFeature(
          expectedOutcome = ExpectedOutcome.FAIL_CURRENT_VERSION_USES_FEATURE,
          // One file was already rewritten in UPDATE, leaving 1 file to rewrite.
          expectedNumFilesRewritten = 1,
          expectedColumnTypes = Map("a" -> IntegerType)
        )

        dropTableFeature(
          expectedOutcome = ExpectedOutcome.FAIL_HISTORICAL_VERSION_USES_FEATURE,
          expectedNumFilesRewritten = 0,
          expectedColumnTypes = Map("a" -> IntegerType)
        )

        advancePastRetentionPeriod()
        dropTableFeature(
          expectedOutcome = ExpectedOutcome.SUCCESS,
          expectedNumFilesRewritten = 0,
          expectedColumnTypes = Map("a" -> IntegerType)
        )
        checkAnswer(
          readDeltaTable(tempPath),
          Seq(Row(11), Row(12), Row(13), Row(4), Row(5), Row(6)))
      }
    }
  }

  for {
    testCase <- supportedTestCases
  }
  test(s"drop feature after type change ${testCase.fromType.sql} -> ${testCase.toType.sql}") {
    append(testCase.initialValuesDF.repartition(2))
    sql(s"ALTER TABLE delta.`$tempPath` CHANGE COLUMN value TYPE ${testCase.toType.sql}")
    append(testCase.additionalValuesDF.repartition(3))
    dropTableFeature(
      expectedOutcome = ExpectedOutcome.FAIL_CURRENT_VERSION_USES_FEATURE,
      expectedNumFilesRewritten = 2,
      expectedColumnTypes = Map("value" -> testCase.toType)
    )
    checkAnswer(readDeltaTable(tempPath), testCase.expectedResult)
  }

  test("drop feature after a type change with schema evolution") {
    setupManualClock()
    sql(s"CREATE TABLE delta.`$tempPath` (a byte) USING DELTA")
    addSingleFile(Seq(1), ByteType)

    withSQLConf(DeltaSQLConf.DELTA_SCHEMA_AUTO_MIGRATE.key -> "true") {
      addSingleFile(Seq(1024), IntegerType)
    }
    assert(readDeltaTable(tempPath).schema("a").dataType === IntegerType)
    checkAnswer(readDeltaTable(tempPath), Seq(Row(1), Row(1024)))

    dropTableFeature(
      expectedOutcome = ExpectedOutcome.FAIL_CURRENT_VERSION_USES_FEATURE,
      expectedNumFilesRewritten = 1,
      expectedColumnTypes = Map("a" -> IntegerType)
    )

    advancePastRetentionPeriod()
    dropTableFeature(
      expectedOutcome = ExpectedOutcome.SUCCESS,
      expectedNumFilesRewritten = 0,
      expectedColumnTypes = Map("a" -> IntegerType)
    )
    checkAnswer(readDeltaTable(tempPath), Seq(Row(1), Row(1024)))
  }

  test("unsupported type changes applied to the table") {
    sql(s"CREATE TABLE delta.`$tempDir` (a array<int>) USING DELTA")
    val metadata = new MetadataBuilder()
      .putMetadataArray("delta.typeChanges", Array(
        new MetadataBuilder()
          .putString("toType", "string")
          .putString("fromType", "int")
          .putLong("tableVersion", 2)
          .putString("fieldPath", "element")
          .build()
      )).build()

    // Add an unsupported type change to the table schema. Only an implementation that isn't
    // compliant with the feature specification would allow this.
    deltaLog.withNewTransaction { txn =>
      txn.commit(
        Seq(txn.snapshot.metadata.copy(
          schemaString = new StructType()
            .add("a", StringType, nullable = true, metadata).json
        )),
        ManualUpdate)
    }

    checkError(
      exception = intercept[DeltaIllegalStateException] {
        readDeltaTable(tempPath).collect()
      },
      errorClass = "DELTA_UNSUPPORTED_TYPE_CHANGE_IN_SCHEMA",
      parameters = Map(
        "fieldName" -> "a.element",
        "fromType" -> "INT",
        "toType" -> "STRING"
      )
    )
  }

  for (testCase <- previewOnlySupportedTestCases)
  test(s"preview only type changes can still be read after the preview - " +
    s"${testCase.fromType.sql} -> ${testCase.toType.sql}") {
    sql(s"CREATE TABLE delta.`$tempDir` (value ${testCase.fromType.sql}) USING DELTA")
    append(testCase.initialValuesDF)

    // Add a type change that was only allowed during the preview phase. We have to manually
    // commit the schema change since ALTER TABLE won't allow this anymore.
    val metadata = new MetadataBuilder()
    .putMetadataArray("delta.typeChanges", Array(
      new MetadataBuilder()
        .putString("toType", testCase.toType.typeName)
        .putString("fromType", testCase.fromType.typeName)
        .putLong("tableVersion", 2)
        .build()
    )).build()
    deltaLog.withNewTransaction { txn =>
      txn.commit(
        Seq(txn.snapshot.metadata.copy(
          schemaString = new StructType()
            .add("value", testCase.toType, nullable = true, metadata).json
        )),
        ManualUpdate)
    }

    checkAnswerWithTolerance(
      actualDf = readDeltaTable(tempPath),
      expectedDf = testCase.initialValuesDF.select($"value".cast(testCase.toType)),
      toType = testCase.toType
    )
  }

  test("type widening rewrite metrics") {
    sql(s"CREATE TABLE delta.`$tempDir` (a byte) USING DELTA")
    addSingleFile(Seq(1, 2, 3), ByteType)
    addSingleFile(Seq(4, 5, 6), ByteType)
    sql(s"ALTER TABLE delta.`$tempDir` CHANGE COLUMN a TYPE int")
    // Update a row from the second file to rewrite it. Only the first file still contains the old
    // data type after this.
    sql(s"UPDATE delta.`$tempDir` SET a = a + 10 WHERE a < 4")
    val usageLogs = Log4jUsageLogger.track {
      dropTableFeature(
        expectedOutcome = ExpectedOutcome.FAIL_CURRENT_VERSION_USES_FEATURE,
        expectedNumFilesRewritten = 1,
        expectedColumnTypes = Map("a" -> IntegerType)
      )
    }

    val metrics = filterUsageRecords(usageLogs, "delta.typeWidening.featureRemoval")
      .map(r => JsonUtils.fromJson[Map[String, String]](r.blob))
      .head

    assert(metrics("downgradeTimeMs").toLong > 0L)
    // Only the first file should get rewritten here since the second file was already rewritten
    // during the UPDATE.
    assert(metrics("numFilesRewritten").toLong === 1L)
    assert(metrics("metadataRemoved").toBoolean)
  }

  test("dropping feature after CLONE correctly rewrite files with old type") {
    withTable("source") {
      sql("CREATE TABLE source (a byte) USING delta")
      sql("INSERT INTO source VALUES (1)")
      sql("INSERT INTO source VALUES (2)")
      sql(s"ALTER TABLE source CHANGE COLUMN a TYPE INT")
      sql("INSERT INTO source VALUES (200)")
      sql(s"CREATE OR REPLACE TABLE delta.`$tempPath` SHALLOW CLONE source")
      checkAnswer(readDeltaTable(tempPath),
        Seq(Row(1), Row(2), Row(200)))

      dropTableFeature(
        expectedOutcome = ExpectedOutcome.FAIL_CURRENT_VERSION_USES_FEATURE,
        expectedNumFilesRewritten = 2,
        expectedColumnTypes = Map("a" -> IntegerType)
      )
      checkAnswer(readDeltaTable(tempPath),
        Seq(Row(1), Row(2), Row(200)))
    }
  }
  test("RESTORE to before type change") {
    addSingleFile(Seq(1), ShortType)
    sql(s"ALTER TABLE delta.`$tempPath` CHANGE COLUMN a TYPE INT")
    sql(s"UPDATE delta.`$tempPath` SET a = ${Int.MinValue} WHERE a = 1")

    // RESTORE to version 0, before the type change was applied.
    sql(s"RESTORE TABLE delta.`$tempPath` VERSION AS OF 0")
    checkAnswer(readDeltaTable(tempPath), Seq(Row(1)))
    dropTableFeature(
      // There should be no files to rewrite but versions before RESTORE still use the feature.
      expectedOutcome = ExpectedOutcome.FAIL_HISTORICAL_VERSION_USES_FEATURE,
      expectedNumFilesRewritten = 0,
      expectedColumnTypes = Map("a" -> ShortType)
    )
  }

  test("dropping feature after RESTORE correctly rewrite files with old type") {
    addSingleFile(Seq(1), ShortType)
    addSingleFile(Seq(2), ShortType)
    sql(s"ALTER TABLE delta.`$tempPath` CHANGE COLUMN a TYPE INT")
    // Delete the first file which will be added back again with RESTORE.
    sql(s"DELETE FROM delta.`$tempPath` WHERE a = 1")
    addSingleFile(Seq(Int.MinValue), IntegerType)

    // RESTORE to version 2 -> ALTER TABLE CHANGE COLUMN TYPE.
    // The type change is then still present and the first file initially added at version 0 is
    // added back during RESTORE (version 5). That file contains the old type and must be rewritten
    // when the feature is dropped.
    sql(s"RESTORE TABLE delta.`$tempPath` VERSION AS OF 2")
    checkAnswer(readDeltaTable(tempPath), Seq(Row(1), Row(2)))
    dropTableFeature(
      expectedOutcome = ExpectedOutcome.FAIL_CURRENT_VERSION_USES_FEATURE,
      // Both files added before the type change must be rewritten.
      expectedNumFilesRewritten = 2,
      expectedColumnTypes = Map("a" -> IntegerType)
    )
    checkAnswer(readDeltaTable(tempPath), Seq(Row(1), Row(2)))
  }

  test("rewriting files fails if there are corrupted files") {
    sql(s"CREATE TABLE delta.`$tempPath` (a byte) USING DELTA")
    sql(s"ALTER TABLE delta.`$tempPath` CHANGE COLUMN a TYPE INT")
    addSingleFile(Seq(2), IntegerType)
    addSingleFile(Seq(3), IntegerType)
    val filePath = deltaLog.update().allFiles.first().absolutePath(deltaLog).toUri.getPath
    val pw = new PrintWriter(filePath)
    pw.write("corrupted")
    pw.close()

    // Rewriting files when dropping type widening should ignore this config, if the corruption is
    // transient it will leave files behind that some clients can't read.
    withSQLConf(SQLConf.IGNORE_CORRUPT_FILES.key -> "true") {
      val ex = intercept[SparkException] {
        sql(s"ALTER TABLE delta.`$tempDir` DROP FEATURE '${TypeWideningPreviewTableFeature.name}'")
      }
      assert(ex.getMessage.contains("Cannot seek after EOF"))
    }
  }

  /**
   * Directly add the preview/stable type widening table feature without using the type widening
   * table property.
   */
  def addTableFeature(tablePath: String, feature: TypeWideningTableFeatureBase): Unit =
    sql(s"ALTER TABLE delta.`$tablePath` " +
      s"SET TBLPROPERTIES ('${propertyKey(feature)}' = 'supported')")

  /** Validate whether the preview stable type widening table feature are supported or not. */
  def assertFeatureSupported(preview: Boolean, stable: Boolean): Unit = {
    val protocol = deltaLog.update().protocol
    def supported(supported: Boolean): String = if (supported) "supported" else "not supported"

    assert(protocol.isFeatureSupported(TypeWideningPreviewTableFeature) === preview,
      s"Expected the preview feature to be ${supported(preview)} but it is ${supported(!preview)}.")
    assert(protocol.isFeatureSupported(TypeWideningTableFeature) === stable,
      s"Expected the stable feature to be ${supported(stable)} but it is ${supported(!stable)}.")
    assert(TypeWidening.isSupported(protocol) === preview || stable,
      s"Expected type widening to be ${supported(preview || stable)} but it is " +
        s"${supported(!(preview || stable))}.")
  }

  test("automatically enabling the preview feature doesn't enable the stable feature") {
    setupManualClock()
    // Create a new table with type widening enabled.
    sql(s"CREATE TABLE delta.`$tempPath` (a byte) USING DELTA " +
      s"TBLPROPERTIES ('${DeltaConfigs.ENABLE_TYPE_WIDENING.key}' = 'true')")
    addSingleFile(Seq(1), ByteType)
    sql(s"ALTER TABLE delta.`$tempPath` CHANGE COLUMN a TYPE int")

    // The stable feature isn't supported and can't be dropped.
    assertFeatureSupported(preview = true, stable = false)
    dropTableFeature(
      feature = TypeWideningTableFeature,
      expectedOutcome = ExpectedOutcome.FAIL_FEATURE_NOT_PRESENT,
      expectedNumFilesRewritten = 0,
      expectedColumnTypes = Map("a" -> ByteType)
    )

    // The preview feature is supported and can be dropped.
    dropTableFeature(
      feature = TypeWideningPreviewTableFeature,
      expectedOutcome = ExpectedOutcome.FAIL_CURRENT_VERSION_USES_FEATURE,
      expectedNumFilesRewritten = 1,
      expectedColumnTypes = Map("a" -> IntegerType)
    )
    assertFeatureSupported(preview = true, stable = false)

    advancePastRetentionPeriod()
    dropTableFeature(
      feature = TypeWideningPreviewTableFeature,
      expectedOutcome = ExpectedOutcome.SUCCESS,
      expectedNumFilesRewritten = 0,
      expectedColumnTypes = Map("a" -> IntegerType)
    )
    assertFeatureSupported(preview = false, stable = false)
  }

  test("manually adding the stable and preview features and dropping them") {
    setupManualClock()
    sql(s"CREATE TABLE delta.`$tempPath` (a byte) USING DELTA " +
      s"TBLPROPERTIES ('${DeltaConfigs.ENABLE_TYPE_WIDENING.key}' = 'false')")
    assertFeatureSupported(preview = false, stable = false)
    // This is undocumented for type widening but users can manually add the preview/stable feature
    // to the table instead of using the table property.
    addTableFeature(tempPath, TypeWideningTableFeature)
    assertFeatureSupported(preview = false, stable = true)

    // Users can manually add both features to the table that way: this is allowed, the two
    // specifications are compatible and supported.
    addTableFeature(tempPath, TypeWideningPreviewTableFeature)
    assertFeatureSupported(preview = true, stable = true)

    enableTypeWidening(tempPath)
    addSingleFile(Seq(1), ByteType)
    sql(s"ALTER TABLE delta.`$tempPath` CHANGE COLUMN a TYPE int")
    // Dropping the stable feature doesn't also drop the preview feature.
    dropTableFeature(
      feature = TypeWideningTableFeature,
      expectedOutcome = ExpectedOutcome.FAIL_CURRENT_VERSION_USES_FEATURE,
      expectedNumFilesRewritten = 1,
      expectedColumnTypes = Map("a" -> IntegerType)
    )
    assertFeatureSupported(preview = true, stable = true)

    advancePastRetentionPeriod()
    dropTableFeature(
      feature = TypeWideningTableFeature,
      expectedOutcome = ExpectedOutcome.SUCCESS,
      expectedNumFilesRewritten = 0,
      expectedColumnTypes = Map("a" -> IntegerType)
    )
    assertFeatureSupported(preview = true, stable = false)

    // Dropping the preview feature is now immediate since all traces have already been removed from
    // the table history.
    dropTableFeature(
      feature = TypeWideningPreviewTableFeature,
      expectedOutcome = ExpectedOutcome.SUCCESS,
      expectedNumFilesRewritten = 0,
      expectedColumnTypes = Map("a" -> IntegerType)
    )
    assertFeatureSupported(preview = false, stable = false)
  }

  test("tables created with the stable feature aren't automatically enabling the preview feature") {
    setupManualClock()
    sql(s"CREATE TABLE delta.`$tempPath` (a byte) USING DELTA " +
      s"TBLPROPERTIES ('${DeltaConfigs.ENABLE_TYPE_WIDENING.key}' = 'false')")

    addTableFeature(tempPath, TypeWideningTableFeature)
    assertFeatureSupported(preview = false, stable = true)

    // Enable the table property, this should keep the stable feature but not add the preview one.
    enableTypeWidening(tempPath)
    assertFeatureSupported(preview = false, stable = true)

    addSingleFile(Seq(1), ByteType)
    sql(s"ALTER TABLE delta.`$tempPath` CHANGE COLUMN a TYPE int")

    dropTableFeature(
      feature = TypeWideningPreviewTableFeature,
      expectedOutcome = ExpectedOutcome.FAIL_FEATURE_NOT_PRESENT,
      expectedNumFilesRewritten = 0,
      expectedColumnTypes = Map("a" -> ByteType)
    )
    // The stable table feature can be dropped.
    dropTableFeature(
      feature = TypeWideningTableFeature,
      expectedOutcome = ExpectedOutcome.FAIL_CURRENT_VERSION_USES_FEATURE,
      expectedNumFilesRewritten = 1,
      expectedColumnTypes = Map("a" -> IntegerType)
    )
    assertFeatureSupported(preview = false, stable = true)

    advancePastRetentionPeriod()
    dropTableFeature(
      feature = TypeWideningTableFeature,
      expectedOutcome = ExpectedOutcome.SUCCESS,
      expectedNumFilesRewritten = 0,
      expectedColumnTypes = Map("a" -> IntegerType)
    )
    assertFeatureSupported(preview = false, stable = false)
  }

  test("tableVersion metadata is correctly set and preserved when using the preview feature") {
    sql(s"CREATE TABLE delta.`$tempPath` (a byte) USING DELTA " +
      s"TBLPROPERTIES ('${DeltaConfigs.ENABLE_TYPE_WIDENING.key}' = 'false')")

    addTableFeature(tempPath, TypeWideningPreviewTableFeature)
    enableTypeWidening(tempPath)
    addSingleFile(Seq(1), ByteType)
    sql(s"ALTER TABLE delta.`$tempPath` CHANGE COLUMN a TYPE short")

    assert(readDeltaTable(tempPath).schema === new StructType()
      .add("a", ShortType, nullable = true, metadata = new MetadataBuilder()
        .putMetadataArray("delta.typeChanges", Array(
          new MetadataBuilder()
            .putLong("tableVersion", 4)
            .putString("fromType", "byte")
            .putString("toType", "short")
            .build()
        )).build()))

    // It's allowed to manually add both the preview and stable feature to the same table - the
    // specs are compatible. In that case, we still populate the `tableVersion` field.
    addTableFeature(tempPath, TypeWideningTableFeature)
    sql(s"ALTER TABLE delta.`$tempPath` CHANGE COLUMN a TYPE int")
    assert(readDeltaTable(tempPath).schema === new StructType()
      .add("a", IntegerType, nullable = true, metadata = new MetadataBuilder()
        .putMetadataArray("delta.typeChanges", Array(
          new MetadataBuilder()
            .putLong("tableVersion", 4)
            .putString("fromType", "byte")
            .putString("toType", "short")
            .build(),
          new MetadataBuilder()
            .putLong("tableVersion", 6)
            .putString("fromType", "short")
            .putString("toType", "integer")
            .build()
        )).build()))
  }

  test("tableVersion isn't set when using the stable feature") {
    sql(s"CREATE TABLE delta.`$tempPath` (a byte) USING DELTA " +
      s"TBLPROPERTIES ('${DeltaConfigs.ENABLE_TYPE_WIDENING.key}' = 'false')")

    addTableFeature(tempPath, TypeWideningTableFeature)
    enableTypeWidening(tempPath)
    addSingleFile(Seq(1), ByteType)
    sql(s"ALTER TABLE delta.`$tempPath` CHANGE COLUMN a TYPE short")
    assert(readDeltaTable(tempPath).schema === new StructType()
      .add("a", ShortType, nullable = true, metadata = new MetadataBuilder()
        .putMetadataArray("delta.typeChanges", Array(
          new MetadataBuilder()
            .putString("fromType", "byte")
            .putString("toType", "short")
            .build()
        )).build()))
  }
}
