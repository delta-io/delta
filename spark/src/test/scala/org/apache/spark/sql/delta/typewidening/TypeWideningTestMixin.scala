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

import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.actions.{RemoveFile, TableFeatureProtocolUtils}
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.delta.commands.AlterTableDropFeatureDeltaCommand
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.util.DeltaFileOperations
import com.google.common.math.DoubleMath

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Encoder, QueryTest}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.{LegacyBehaviorPolicy, SQLConf}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

/**
 * Test mixin that enables type widening by default for all tests in the suite.
 */
trait TypeWideningTestMixin extends DeltaSQLCommandTest with DeltaDMLTestUtils { self: QueryTest =>

  import testImplicits._

  protected override def sparkConf: SparkConf = {
    super.sparkConf
      .set(DeltaConfigs.ENABLE_TYPE_WIDENING.defaultTablePropertyKey, "true")
      .set(TableFeatureProtocolUtils.defaultPropertyKey(TimestampNTZTableFeature), "supported")
      // Ensure we don't silently cast test inputs to null on overflow.
      .set(SQLConf.ANSI_ENABLED.key, "true")
      // Rebase mode must be set explicitly to allow writing dates before 1582-10-15.
      .set(SQLConf.PARQUET_REBASE_MODE_IN_WRITE.key, LegacyBehaviorPolicy.CORRECTED.toString)
  }

  /** Enable (or disable) type widening for the table under the given path. */
  protected def enableTypeWidening(tablePath: String, enabled: Boolean = true): Unit =
    sql(s"ALTER TABLE delta.`$tablePath` " +
          s"SET TBLPROPERTIES('${DeltaConfigs.ENABLE_TYPE_WIDENING.key}' = '${enabled.toString}')")

  /** Whether the test table supports the type widening table feature. */
  def isTypeWideningSupported: Boolean = {
    TypeWidening.isSupported(deltaLog.update().protocol)
  }

  /** Whether the type widening table property is enabled on the test table. */
  def isTypeWideningEnabled: Boolean = {
    val snapshot = deltaLog.update()
    TypeWidening.isEnabled(snapshot.protocol, snapshot.metadata)
  }

  /** Short-hand to create type widening metadata for struct fields. */
  protected def typeWideningMetadata(
      version: Long,
      from: AtomicType,
      to: AtomicType,
      path: Seq[String] = Seq.empty): Metadata =
    new MetadataBuilder()
      .putMetadataArray(
        "delta.typeChanges", Array(TypeChange(Some(version), from, to, path).toMetadata))
      .build()

  def addSingleFile[T: Encoder](values: Seq[T], dataType: DataType): Unit =
      append(values.toDF("a").select(col("a").cast(dataType)).repartition(1))

  /**
   * Similar to `QueryTest.checkAnswer` but using fuzzy equality for double values. This is needed
   * because double partition values are serialized as string leading to loss of precision. Also
   * `checkAnswer` treats -0f and 0f as different values without tolerance.
   */
  def checkAnswerWithTolerance(
      actualDf: DataFrame,
      expectedDf: DataFrame,
      toType: DataType,
      tolerance: Double = 0.001)
    : Unit = {
    // Widening to float isn't supported so only handle double here.
    if (toType == DoubleType) {
      val actual = actualDf.sort("value").collect()
      val expected = expectedDf.sort("value").collect()
      assert(actual.length === expected.length, s"Wrong result: $actual did not equal $expected")

      actual.zip(expected).foreach { case (a, e) =>
        val expectedValue = e.getAs[Double]("value")
        val actualValue = a.getAs[Double]("value")
        val absTolerance = if (expectedValue.isNaN || expectedValue.isInfinity) {
          0
        } else {
          tolerance * Math.abs(expectedValue)
        }
        assert(
          DoubleMath.fuzzyEquals(actualValue, expectedValue, absTolerance),
          s"$actualValue did not equal $expectedValue"
        )
      }
    } else {
      checkAnswer(actualDf, expectedDf)
    }
  }
}

/**
 * Mixin trait containing helpers to test dropping the type widening table feature.
 */
trait TypeWideningDropFeatureTestMixin
    extends QueryTest
    with SharedSparkSession
    with DeltaDMLTestUtils {

  /** Expected outcome of dropping the type widening table feature. */
  object ExpectedOutcome extends Enumeration {
    val SUCCESS,
    FAIL_CURRENT_VERSION_USES_FEATURE,
    FAIL_HISTORICAL_VERSION_USES_FEATURE,
    FAIL_FEATURE_NOT_PRESENT = Value
  }

  /**
   * Helper method to drop the type widening table feature and check for an expected outcome.
   * Validates in particular that the right number of files were rewritten and that the rewritten
   * files all contain the expected type for specified columns.
   */
  def dropTableFeature(
      feature: TableFeature = TypeWideningPreviewTableFeature,
      expectedOutcome: ExpectedOutcome.Value,
      expectedNumFilesRewritten: Long,
      expectedColumnTypes: Map[String, DataType]): Unit = {
    val snapshot = deltaLog.update()
    // Need to directly call ALTER TABLE command to pass our deltaLog with manual clock.
    val dropFeature =
      AlterTableDropFeatureDeltaCommand(DeltaTableV2(spark, deltaLog.dataPath), feature.name)

    expectedOutcome match {
      case ExpectedOutcome.SUCCESS =>
        dropFeature.run(spark)
      case ExpectedOutcome.FAIL_CURRENT_VERSION_USES_FEATURE =>
        checkError(
          exception = intercept[DeltaTableFeatureException] { dropFeature.run(spark) },
          errorClass = "DELTA_FEATURE_DROP_WAIT_FOR_RETENTION_PERIOD",
          parameters = Map(
            "feature" -> feature.name,
            "logRetentionPeriodKey" -> DeltaConfigs.LOG_RETENTION.key,
            "logRetentionPeriod" -> DeltaConfigs.LOG_RETENTION
              .fromMetaData(snapshot.metadata).toString,
            "truncateHistoryLogRetentionPeriod" ->
              DeltaConfigs.TABLE_FEATURE_DROP_TRUNCATE_HISTORY_LOG_RETENTION
                .fromMetaData(snapshot.metadata).toString)
        )
      case ExpectedOutcome.FAIL_HISTORICAL_VERSION_USES_FEATURE =>
        checkError(
          exception = intercept[DeltaTableFeatureException] { dropFeature.run(spark) },
          errorClass = "DELTA_FEATURE_DROP_HISTORICAL_VERSIONS_EXIST",
          parameters = Map(
            "feature" -> feature.name,
            "logRetentionPeriodKey" -> DeltaConfigs.LOG_RETENTION.key,
            "logRetentionPeriod" -> DeltaConfigs.LOG_RETENTION
              .fromMetaData(snapshot.metadata).toString,
            "truncateHistoryLogRetentionPeriod" ->
              DeltaConfigs.TABLE_FEATURE_DROP_TRUNCATE_HISTORY_LOG_RETENTION
                .fromMetaData(snapshot.metadata).toString)
        )
      case ExpectedOutcome.FAIL_FEATURE_NOT_PRESENT =>
        checkError(
          exception = intercept[DeltaTableFeatureException] { dropFeature.run(spark) },
          errorClass = "DELTA_FEATURE_DROP_FEATURE_NOT_PRESENT",
          parameters = Map("feature" -> feature.name)
        )
    }

    if (expectedOutcome != ExpectedOutcome.FAIL_FEATURE_NOT_PRESENT) {
      assert(!TypeWideningMetadata.containsTypeWideningMetadata(deltaLog.update().schema))
    }

    // Check the number of files rewritten.
    assert(getNumRemoveFilesSinceVersion(snapshot.version + 1) === expectedNumFilesRewritten,
      s"Expected $expectedNumFilesRewritten file(s) to be rewritten but found " +
        s"${getNumRemoveFilesSinceVersion(snapshot.version + 1)} rewritten file(s).")

    // Check that all files now contain the expected data types.
    expectedColumnTypes.foreach { case (colName, expectedType) =>
      withSQLConf("spark.databricks.delta.formatCheck.enabled" -> "false") {
        deltaLog.update().filesForScan(Seq.empty, keepNumRecords = false).files.foreach { file =>
          val filePath = DeltaFileOperations.absolutePath(deltaLog.dataPath.toString, file.path)
          val data = spark.read.parquet(filePath.toString)
          val physicalColName = DeltaColumnMapping.getPhysicalName(snapshot.schema(colName))
          assert(data.schema(physicalColName).dataType === expectedType,
            s"File with values ${data.collect().mkString(", ")} wasn't rewritten.")
        }
      }
    }
  }

  /** Get the number of remove actions committed since the given table version (included). */
  def getNumRemoveFilesSinceVersion(version: Long): Long =
    deltaLog
      .getChanges(startVersion = version)
      .flatMap { case (_, actions) => actions }
      .collect { case r: RemoveFile => r }
      .size
}
