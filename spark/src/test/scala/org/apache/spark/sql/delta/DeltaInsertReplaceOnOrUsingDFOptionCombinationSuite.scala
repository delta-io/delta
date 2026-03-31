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

import org.apache.spark.sql.delta.DeltaOptions.{
  DATA_CHANGE_OPTION,
  OVERWRITE_SCHEMA_OPTION,
  PARTITION_OVERWRITE_MODE_OPTION,
  REPLACE_ON_OPTION,
  REPLACE_USING_OPTION,
  REPLACE_WHERE_OPTION
}
import org.apache.spark.sql.delta.sources.DeltaSQLConf

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, DataFrameWriter, QueryTest, Row, SaveMode}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Tests incompatible DataFrame option combinations with `replaceOn` or `replaceUsing`.
 */
class DeltaInsertReplaceOnOrUsingDFOptionCombinationSuite extends QueryTest
  with SharedSparkSession {

  override protected def sparkConf: SparkConf = super.sparkConf
    .set(DeltaSQLConf.REPLACE_USING_OPTION_IN_DATAFRAME_WRITER_ENABLED.key, "true")
    .set(DeltaSQLConf.REPLACE_ON_OPTION_IN_DATAFRAME_WRITER_ENABLED.key, "true")

  import testImplicits._

  private val dynamicPartitionOverwriteErrorClass =
    "DELTA_DYNAMIC_PARTITION_OVERWRITE_INCOMPATIBLE_REPLACE_ON_OR_USING"

  private val overwriteByFilterErrorClass =
    "DELTA_OVERWRITE_BY_FILTER_INCOMPATIBLE_REPLACE_ON_OR_USING"

  private def checkDynamicPartitionOverwriteIncompatibleReplaceOnOrUsingError(
      dataFrameWriteScenario: => Unit): Unit = {
    checkError(
      exception = intercept[DeltaIllegalArgumentException](dataFrameWriteScenario),
      condition = dynamicPartitionOverwriteErrorClass,
      sqlState = Some("42613")
    )
  }

  private def checkOverwriteByFilterIncompatibleReplaceOnOrUsingError(
      dataFrameWriteScenario: => Unit): Unit = {
    checkError(
      exception = intercept[DeltaIllegalArgumentException](dataFrameWriteScenario),
      condition = overwriteByFilterErrorClass,
      sqlState = Some("42613")
    )
  }

  private def checkIncompatibleWithReplaceOnOrUsingError(
      incompatibleOption: String,
      replaceOnOrUsingOption: String)(dataFrameWriteScenario: => Unit): Unit = {
    checkError(
      exception = intercept[DeltaIllegalArgumentException](dataFrameWriteScenario),
      condition = "DELTA_INCOMPATIBLE_DATAFRAME_OPTIONS",
      sqlState = Some("42613"),
      parameters = Map(
        "firstDeltaOption" -> incompatibleOption,
        "secondDeltaOption" -> replaceOnOrUsingOption)
    )
  }

  private def testDynamicPartitionOverwriteWithDFv1APIs(
      testNamePrefix: String)(
      targetDF: => DataFrame,
      sourceDF: => DataFrame,
      replaceOnOrUsingOption: String,
      replaceOnOrUsingValue: String,
      addIncompatibleOption: DataFrameWriter[Row] => DataFrameWriter[Row],
      partitionBy: Seq[String]): Unit = {

    def setupTarget(df: DataFrame): DataFrameWriter[Row] =
      df.write.format("delta").partitionBy(partitionBy: _*)

    def writeSource: DataFrameWriter[Row] = addIncompatibleOption(
      sourceDF.write.mode(SaveMode.Overwrite).format("delta")
        .option(replaceOnOrUsingOption, replaceOnOrUsingValue)
    )

    test(s"$testNamePrefix, save DataFrameWriterV1 API") {
      withTempDir { tempDir =>
        setupTarget(targetDF).save(tempDir.getAbsolutePath)
        checkDynamicPartitionOverwriteIncompatibleReplaceOnOrUsingError {
          writeSource.save(tempDir.getAbsolutePath)
        }
      }
    }

    test(s"$testNamePrefix, insertInto DataFrameWriterV1 API") {
      withTable("target") {
        setupTarget(targetDF).saveAsTable("target")
        checkDynamicPartitionOverwriteIncompatibleReplaceOnOrUsingError {
          writeSource.insertInto("target")
        }
      }
    }

    test(s"$testNamePrefix, insertInto path-based DataFrameWriterV1 API") {
      withTempDir { tempDir =>
        setupTarget(targetDF).save(tempDir.getAbsolutePath)
        checkDynamicPartitionOverwriteIncompatibleReplaceOnOrUsingError {
          writeSource.insertInto(s"delta.`${tempDir.getAbsolutePath}`")
        }
      }
    }

    test(s"$testNamePrefix, saveAsTable DataFrameWriterV1 API") {
      withTable("target") {
        setupTarget(targetDF).saveAsTable("target")
        checkDynamicPartitionOverwriteIncompatibleReplaceOnOrUsingError {
          writeSource.saveAsTable("target")
        }
      }
    }

    test(s"$testNamePrefix, saveAsTable path-based DataFrameWriterV1 API") {
      withTempDir { tempDir =>
        setupTarget(targetDF).save(tempDir.getAbsolutePath)
        checkDynamicPartitionOverwriteIncompatibleReplaceOnOrUsingError {
          writeSource.saveAsTable(s"delta.`${tempDir.getAbsolutePath}`")
        }
      }
    }
  }

  private def testOverwriteByFilterWithDFv1APIs(
      testNamePrefix: String)(
      targetDF: => DataFrame,
      sourceDF: => DataFrame,
      replaceOnOrUsingOption: String,
      replaceOnOrUsingValue: String,
      addIncompatibleOption: DataFrameWriter[Row] => DataFrameWriter[Row]): Unit = {

    def setupTarget(df: DataFrame): DataFrameWriter[Row] = df.write.format("delta")

    def writeSource: DataFrameWriter[Row] = addIncompatibleOption(
      sourceDF.write.mode(SaveMode.Overwrite).format("delta")
        .option(replaceOnOrUsingOption, replaceOnOrUsingValue)
    )

    test(s"$testNamePrefix, save DataFrameWriterV1 API") {
      withTempDir { tempDir =>
        setupTarget(targetDF).save(tempDir.getAbsolutePath)
        checkOverwriteByFilterIncompatibleReplaceOnOrUsingError {
          writeSource.save(tempDir.getAbsolutePath)
        }
      }
    }

    test(s"$testNamePrefix, insertInto DataFrameWriterV1 API") {
      withTable("target") {
        setupTarget(targetDF).saveAsTable("target")
        checkOverwriteByFilterIncompatibleReplaceOnOrUsingError {
          writeSource.insertInto("target")
        }
      }
    }

    test(s"$testNamePrefix, insertInto path-based DataFrameWriterV1 API") {
      withTempDir { tempDir =>
        setupTarget(targetDF).save(tempDir.getAbsolutePath)
        checkOverwriteByFilterIncompatibleReplaceOnOrUsingError {
          writeSource.insertInto(s"delta.`${tempDir.getAbsolutePath}`")
        }
      }
    }

    test(s"$testNamePrefix, saveAsTable DataFrameWriterV1 API") {
      withTable("target") {
        setupTarget(targetDF).saveAsTable("target")
        checkOverwriteByFilterIncompatibleReplaceOnOrUsingError {
          writeSource.saveAsTable("target")
        }
      }
    }

    test(s"$testNamePrefix, saveAsTable path-based DataFrameWriterV1 API") {
      withTempDir { tempDir =>
        setupTarget(targetDF).save(tempDir.getAbsolutePath)
        checkOverwriteByFilterIncompatibleReplaceOnOrUsingError {
          writeSource.saveAsTable(s"delta.`${tempDir.getAbsolutePath}`")
        }
      }
    }
  }

  // `replaceOn` and `replaceUsing` are mutually exclusive.
  test("`replaceOn` should be invalid with `replaceUsing` option, save DataFrameWriterV1 API") {
    withTempDir { tempDir =>
      Seq((1, "a"), (2, "b")).toDF("id", "value")
        .write.format("delta").save(tempDir.getAbsolutePath)
      checkIncompatibleWithReplaceOnOrUsingError(
          REPLACE_ON_OPTION, REPLACE_USING_OPTION) {
        Seq((3, "c"), (4, "d")).toDF("id", "value")
          .write.mode(SaveMode.Overwrite).format("delta")
          .option(REPLACE_USING_OPTION, "id")
          .option(REPLACE_ON_OPTION, "id = 5")
          .save(tempDir.getAbsolutePath)
      }
    }
  }

  test("`replaceOn` should be invalid with `replaceUsing` option, saveAsTable DataFrameWriterV1" +
      " API") {
    withTable("target") {
      Seq((1, "a"), (2, "b")).toDF("id", "value")
        .write.format("delta").saveAsTable("target")
      checkIncompatibleWithReplaceOnOrUsingError(
          REPLACE_ON_OPTION, REPLACE_USING_OPTION) {
        Seq((3, "c"), (4, "d")).toDF("id", "value")
          .write.mode(SaveMode.Overwrite).format("delta")
          .option(REPLACE_USING_OPTION, "id")
          .option(REPLACE_ON_OPTION, "id = 5")
          .saveAsTable("target")
      }
    }
  }

  Seq(
    (REPLACE_ON_OPTION, "true"),
    (REPLACE_USING_OPTION, "id")
  ).foreach { case (optionName, optionValue) =>

    testOverwriteByFilterWithDFv1APIs(
      s"`replaceWhere` should be invalid with `$optionName` option")(
      targetDF = Seq((1, "a"), (2, "b")).toDF("id", "value"),
      sourceDF = Seq((3, "c"), (4, "d")).toDF("id", "value"),
      replaceOnOrUsingOption = optionName,
      replaceOnOrUsingValue = optionValue,
      addIncompatibleOption = _.option(REPLACE_WHERE_OPTION, "id > 1")
    )

    testDynamicPartitionOverwriteWithDFv1APIs(
      s"`partitionOverwriteMode` should be invalid with `$optionName` option")(
      targetDF = Seq((1, "a", "x"), (2, "b", "y")).toDF("id", "value", "part"),
      sourceDF = Seq((3, "c", "x")).toDF("id", "value", "part"),
      replaceOnOrUsingOption = optionName,
      replaceOnOrUsingValue = optionValue,
      addIncompatibleOption = _.option(PARTITION_OVERWRITE_MODE_OPTION, "dynamic"),
      partitionBy = Seq("part")
    )

    test(s"DataFrameWriterV2 overwrite(predicate) should be invalid with `$optionName` option") {
      withTable("target") {
        Seq((1, "a"), (2, "b")).toDF("id", "value")
          .write.format("delta").saveAsTable("target")

        checkOverwriteByFilterIncompatibleReplaceOnOrUsingError {
          Seq((3, "c"), (4, "d")).toDF("id", "value")
            .writeTo("target")
            .option(optionName, optionValue)
            .overwrite(col("id") === 99)
        }
      }
    }

    test(s"DataFrameWriterV2 overwritePartitions should be invalid with `$optionName` option") {
      withTable("target") {
        Seq((1, "a", "x"), (2, "b", "y")).toDF("id", "value", "part")
          .write.format("delta").partitionBy("part").saveAsTable("target")

        checkDynamicPartitionOverwriteIncompatibleReplaceOnOrUsingError {
          Seq((3, "c", "x")).toDF("id", "value", "part")
            .writeTo("target")
            .option(optionName, optionValue)
            .overwritePartitions()
        }
      }
    }

    def writeSource(
        sourceDF: DataFrame,
        incompatibleOptionFn: DataFrameWriter[Row] => DataFrameWriter[Row])
      : DataFrameWriter[Row] = {
      incompatibleOptionFn(
        sourceDF.write.mode(SaveMode.Overwrite).format("delta")
          .option(optionName, optionValue)
      )
    }

    test(s"`overwriteSchema`=true should be invalid with `$optionName`" +
        ", save DataFrameWriterV1 API") {
      withTempDir { tempDir =>
        Seq((1, "a"), (2, "b")).toDF("id", "value")
          .write.format("delta").save(tempDir.getAbsolutePath)
        checkIncompatibleWithReplaceOnOrUsingError(
            incompatibleOption = OVERWRITE_SCHEMA_OPTION,
            replaceOnOrUsingOption = optionName) {
          writeSource(
            sourceDF = Seq((3, "c"), (4, "d")).toDF("id", "value"),
            incompatibleOptionFn = _.option(OVERWRITE_SCHEMA_OPTION, "true")
          ).save(tempDir.getAbsolutePath)
        }
      }
    }

    test(s"`overwriteSchema`=true should be invalid with `$optionName`" +
        ", saveAsTable DataFrameWriterV1 API") {
      withTable("target") {
        Seq((1, "a"), (2, "b")).toDF("id", "value")
          .write.format("delta").saveAsTable("target")
        checkIncompatibleWithReplaceOnOrUsingError(
            incompatibleOption = OVERWRITE_SCHEMA_OPTION,
            replaceOnOrUsingOption = optionName) {
          writeSource(
            sourceDF = Seq((3, "c"), (4, "d")).toDF("id", "value"),
            incompatibleOptionFn = _.option(OVERWRITE_SCHEMA_OPTION, "true")
          ).saveAsTable("target")
        }
      }
    }

    test(s"`overwriteSchema`=true should be invalid with `$optionName`" +
        ", insertInto DataFrameWriterV1 API") {
      withTable("target") {
        Seq((1, "a"), (2, "b")).toDF("id", "value")
          .write.format("delta").saveAsTable("target")
        checkIncompatibleWithReplaceOnOrUsingError(
            incompatibleOption = OVERWRITE_SCHEMA_OPTION,
            replaceOnOrUsingOption = optionName) {
          writeSource(
            sourceDF = Seq((3, "c"), (4, "d")).toDF("id", "value"),
            incompatibleOptionFn = _.option(OVERWRITE_SCHEMA_OPTION, "true")
          ).insertInto("target")
        }
      }
    }

    test(s"`dataChange`=false should be invalid with `$optionName`" +
        ", save DataFrameWriterV1 API") {
      withTempDir { tempDir =>
        Seq((1, "a"), (2, "b")).toDF("id", "value")
          .write.format("delta").save(tempDir.getAbsolutePath)
        checkIncompatibleWithReplaceOnOrUsingError(
            incompatibleOption = DATA_CHANGE_OPTION,
            replaceOnOrUsingOption = optionName) {
          writeSource(
            sourceDF = Seq((3, "c"), (4, "d")).toDF("id", "value"),
            incompatibleOptionFn = _.option(DATA_CHANGE_OPTION, "false")
          ).save(tempDir.getAbsolutePath)
        }
      }
    }

    test(s"`dataChange`=false should be invalid with `$optionName`" +
        ", saveAsTable DataFrameWriterV1 API") {
      withTable("target") {
        Seq((1, "a"), (2, "b")).toDF("id", "value")
          .write.format("delta").saveAsTable("target")
        checkIncompatibleWithReplaceOnOrUsingError(
            incompatibleOption = DATA_CHANGE_OPTION,
            replaceOnOrUsingOption = optionName) {
          writeSource(
            sourceDF = Seq((3, "c"), (4, "d")).toDF("id", "value"),
            incompatibleOptionFn = _.option(DATA_CHANGE_OPTION, "false")
          ).saveAsTable("target")
        }
      }
    }

    test(s"`dataChange`=false should be invalid with `$optionName`" +
        ", insertInto DataFrameWriterV1 API") {
      withTable("target") {
        Seq((1, "a"), (2, "b")).toDF("id", "value")
          .write.format("delta").saveAsTable("target")
        checkIncompatibleWithReplaceOnOrUsingError(
            incompatibleOption = DATA_CHANGE_OPTION,
            replaceOnOrUsingOption = optionName) {
          writeSource(
            sourceDF = Seq((3, "c"), (4, "d")).toDF("id", "value"),
            incompatibleOptionFn = _.option(DATA_CHANGE_OPTION, "false")
          ).insertInto("target")
        }
      }
    }

    // In Append mode, overwriteSchema=true has no effect (isOverwriteOperation is false),
    // so it does not conflict with replaceOn/replaceUsing. The write just appends.
    test(s"`overwriteSchema`=true in Append mode should just append with `$optionName`" +
        ", save DataFrameWriterV1 API") {
      withTempDir { tempDir =>
        Seq((1, "a"), (2, "b")).toDF("id", "value")
          .write.format("delta").save(tempDir.getAbsolutePath)
        Seq((2, "c"), (3, "d")).toDF("id", "value")
          .write.mode(SaveMode.Append).format("delta")
          .option(optionName, optionValue)
          .option(OVERWRITE_SCHEMA_OPTION, "true")
          .save(tempDir.getAbsolutePath)
        checkAnswer(
          spark.read.format("delta").load(tempDir.getAbsolutePath).orderBy("id", "value"),
          Seq(Row(1, "a"), Row(2, "b"), Row(2, "c"), Row(3, "d"))
        )
      }
    }
  }

  Seq(
    ("REPLACEON", "true", REPLACE_ON_OPTION),
    ("REPLACEUSING", "id", REPLACE_USING_OPTION)
  ).foreach { case (optionUpperCase, optionValue, canonicalOptionName) =>
    test(s"`overwriteSchema`=true should be invalid with `$canonicalOptionName`" +
        " (case insensitive)") {
      withTempDir { tempDir =>
        Seq((1, "a"), (2, "b")).toDF("id", "value")
          .write.format("delta").save(tempDir.getAbsolutePath)

        checkIncompatibleWithReplaceOnOrUsingError(
            incompatibleOption = OVERWRITE_SCHEMA_OPTION,
            replaceOnOrUsingOption = canonicalOptionName) {
          Seq((3, "c"), (4, "d")).toDF("id", "value")
            .write.mode(SaveMode.Overwrite).format("delta")
            .option(optionUpperCase, optionValue)
            .option("OVERWRITESCHEMA", "true")
            .save(tempDir.getAbsolutePath)
        }
      }
    }
  }
}
