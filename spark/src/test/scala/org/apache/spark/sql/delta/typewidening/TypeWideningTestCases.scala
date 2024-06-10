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

import org.apache.spark.sql.{DataFrame, Encoder, Row}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{DataType, StructField, StructType}

/**
 * Trait collecting supported and unsupported type change test cases.
 */
trait TypeWideningTestCases extends TypeWideningTestCasesShims { self: SharedSparkSession =>
  import testImplicits._

  /**
   * Represents the input of a type change test.
   * @param fromType         The original type of the column 'value' in the test table.
   * @param toType           The type to use when changing the type of column 'value'.
   */
  abstract class TypeEvolutionTestCase(
      val fromType: DataType,
      val toType: DataType) {
    /** The initial values to insert with type `fromType` in column 'value' after table creation. */
    def initialValuesDF: DataFrame
    /** Additional values to insert after changing the type of the column 'value' to `toType`. */
    def additionalValuesDF: DataFrame
    /** Expected content of the table after inserting the additional values. */
    def expectedResult: DataFrame
  }

  /**
   * Represents the input of a supported type change test. Handles converting the test values from
   * scala types to a dataframe.
   */
  case class SupportedTypeEvolutionTestCase[
      FromType  <: DataType, ToType <: DataType,
      FromVal: Encoder, ToVal: Encoder
    ](
      override val fromType: FromType,
      override val toType: ToType,
      initialValues: Seq[FromVal],
      additionalValues: Seq[ToVal]
  ) extends TypeEvolutionTestCase(fromType, toType) {
    override def initialValuesDF: DataFrame =
      initialValues.toDF("value").select($"value".cast(fromType))

    override def additionalValuesDF: DataFrame =
      additionalValues.toDF("value").select($"value".cast(toType))

    override def expectedResult: DataFrame =
      initialValuesDF.union(additionalValuesDF).select($"value".cast(toType))
  }

  /**
   * Represents the input of an unsupported type change test. Handles converting the test values
   * from scala types to a dataframe. Additional values to insert are always empty since the type
   * change is expected to fail.
   */
  case class UnsupportedTypeEvolutionTestCase[
    FromType  <: DataType, ToType <: DataType, FromVal : Encoder](
      override val fromType: FromType,
      override val toType: ToType,
      initialValues: Seq[FromVal]) extends TypeEvolutionTestCase(fromType, toType) {
    override def initialValuesDF: DataFrame =
      initialValues.toDF("value").select($"value".cast(fromType))

    override def additionalValuesDF: DataFrame =
      spark.createDataFrame(
        sparkContext.emptyRDD[Row],
        new StructType().add(StructField("value", toType)))

    override def expectedResult: DataFrame =
      initialValuesDF.select($"value".cast(toType))
  }
}
