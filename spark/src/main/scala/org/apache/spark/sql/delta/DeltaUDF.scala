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

import org.apache.spark.sql.delta.sources.DeltaSQLConf

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.{SparkUserDefinedFunction, UserDefinedFunction}
import org.apache.spark.sql.functions.udf

/**
 * Define a few templates for udfs used by Delta. Use these templates to create
 * `SparkUserDefinedFunction` to avoid creating new Encoders. This would save us from touching
 * `ScalaReflection` to reduce the lock contention in concurrent queries.
 */
object DeltaUDF {

  def stringFromString(f: String => String): UserDefinedFunction =
    createUdfFromTemplateUnsafe(stringFromStringTemplate, f, udf(f))

  def intFromString(f: String => Int): UserDefinedFunction =
    createUdfFromTemplateUnsafe(intFromStringTemplate, f, udf(f))

  def intFromStringBoolean(f: (String, Boolean) => Int): UserDefinedFunction =
    createUdfFromTemplateUnsafe(intFromStringBooleanTemplate, f, udf(f))

  def boolean(f: () => Boolean): UserDefinedFunction =
    createUdfFromTemplateUnsafe(booleanTemplate, f, udf(f))

  def stringFromMap(f: Map[String, String] => String): UserDefinedFunction =
    createUdfFromTemplateUnsafe(stringFromMapTemplate, f, udf(f))

  def booleanFromMap(f: Map[String, String] => Boolean): UserDefinedFunction =
    createUdfFromTemplateUnsafe(booleanFromMapTemplate, f, udf(f))

  def booleanFromByte(x: Byte => Boolean): UserDefinedFunction =
    createUdfFromTemplateUnsafe(booleanFromByteTemplate, x, udf(x))

  private lazy val stringFromStringTemplate =
    udf[String, String](identity).asInstanceOf[SparkUserDefinedFunction]

  private lazy val booleanTemplate = udf(() => true).asInstanceOf[SparkUserDefinedFunction]

  private lazy val intFromStringTemplate =
    udf((_: String) => 1).asInstanceOf[SparkUserDefinedFunction]

  private lazy val intFromStringBooleanTemplate =
    udf((_: String, _: Boolean) => 1).asInstanceOf[SparkUserDefinedFunction]

  private lazy val stringFromMapTemplate =
    udf((_: Map[String, String]) => "").asInstanceOf[SparkUserDefinedFunction]

  private lazy val booleanFromMapTemplate =
    udf((_: Map[String, String]) => true).asInstanceOf[SparkUserDefinedFunction]

  private lazy val booleanFromByteTemplate =
    udf((_: Byte) => true).asInstanceOf[SparkUserDefinedFunction]

  /**
   * Return a `UserDefinedFunction` for the given `f` from `template` if
   * `INTERNAL_UDF_OPTIMIZATION_ENABLED` is enabled. Otherwise, `orElse` will be called to create a
   * new `UserDefinedFunction`.
   */
  private def createUdfFromTemplateUnsafe(
      template: SparkUserDefinedFunction,
      f: AnyRef,
      orElse: => UserDefinedFunction): UserDefinedFunction = {
    if (SparkSession.active.sessionState.conf
      .getConf(DeltaSQLConf.INTERNAL_UDF_OPTIMIZATION_ENABLED)) {
      val inputEncoders = template.inputEncoders.map(_.map(_.copy()))
      val outputEncoder = template.outputEncoder.map(_.copy())
      template.copy(f = f, inputEncoders = inputEncoders, outputEncoder = outputEncoder)
    } else {
      orElse
    }
  }
}
