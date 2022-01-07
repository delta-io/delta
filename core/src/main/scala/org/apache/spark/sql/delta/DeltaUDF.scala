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

object DeltaUDF {

  /**
   * A template for String => String udfs. It's used to create `SparkUserDefinedFunction` for
   * String => String udfs without touching Scala Reflection to reduce the log contention.
   */
  private lazy val stringStringUdfTemplate =
    udf[String, String]((x: String) => x).asInstanceOf[SparkUserDefinedFunction]

  private def createUdfFromTemplate[R, T](
      template: SparkUserDefinedFunction,
      f: T => R): UserDefinedFunction = {
    template.copy(
      f = f,
      inputEncoders = template.inputEncoders.map(_.map(_.copy())),
      outputEncoder = template.outputEncoder.map(_.copy())
    )
  }

  def stringStringUdf(f: String => String): UserDefinedFunction = {
    if (SparkSession.active.sessionState.conf
      .getConf(DeltaSQLConf.INTERNAL_UDF_OPTIMIZATION_ENABLED)) {
      createUdfFromTemplate(stringStringUdfTemplate, f)
    } else {
      udf[String, String](f)
    }
  }
}
