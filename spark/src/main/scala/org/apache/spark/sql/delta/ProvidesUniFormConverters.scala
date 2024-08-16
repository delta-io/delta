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

import java.lang.reflect.InvocationTargetException

import org.apache.commons.lang3.exception.ExceptionUtils

import org.apache.spark.sql.SparkSession
import org.apache.spark.util.Utils

trait ProvidesUniFormConverters { self: DeltaLog =>
  /**
   * Helper trait to instantiate the icebergConverter member variable of the [[DeltaLog]]. We do
   * this through reflection so that delta-spark doesn't have a compile-time dependency on the
   * shaded iceberg module.
   */
  protected lazy val _icebergConverter: UniversalFormatConverter = try {
    val clazz =
      Utils.classForName("org.apache.spark.sql.delta.icebergShaded.IcebergConverter")
    val constructor = clazz.getConstructor(classOf[SparkSession])
    constructor.newInstance(spark)
  } catch {
    case e: ClassNotFoundException =>
      logError("Failed to find Iceberg converter class", e)
      throw DeltaErrors.icebergClassMissing(spark.sparkContext.getConf, e)
    case e: InvocationTargetException =>
      logError("Got error when creating an Iceberg converter", e)
      // The better error is within the cause
      throw ExceptionUtils.getRootCause(e)
  }

  protected lazy val _hudiConverter: UniversalFormatConverter = try {
    val clazz =
      Utils.classForName("org.apache.spark.sql.delta.hudi.HudiConverter")
    val constructor = clazz.getConstructor(classOf[SparkSession])
    constructor.newInstance(spark)
  } catch {
    case e: ClassNotFoundException =>
      logError("Failed to find Hudi converter class", e)
      throw DeltaErrors.hudiClassMissing(spark.sparkContext.getConf, e)
    case e: InvocationTargetException =>
      logError("Got error when creating an Hudi converter", e)
      // The better error is within the cause
      throw ExceptionUtils.getRootCause(e)
  }


  /** Visible for tests (to be able to mock). */
  private[delta] var testIcebergConverter: Option[UniversalFormatConverter] = None
  private[delta] var testHudiConverter: Option[UniversalFormatConverter] = None

  def icebergConverter: UniversalFormatConverter = testIcebergConverter.getOrElse(_icebergConverter)
  def hudiConverter: UniversalFormatConverter = testHudiConverter.getOrElse(_hudiConverter)
}

