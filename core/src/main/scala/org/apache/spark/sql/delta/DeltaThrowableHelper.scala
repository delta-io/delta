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

// scalastyle:off import.ordering.noEmptyLine
import java.net.URL

import scala.collection.immutable.SortedMap

import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import org.apache.spark.ErrorInfo
import org.apache.spark.util.Utils

/**
 * The helper object for Delta code base to pick error class template and compile
 * the exception message.
 */
object DeltaThrowableHelper
{
  private val mapper: JsonMapper = JsonMapper.builder().addModule(DefaultScalaModule).build()

  val sparkErrorClassSource: URL =
    Utils.getSparkClassLoader.getResource("error/error-classes.json")

  def deltaErrorClassSource: URL = {
    Utils.getSparkClassLoader.getResource("error/delta-error-classes.json")
  }

  /** The error classes of spark. */
  val sparkErrorClassesMap: SortedMap[String, ErrorInfo] = {
    mapper.readValue(sparkErrorClassSource, new TypeReference[SortedMap[String, ErrorInfo]]() {})
  }

  /** The error classes of delta. */
  val deltaErrorClassToInfoMap: SortedMap[String, ErrorInfo] = {
    mapper.readValue(deltaErrorClassSource, new TypeReference[SortedMap[String, ErrorInfo]]() {})
  }
  /**
   * Combined error classes from delta and spark. There should not be same error classes between
   * deltaErrorClassesMap and sparkErrorClassesMap.
   */
  private val errorClassToInfoMap: SortedMap[String, ErrorInfo] = {
    deltaErrorClassToInfoMap ++ sparkErrorClassesMap
  }

  def getMessage(errorClass: String, messageParameters: Array[String]): String = {
    val errorInfo = errorClassToInfoMap.getOrElse(errorClass,
      throw new IllegalArgumentException(s"Cannot find error class '$errorClass'"))
    String.format(errorInfo.messageFormat, messageParameters: _*)
  }

  def getSqlState(errorClass: String): String =
    Option(errorClass).flatMap(errorClassToInfoMap.get).flatMap(_.sqlState).orNull

  def isInternalError(errorClass: String): Boolean = errorClass == "INTERNAL_ERROR"
}
