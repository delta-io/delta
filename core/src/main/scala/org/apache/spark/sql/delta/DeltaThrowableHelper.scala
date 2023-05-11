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
import java.io.FileNotFoundException
import java.net.URL

import org.apache.spark.ErrorClassesJsonReader
import org.apache.spark.util.Utils

/**
 * The helper object for Delta code base to pick error class template and compile
 * the exception message.
 */
object DeltaThrowableHelper
{
  /**
   * Try to find the error class source file and throw exception if it is no found.
   */
  private def safeGetErrorClassesSource(sourceFile: String): URL = {
    val classLoader = Utils.getContextOrSparkClassLoader
    Option(classLoader.getResource(sourceFile)).getOrElse {
      throw new FileNotFoundException(
        s"""Cannot find the error class definition file on path $sourceFile" through the """ +
          s"class loader ${classLoader.toString}")
    }
  }

  lazy val sparkErrorClassSource: URL = {
    safeGetErrorClassesSource("error/error-classes.json")
  }

  def deltaErrorClassSource: URL = {
    safeGetErrorClassesSource("error/delta-error-classes.json")
  }

  private val errorClassReader = new ErrorClassesJsonReader(
    Seq(deltaErrorClassSource, sparkErrorClassSource))

  def getMessage(errorClass: String, messageParameters: Array[String]): String = {
    val template = errorClassReader.getMessageTemplate(errorClass)
    String.format(template.replaceAll("<[a-zA-Z0-9_-]+>", "%s"),
      messageParameters: _*)
  }

  def getSqlState(errorClass: String): String = errorClassReader.getSqlState(errorClass)

  def isInternalError(errorClass: String): Boolean = errorClass == "INTERNAL_ERROR"

}
