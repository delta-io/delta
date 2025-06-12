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

import scala.collection.JavaConverters._

import org.apache.spark.sql.delta.DeltaThrowableHelperShims._

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
    safeGetErrorClassesSource(SPARK_ERROR_CLASS_SOURCE_FILE)
  }

  def deltaErrorClassSource: URL = {
    safeGetErrorClassesSource("error/delta-error-classes.json")
  }

  private val errorClassReader = new ErrorClassesJsonReader(
    Seq(deltaErrorClassSource, sparkErrorClassSource))

  def getMessage(errorClass: String, messageParameters: Array[String]): String = {
    validateParameterValues(errorClass, errorSubClass = null, messageParameters)
    val template = errorClassReader.getMessageTemplate(errorClass)
    val message = String.format(template.replaceAll("<[a-zA-Z0-9_-]+>", "%s"),
      messageParameters: _*)
    s"[$errorClass] $message"
  }

  def getSqlState(errorClass: String): String = errorClassReader.getSqlState(errorClass)

  def isInternalError(errorClass: String): Boolean = errorClass == "INTERNAL_ERROR"

  def getParameterNames(errorClass: String, errorSubClass: String): Array[String] = {
    val wholeErrorClass = if (errorSubClass == null) {
      errorClass
    } else {
      errorClass + "." + errorSubClass
    }
    val parameterizedMessage = errorClassReader.getMessageTemplate(wholeErrorClass)
    val pattern = "<[a-zA-Z0-9_-]+>".r
    val matches = pattern.findAllIn(parameterizedMessage)
    val parameterSeq = matches.toArray
    val parameterNames = parameterSeq.map(p => p.stripPrefix("<").stripSuffix(">"))
    parameterNames
  }

  def getMessageParameters(
      errorClass: String,
      errorSubClass: String,
      parameterValues: Array[String]): java.util.Map[String, String] = {
    validateParameterValues(errorClass, errorSubClass, parameterValues)
    getParameterNames(errorClass, errorSubClass).zip(parameterValues).toMap.asJava
  }

  /**
   * Verify that the provided parameter values match the parameter names in the error message
   * template. The number of parameters must match, and the parameters with the same name must
   * have the same value.
   */
  private def validateParameterValues(
      errorClass: String,
      errorSubClass: String,
      parameterValues: Array[String]): Unit = if (Utils.isTesting) {
    val parameterNames = getParameterNames(errorClass, errorSubClass)
    assert(parameterNames.size == parameterValues.size, "The number of parameter values provided " +
      s"to error class $errorClass ${Option(errorSubClass).getOrElse("")}  does not match the " +
      s"number of parameters in the error message template.\n" +
      s"Parameters in the template: ${parameterNames.mkString(", ")}\n" +
      s"Parameter values provided: ${parameterValues.mkString(", ")}")
    val parameterPairs = parameterNames.zip(parameterValues)
    val parameterMap = parameterPairs.toMap
    parameterPairs.foreach { case (name, value) =>
      assert(parameterMap(name) == value, s"Parameter <$name> in the error message for error " +
        s"class $errorClass ${Option(errorSubClass).getOrElse("")} was assigned two different " +
        s"values: ${parameterMap(name)} and $value")
    }
  }
}
