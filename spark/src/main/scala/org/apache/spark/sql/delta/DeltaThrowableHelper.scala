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

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.{SparkException, SparkThrowable}

import org.apache.spark.ErrorClassesJsonReader
import org.apache.spark.util.Utils

/**
 * The helper object for Delta code base to pick error class template and compile
 * the exception message.
 */
object DeltaThrowableHelper
{
  /**
   * Handles a breaking change (SPARK-46810) between Spark 3.5 and Spark Master (4.0) where
   * `error-classes.json` was renamed to `error-conditions.json`.
   */
  val SPARK_ERROR_CLASS_SOURCE_FILE = "error/error-conditions.json"

  def showColumnsWithConflictDatabasesError(
      db: Seq[String], v1TableName: TableIdentifier): Throwable = {
    QueryCompilationErrors.showColumnsWithConflictNamespacesError(
      namespaceA = db,
      namespaceB = v1TableName.database.get :: Nil)
  }

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

  /**
   * @return The formated error message. The format for standalone error classes is:
   *         [ERROR_CLASS] Main error message
   *         The format for errors with sub-error classes:
   *         [MAIN_CLASS.SUB_CLASS] Main error message Sub-error message
   */
  def getMessage(errorClass: String, messageParameters: Array[String]): String = {
    validateParameterValues(errorClass, errorSubClass = null, messageParameters)
    val template = errorClassReader.getMessageTemplate(errorClass)
    val message = formatMessage(errorClass, messageParameters, template)
    s"[$errorClass] $message"
  }

  private def formatMessage(
      errorClass: String,
      messageParameters: Array[String],
      template: String) = {
    String.format(template.replaceAll("<[a-zA-Z0-9_-]+>", "%s"), messageParameters: _*)
  }

  /**
   * Returns a combined error message for an error class with multiple sub-error classes.
   * Use [[getMessage]] to load a single sub-error class message prefixed with
   * the main class message.
   * @return The formatted error message including main and sub-error messages. The format is:
   *         [ERROR_CLASS] Main error message
   *         - Sub-error message 1
   *         - Sub-error message 2
   *         ...
   */
  def getMessageWithSubErrors(
      mainErrorClass: String,
      mainMessageParameters: Array[String],
      subErrorInformationSeq: Seq[(String, Array[String])]): String = {
    require(subErrorInformationSeq.nonEmpty)
    // Get main message
    val mainMessage = {
      val template = getMainMessageTemplate(mainErrorClass)
      formatMessage(mainErrorClass, mainMessageParameters, template)
    }

    // Get sub-error messages
    val subMessageSeq = subErrorInformationSeq.map {
      case (subErrorClass, subMessageParameters) =>
        val fullErrorClass = s"$mainErrorClass.$subErrorClass"
        val template = getSubMessageTemplate(fullErrorClass)
        formatMessage(fullErrorClass, subMessageParameters, template)
    }

    // Combine main and sub errors
    s"[$mainErrorClass] $mainMessage\n${subMessageSeq.map("- " + _ + "\n")
      .mkString.stripSuffix("\n")}"
  }

  /**
   * Get the message template for a main error class.
   * @param errorClass The main error class. It can only be MAIN_CLASS (not MAIN_CLASS.SUB_CLASS).
   * @return The message template.
   */
  def getMainMessageTemplate(errorClass: String): String = {
    val errorClasses = errorClass.split("\\.")
    assert(errorClasses.length == 1)

    val mainErrorClass = errorClasses.head
    val errorInfo = errorClassReader.errorInfoMap.getOrElse(
      mainErrorClass,
      throw SparkException.internalError(s"Cannot find main error class '$errorClass'"))

    errorInfo.messageTemplate
  }

  /**
   * Get the message template for a sub error class without prefixing with the main error template.
   * @param errorClass The sub error class. It can only be MAIN_CLASS.SUB_CLASS (not MAIN_CLASS).
   * @return The message template.
   */
  def getSubMessageTemplate(errorClass: String): String = {
    val errorClasses = errorClass.split("\\.")
    assert(errorClasses.length == 2)

    val mainErrorClass = errorClasses.head
    val subErrorClass = errorClasses.last
    val errorInfo = errorClassReader.errorInfoMap.getOrElse(
      mainErrorClass,
      throw SparkException.internalError(s"Cannot find main error class '$errorClass'"))
    assert(errorInfo.subClass.isDefined, errorClass)

    val errorSubInfo = errorInfo.subClass.get.getOrElse(
      subErrorClass,
      throw SparkException.internalError(s"Cannot find sub error class '$errorClass'"))
    errorSubInfo.messageTemplate
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
    parsePrameterNamesFromParameterizedMessage(parameterizedMessage)
  }

  private def parsePrameterNamesFromParameterizedMessage(parameterizedMessage: String) = {
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

  def getMainErrorMessageParameters(
      errorClass: String,
      parameterValues: Array[String]): java.util.Map[String, String] = {
    val parameterizedMessage = getMainMessageTemplate(errorClass)
    parsePrameterNamesFromParameterizedMessage(parameterizedMessage)
      .zip(parameterValues).toMap.asJava
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
