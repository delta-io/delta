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

import scala.collection.JavaConverters._

import org.antlr.v4.runtime.ParserRuleContext

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.parser.{DeltaParseException, ParseException, ParseExceptionShims, ParserUtils}
import org.apache.spark.sql.catalyst.trees.Origin
import org.apache.spark.QueryContext

class DeltaAnalysisException(
    errorClass: String,
    messageParameters: Array[String],
    cause: Option[Throwable] = None,
    origin: Option[Origin] = None)
  extends AnalysisException(
    message = DeltaThrowableHelper.getMessage(errorClass, messageParameters),
    messageParameters = DeltaThrowableHelper
      .getMessageParameters(errorClass, errorSubClass = null, messageParameters).asScala.toMap,
    errorClass = Some(errorClass),
    line = origin.flatMap(_.line),
    startPosition = origin.flatMap(_.startPosition),
    context = origin.map(_.getQueryContext).getOrElse(Array.empty),
    cause = cause)
  with DeltaThrowable {
  def getMessageParametersArray: Array[String] = messageParameters
  override def getErrorClass: String = errorClass
  override def getMessageParameters: java.util.Map[String, String] =
    DeltaThrowableHelper.getMessageParameters(errorClass, errorSubClass = null, messageParameters)
  override def withPosition(origin: Origin): AnalysisException =
    new DeltaAnalysisException(errorClass, messageParameters, cause, Some(origin))
}

class DeltaIllegalArgumentException(
    errorClass: String,
    messageParameters: Array[String] = Array.empty,
    cause: Throwable = null)
  extends IllegalArgumentException(
      DeltaThrowableHelper.getMessage(errorClass, messageParameters), cause)
    with DeltaThrowable {
    override def getErrorClass: String = errorClass
  def getMessageParametersArray: Array[String] = messageParameters

  override def getMessageParameters: java.util.Map[String, String] = {
    DeltaThrowableHelper.getMessageParameters(errorClass, errorSubClass = null, messageParameters)
  }

  override def getQueryContext: Array[QueryContext] = new Array(0);
}

class DeltaUnsupportedOperationException(
    errorClass: String,
    messageParameters: Array[String] = Array.empty)
  extends UnsupportedOperationException(
      DeltaThrowableHelper.getMessage(errorClass, messageParameters))
    with DeltaThrowable {
  override def getErrorClass: String = errorClass
  def getMessageParametersArray: Array[String] = messageParameters

  override def getMessageParameters: java.util.Map[String, String] = {
    DeltaThrowableHelper.getMessageParameters(errorClass, errorSubClass = null, messageParameters)
  }

  override def getQueryContext: Array[QueryContext] = new Array(0);
}

// DeltaParseException is now defined in ParseExceptionShims
// (see scala-spark-*/shims/ParseExceptionShims.scala) to handle the different ParseException
// constructor signatures between Spark versions.
// In Spark 4.1, ParseException removed the 'stop' parameter from its constructor.

class DeltaArithmeticException(
    errorClass: String,
    messageParameters: Array[String])
  extends ArithmeticException(
      DeltaThrowableHelper.getMessage(errorClass, messageParameters))
    with DeltaThrowable {
  override def getErrorClass: String = errorClass

  override def getMessageParameters: java.util.Map[String, String] = {
    DeltaThrowableHelper.getMessageParameters(errorClass, errorSubClass = null, messageParameters)
  }
}

