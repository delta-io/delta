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

import org.apache.spark.QueryContext
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.parser.{ParseException, ParserUtils}
import org.apache.spark.sql.catalyst.trees.Origin

class DeltaAnalysisException(
    message: String,
    line: Option[Int] = None,
    startPosition: Option[Int] = None,
    cause: Option[Throwable] = None,
    errorClass: Option[String] = None,
    messageParameters: Array[String] = Array.empty,
    context: Array[QueryContext] = Array.empty)
  extends AnalysisException(
    message,
    line,
    startPosition,
    cause,
    errorClass,
    messageParameters = DeltaThrowableHelper
      .getParameterNames(errorClass, errorSubClass = None)
      .zip(messageParameters)
      .toMap,
    context)
    with DeltaThrowable {

  /* Implemented for testing */
  private[delta] def getMessageParametersArray: Array[String] = messageParameters

  def this(
    errorClass: String,
    messageParameters: Array[String]) =
    this(
      message = DeltaThrowableHelper.getMessage(errorClass, messageParameters),
      messageParameters = messageParameters,
      errorClass = Some(errorClass)
    )

  def this(
    errorClass: String,
    messageParameters: Array[String],
    cause: Option[Throwable]) =
    this(
      message = DeltaThrowableHelper.getMessage(errorClass, messageParameters),
      messageParameters = messageParameters,
      errorClass = Some(errorClass),
      cause = cause)

  def this(
    errorClass: String,
    messageParameters: Array[String],
    cause: Option[Throwable],
    origin: Option[Origin]) =
    this(
      message = DeltaThrowableHelper.getMessage(errorClass, messageParameters),
      messageParameters = messageParameters,
      errorClass = Some(errorClass),
      line = origin.flatMap(_.line),
      startPosition = origin.flatMap(_.startPosition),
      context = origin.map(_.getQueryContext).getOrElse(Array.empty),
      cause = cause)
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
    DeltaThrowableHelper.getParameterNames(Option(errorClass), errorSubClass = None)
      .zip(messageParameters).toMap.asJava
  }
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
    DeltaThrowableHelper.getParameterNames(Option(errorClass), errorSubClass = None)
      .zip(messageParameters).toMap.asJava
  }
}

class DeltaParseException(
    ctx: ParserRuleContext,
    errorClass: String,
    messageParameters: Map[String, String] = Map.empty)
  extends ParseException(
      Option(ParserUtils.command(ctx)),
      ParserUtils.position(ctx.getStart),
      ParserUtils.position(ctx.getStop),
      errorClass,
      messageParameters
    ) with DeltaThrowable

class DeltaArithmeticException(
    errorClass: String,
    messageParameters: Array[String])
  extends ArithmeticException(
      DeltaThrowableHelper.getMessage(errorClass, messageParameters))
    with DeltaThrowable {
  override def getErrorClass: String = errorClass

  override def getMessageParameters: java.util.Map[String, String] = {
    DeltaThrowableHelper.getParameterNames(Option(errorClass), errorSubClass = None)
      .zip(messageParameters).toMap.asJava
  }
}

