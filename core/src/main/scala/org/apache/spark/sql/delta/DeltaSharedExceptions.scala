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

import org.antlr.v4.runtime.ParserRuleContext

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.parser.{ParseException, ParserUtils}
import org.apache.spark.sql.catalyst.trees.Origin

class DeltaAnalysisException(
    errorClass: String,
    messageParameters: Array[String],
    cause: Option[Throwable] = None,
    origin: Option[Origin] = None)
  extends AnalysisException(
    message = DeltaThrowableHelper.getMessage(errorClass, messageParameters),
    errorClass = Some(errorClass),
    line = origin.flatMap(_.line),
    startPosition = origin.flatMap(_.startPosition),
    context = origin.map(_.getQueryContext).getOrElse(Array.empty),
    cause = cause)
  with DeltaThrowable {
  def getMessageParametersArray: Array[String] = messageParameters
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
}

class DeltaUnsupportedOperationException(
    errorClass: String,
    messageParameters: Array[String] = Array.empty)
  extends UnsupportedOperationException(
      DeltaThrowableHelper.getMessage(errorClass, messageParameters))
    with DeltaThrowable {
    override def getErrorClass: String = errorClass
    def getMessageParametersArray: Array[String] = messageParameters
}

class DeltaParseException(
    message: String,
    ctx: ParserRuleContext)
  extends ParseException(
      Option(ParserUtils.command(ctx)),
      message,
      ParserUtils.position(ctx.getStart),
      ParserUtils.position(ctx.getStop)
    ) with DeltaThrowable
