/*
 * Copyright (2025) The Delta Lake Project Authors.
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

package org.apache.spark.sql.catalyst.parser

import org.antlr.v4.runtime.ParserRuleContext

import org.apache.spark.sql.catalyst.parser.{ParseException, ParserUtils}
import org.apache.spark.sql.catalyst.trees.Origin
import org.apache.spark.sql.delta.DeltaThrowable

/**
 * DeltaParseException for Spark 4.0 and earlier.
 * In these versions, ParseException takes both start and stop Origin parameters.
 */
class DeltaParseException(
    ctx: ParserRuleContext,
    errorClass: String,
    messageParameters: Map[String, String] = Map.empty)
  extends ParseException(
      Option(ParserUtils.command(ctx)),
      ParserUtils.position(ctx.getStart),
      ParserUtils.position(ctx.getStop),  // In Spark 4.0, we have the stop parameter
      errorClass,
      messageParameters
    ) with DeltaThrowable {
  override def getErrorClass: String = errorClass
}

/**
 * Shim for ParseException to handle API changes between Spark versions.
 * In Spark 4.0 and earlier, ParseException has separate start and stop parameters.
 */
object ParseExceptionShims {

  /**
   * Create a ParseException with the appropriate constructor for this Spark version.
   * In Spark 4.0, we use both start and stop Origin parameters.
   */
  def createParseException(
      command: Option[String],
      start: Origin,
      stop: Origin,
      errorClass: String,
      messageParameters: Map[String, String]): ParseException = {
    new ParseException(command, start, stop, errorClass, messageParameters)
  }
}
