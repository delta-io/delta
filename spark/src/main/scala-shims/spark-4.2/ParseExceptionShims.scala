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
 * DeltaParseException for Spark 4.2 (same as Spark 4.1).
 * In this version, ParseException only takes a single origin parameter (stop was removed).
 */
class DeltaParseException(
    ctx: ParserRuleContext,
    errorClass: String,
    messageParameters: Map[String, String] = Map.empty)
  extends ParseException(
      Option(ParserUtils.command(ctx)),
      ParserUtils.position(ctx.getStart),  // In Spark 4.2, only start position is used
      // No stop parameter in Spark 4.2
      errorClass,
      messageParameters
    ) with DeltaThrowable {
  override def getErrorClass: String = errorClass
}

/**
 * Shim for ParseException to handle API changes between Spark versions.
 * In Spark 4.2, ParseException only has a single origin parameter (same as Spark 4.1,
 * stop was removed).
 */
object ParseExceptionShims {

  /**
   * Create a ParseException with the appropriate constructor for this Spark version.
   * In Spark 4.2, we only use the start Origin (same as Spark 4.1, stop parameter was removed).
   */
  def createParseException(
      command: Option[String],
      start: Origin,
      stop: Origin,  // This parameter is ignored in Spark 4.2 (same as 4.1)
      errorClass: String,
      messageParameters: Map[String, String]): ParseException = {
    // In Spark 4.2, ParseException only takes a single origin parameter (same as 4.1)
    new ParseException(command, start, errorClass, messageParameters)
  }
}
