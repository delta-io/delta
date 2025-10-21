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

package org.apache.spark.sql.delta.util

import scala.collection.JavaConverters._

import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.parser.{AbstractSqlParser, AstBuilder, ParseException, ParserUtils, SqlBaseParser}

/**
 * Utility functions for SQL parsing operations.
 */
object DeltaSqlParserUtils {
  /**
   * The SQL grammar already includes a `multipartIdentifierList` rule for parsing a string into a
   * list of multi-part identifiers. We just expose it here, with a custom parser and AstBuilder.
   */
  private class MultipartIdentifierSqlParser extends AbstractSqlParser {
    override val astBuilder = new AstBuilder {
      override def visitMultipartIdentifierList(ctx: SqlBaseParser.MultipartIdentifierListContext)
      : Seq[UnresolvedAttribute] = ParserUtils.withOrigin(ctx) {
        ctx.multipartIdentifier.asScala.toSeq.map(typedVisit[Seq[String]])
          .map(new UnresolvedAttribute(_))
      }
    }
    def parseMultipartIdentifierList(sqlText: String): Seq[UnresolvedAttribute] = {
      parse(sqlText) { parser =>
        astBuilder.visitMultipartIdentifierList(parser.multipartIdentifierList())
      }
    }
  }

  private val multipartIdentifierSqlParser = new MultipartIdentifierSqlParser

  /** Parses a comma-separated list of column names; returns None if parsing fails. */
  def parseMultipartColumnList(columns: String): Option[Seq[UnresolvedAttribute]] = {
    // The parser rejects empty lists, so handle that specially here.
    if (columns.trim.isEmpty) return Some(Nil)
    try {
      Some(multipartIdentifierSqlParser.parseMultipartIdentifierList(columns))
    } catch {
      case _: ParseException => None
    }
  }
}
