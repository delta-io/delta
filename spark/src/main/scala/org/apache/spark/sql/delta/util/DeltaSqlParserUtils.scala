/*
 * DATABRICKS CONFIDENTIAL & PROPRIETARY
 * __________________
 *
 * Copyright 2025-present Databricks, Inc.
 * All Rights Reserved.
 *
 * NOTICE:  All information contained herein is, and remains the property of Databricks, Inc.
 * and its suppliers, if any.  The intellectual and technical concepts contained herein are
 * proprietary to Databricks, Inc. and its suppliers and may be covered by U.S. and foreign Patents,
 * patents in process, and are protected by trade secret and/or copyright law. Dissemination, use,
 * or reproduction of this information is strictly forbidden unless prior written permission is
 * obtained from Databricks, Inc.
 *
 * If you view or obtain a copy of this information and believe Databricks, Inc. may not have
 * intended it to be made available, please promptly report it to Databricks Legal Department
 * @ legal@databricks.com.
 */

package com.databricks.sql.transaction.tahoe.util

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
  def parseMultipartColumnList(
      hierarchicalClusteringColumns: String): Option[Seq[UnresolvedAttribute]] = {
    // The parser rejects empty lists, so handle that specially here.
    if (hierarchicalClusteringColumns.trim.isEmpty) return Some(Nil)
    try {
      Some(multipartIdentifierSqlParser.parseMultipartIdentifierList(hierarchicalClusteringColumns))
    } catch {
      case _: ParseException => None
    }
  }
}
