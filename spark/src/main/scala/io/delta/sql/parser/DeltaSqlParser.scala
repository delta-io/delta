/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * This file contains code from the Apache Spark project (original license above).
 * It contains modifications, which are licensed as follows:
 */

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

package io.delta.sql.parser

import java.util.Locale

import scala.collection.JavaConverters._

import org.apache.spark.sql.catalyst.TimeTravel
import org.apache.spark.sql.delta.skipping.clustering.temp.{AlterTableClusterBy, ClusterByParserUtils, ClusterByPlan, ClusterBySpec}

import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.commands._
import io.delta.sql.parser.DeltaSqlBaseParser._
import io.delta.tables.execution.VacuumTableCommand
import org.antlr.v4.runtime._
import org.antlr.v4.runtime.atn.PredictionMode
import org.antlr.v4.runtime.misc.{Interval, ParseCancellationException}
import org.antlr.v4.runtime.tree._

import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.analysis.UnresolvedTableImplicits._
import org.apache.spark.sql.catalyst.parser.{CompoundBody, ParseErrorListener, ParseException, ParserInterface, ParserInterfaceShims}
import org.apache.spark.sql.catalyst.parser.ParserUtils.{checkDuplicateClauses, string, withOrigin}
import org.apache.spark.sql.catalyst.plans.logical.{AlterColumnSyncIdentity, AlterTableAddConstraint, AlterTableDropConstraint, AlterTableDropFeature, CloneTableStatement, LogicalPlan, RestoreTableStatement}
import org.apache.spark.sql.catalyst.trees.Origin
import org.apache.spark.sql.connector.catalog.{CatalogV2Util, TableCatalog}
import org.apache.spark.sql.errors.QueryParsingErrors
import org.apache.spark.sql.internal.{SQLConf, VariableSubstitution}
import org.apache.spark.sql.types._

/**
 * A SQL parser that tries to parse Delta commands. If failing to parse the SQL text, it will
 * forward the call to `delegate`.
 */
class DeltaSqlParser(val delegateSpark: ParserInterface) extends ParserInterfaceShims {
  private val delegate = ParserInterfaceShims(delegateSpark)
  private val builder = new DeltaSqlAstBuilder
  private val substitution = new VariableSubstitution

  override def parsePlan(sqlText: String): LogicalPlan = parse(sqlText) { parser =>
    builder.visit(parser.singleStatement()) match {
      case clusterByPlan: ClusterByPlan =>
        ClusterByParserUtils(clusterByPlan, delegate).parsePlan(sqlText)
      case plan: LogicalPlan => plan
      case _ => delegate.parsePlan(sqlText)
    }
  }

  /**
   * This API is used just for parsing the SELECT queries. Delta parser doesn't override
   * the Spark parser, that means this can be delegated directly to the Spark parser.
   */
  override def parseQuery(sqlText: String): LogicalPlan = delegate.parseQuery(sqlText)

  // scalastyle:off line.size.limit
  /**
   * Fork from `org.apache.spark.sql.catalyst.parser.AbstractSqlParser#parse(java.lang.String, scala.Function1)`.
   *
   * @see https://github.com/apache/spark/blob/v2.4.4/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/parser/ParseDriver.scala#L81
   */
  // scalastyle:on
  protected def parse[T](command: String)(toResult: DeltaSqlBaseParser => T): T = {
    val lexer = new DeltaSqlBaseLexer(
      new UpperCaseCharStream(CharStreams.fromString(substitution.substitute(command))))
    lexer.removeErrorListeners()
    lexer.addErrorListener(ParseErrorListener)

    val tokenStream = new CommonTokenStream(lexer)
    val parser = new DeltaSqlBaseParser(tokenStream)
    parser.addParseListener(PostProcessor)
    parser.removeErrorListeners()
    parser.addErrorListener(ParseErrorListener)

    try {
      try {
        // first, try parsing with potentially faster SLL mode
        parser.getInterpreter.setPredictionMode(PredictionMode.SLL)
        toResult(parser)
      } catch {
        case e: ParseCancellationException =>
          // if we fail, parse with LL mode
          tokenStream.seek(0) // rewind input stream
          parser.reset()

          // Try Again.
          parser.getInterpreter.setPredictionMode(PredictionMode.LL)
          toResult(parser)
      }
    } catch {
      case e: ParseException if e.command.isDefined =>
        throw e
      case e: ParseException =>
        throw e.withCommand(command)
      case e: AnalysisException =>
        val position = Origin(e.line, e.startPosition)
        throw new ParseException(
          command = Option(command),
          start = position,
          stop = position,
          errorClass = "DELTA_PARSING_ANALYSIS_ERROR",
          messageParameters = Map("msg" -> e.message))
    }
  }

  override def parseExpression(sqlText: String): Expression = delegate.parseExpression(sqlText)

  override def parseTableIdentifier(sqlText: String): TableIdentifier =
    delegate.parseTableIdentifier(sqlText)

  override def parseFunctionIdentifier(sqlText: String): FunctionIdentifier =
    delegate.parseFunctionIdentifier(sqlText)

  override def parseMultipartIdentifier (sqlText: String): Seq[String] =
    delegate.parseMultipartIdentifier(sqlText)

  override def parseTableSchema(sqlText: String): StructType = delegate.parseTableSchema(sqlText)

  override def parseDataType(sqlText: String): DataType = delegate.parseDataType(sqlText)

  override def parseScript(sqlScriptText: String): CompoundBody =
    delegate.parseScript(sqlScriptText)
}

/**
 * Define how to convert an AST generated from `DeltaSqlBase.g4` to a `LogicalPlan`. The parent
 * class `DeltaSqlBaseBaseVisitor` defines all visitXXX methods generated from `#` instructions in
 * `DeltaSqlBase.g4` (such as `#vacuumTable`).
 */
class DeltaSqlAstBuilder extends DeltaSqlBaseBaseVisitor[AnyRef] {

  import org.apache.spark.sql.catalyst.parser.ParserUtils._

  /**
   * Convert a property list into a key-value map.
   * This should be called through [[visitPropertyKeyValues]] or [[visitPropertyKeys]].
   */
  override def visitPropertyList(
      ctx: PropertyListContext): Map[String, String] = withOrigin(ctx) {
    val properties = ctx.property.asScala.map { property =>
      val key = visitPropertyKey(property.key)
      val value = visitPropertyValue(property.value)
      key -> value
    }
    // Check for duplicate property names.
    checkDuplicateKeys(properties.toSeq, ctx)
    properties.toMap
  }

  /**
   * Parse a key-value map from a [[PropertyListContext]], assuming all values are specified.
   */
  def visitPropertyKeyValues(ctx: PropertyListContext): Map[String, String] = {
    val props = visitPropertyList(ctx)
    val badKeys = props.collect { case (key, null) => key }
    if (badKeys.nonEmpty) {
      operationNotAllowed(
        s"Values must be specified for key(s): ${badKeys.mkString("[", ",", "]")}", ctx)
    }
    props
  }

  /**
   * Parse a list of keys from a [[PropertyListContext]], assuming no values are specified.
   */
  def visitPropertyKeys(ctx: PropertyListContext): Seq[String] = {
    val props = visitPropertyList(ctx)
    val badKeys = props.filter { case (_, v) => v != null }.keys
    if (badKeys.nonEmpty) {
      operationNotAllowed(
        s"Values should not be specified for key(s): ${badKeys.mkString("[", ",", "]")}", ctx)
    }
    props.keys.toSeq
  }

  /**
   * A property key can either be String or a collection of dot separated elements. This
   * function extracts the property key based on whether its a string literal or a property
   * identifier.
   */
  override def visitPropertyKey(key: PropertyKeyContext): String = {
    if (key.stringLit() != null) {
      string(visitStringLit(key.stringLit()))
    } else {
      key.getText
    }
  }

  /**
   * A property value can be String, Integer, Boolean or Decimal. This function extracts
   * the property value based on whether its a string, integer, boolean or decimal literal.
   */
  override def visitPropertyValue(value: PropertyValueContext): String = {
    if (value == null) {
      null
    } else if (value.identifier != null) {
      value.identifier.getText
    } else if (value.value != null) {
      string(visitStringLit(value.value))
    } else if (value.booleanValue != null) {
      value.getText.toLowerCase(Locale.ROOT)
    } else {
      value.getText
    }
  }

  override def visitStringLit(ctx: StringLitContext): Token = {
    if (ctx != null) {
      if (ctx.STRING != null) {
        ctx.STRING.getSymbol
      } else {
        ctx.DOUBLEQUOTED_STRING.getSymbol
      }
    } else {
      null
    }
  }

  /**
   * Parse either create table header or replace table header.
   * @return TableIdentifier for the target table
   *         Boolean for whether we are creating a table
   *         Boolean for whether we are replacing a table
   *         Boolean for whether we are creating a table if not exists
   */
  override def visitCloneTableHeader(
      ctx: CloneTableHeaderContext): (TableIdentifier, Boolean, Boolean, Boolean) = withOrigin(ctx) {
    ctx.children.asScala.head match {
      case createHeader: CreateTableHeaderContext =>
        (visitTableIdentifier(createHeader.table), true, false, createHeader.EXISTS() != null)
      case replaceHeader: ReplaceTableHeaderContext =>
        (visitTableIdentifier(replaceHeader.table), replaceHeader.CREATE() != null, true, false)
      case _ =>
        throw new DeltaParseException(ctx, "DELTA_PARSING_INCORRECT_CLONE_HEADER")
    }
  }

  /**
   * Creates a [[CloneTableStatement]] logical plan. Example SQL:
   * {{{
   *   CREATE [OR REPLACE] TABLE <table-identifier> SHALLOW CLONE <source-table-identifier>
   *     [TBLPROPERTIES ('propA' = 'valueA', ...)]
   *     [LOCATION '/path/to/cloned/table']
   * }}}
   */
  override def visitClone(ctx: CloneContext): LogicalPlan = withOrigin(ctx) {
    val (target, isCreate, isReplace, ifNotExists) = visitCloneTableHeader(ctx.cloneTableHeader())

    if (!isCreate && ifNotExists) {
      throw new DeltaParseException(
        ctx.cloneTableHeader(),
        "DELTA_PARSING_MUTUALLY_EXCLUSIVE_CLAUSES",
        Map("clauseOne" -> "IF NOT EXISTS", "clauseTwo" -> "REPLACE")
      )
    }

    // Get source for clone (and time travel source if necessary)
    // The source relation can be an Iceberg table in form of `catalog.db.table` so we visit
    // a multipart identifier instead of TableIdentifier (which does not support 3L namespace)
    // in Spark 3.3. In Spark 3.4 we should have TableIdentifier supporting 3L namespace so we
    // could revert back to that.
    val sourceRelation = new UnresolvedRelation(visitMultipartIdentifier(ctx.source))
    val maybeTimeTravelSource = maybeTimeTravelChild(ctx.clause, sourceRelation)
    val targetRelation = UnresolvedRelation(target.nameParts)

    val tablePropertyOverrides = Option(ctx.tableProps)
      .map(visitPropertyKeyValues)
      .getOrElse(Map.empty[String, String])

    CloneTableStatement(
      maybeTimeTravelSource,
      targetRelation,
      ifNotExists,
      isReplace,
      isCreate,
      tablePropertyOverrides,
      Option(ctx.location).map(s => string(visitStringLit(s))))
  }

  /**
   * Create a [[VacuumTableCommand]] logical plan. Example SQL:
   * {{{
   *   VACUUM ('/path/to/dir' | delta.`/path/to/dir`) [RETAIN number HOURS] [DRY RUN];
   * }}}
   */
  override def visitVacuumTable(ctx: VacuumTableContext): AnyRef = withOrigin(ctx) {
    VacuumTableCommand(
      path = Option(ctx.path).map(string),
      table = Option(ctx.table).map(visitTableIdentifier),
      inventoryTable = Option(ctx.inventoryTable).map(visitTableIdentifier),
      inventoryQuery = Option(ctx.inventoryQuery).map(extractRawText),
      horizonHours = Option(ctx.number).map(_.getText.toDouble),
      dryRun = ctx.RUN != null)
  }

  /** Provides a list of unresolved attributes for multi dimensional clustering. */
  override def visitZorderSpec(ctx: ZorderSpecContext): Seq[UnresolvedAttribute] = {
    ctx.interleave.asScala
      .map(_.identifier.asScala.map(_.getText).toSeq)
      .map(new UnresolvedAttribute(_)).toSeq
  }

  /**
   * Create a [[OptimizeTableCommand]] logical plan.
   * Syntax:
   * {{{
   *    OPTIMIZE <table-identifier>
   *      [WHERE predicate-using-partition-columns]
   *      [ZORDER BY [(] col1, col2 ..[)]]
   * }}}
   * Examples:
   * {{{
   *    OPTIMIZE '/path/to/delta/table';
   *    OPTIMIZE delta_table_name;
   *    OPTIMIZE delta.`/path/to/delta/table`;
   *    OPTIMIZE delta_table_name WHERE partCol = 25;
   *    OPTIMIZE delta_table_name WHERE partCol = 25 ZORDER BY col2, col2;
   * }}}
   */
  override def visitOptimizeTable(ctx: OptimizeTableContext): AnyRef = withOrigin(ctx) {
    if (ctx.path == null && ctx.table == null) {
      throw new DeltaParseException(
        ctx,
        "DELTA_PARSING_MISSING_TABLE_NAME_OR_PATH",
        Map("command" -> "OPTIMIZE")
      )
    }
    val interleaveBy = Option(ctx.zorderSpec).map(visitZorderSpec).getOrElse(Seq.empty)
    OptimizeTableCommand(
      Option(ctx.path).map(string),
      Option(ctx.table).map(visitTableIdentifier),
      Option(ctx.partitionPredicate).map(extractRawText(_)).toSeq,
      DeltaOptimizeContext(isFull = ctx.FULL != null))(interleaveBy)
  }

  /**
   * Creates a [[DeltaReorgTable]] logical plan.
   * Examples:
   * {{{
   *   -- Physically delete dropped rows and columns of target table
   *   REORG TABLE (delta.`/path/to/table` | delta_table_name)
   *    [WHERE partition_predicate] APPLY (PURGE)
   *
   *   -- Rewrite the files in UNIFORM(ICEBERG) compliant way.
   *   REORG TABLE table_name (delta.`/path/to/table` | catalog.db.table)
   *    APPLY (UPGRADE UNIFORM(ICEBERG_COMPAT_VERSION=version))
   * }}}
   */
  override def visitReorgTable(ctx: ReorgTableContext): AnyRef = withOrigin(ctx) {
    if (ctx.table == null) {
      throw new DeltaParseException(
        ctx,
        "DELTA_PARSING_MISSING_TABLE_NAME_OR_PATH",
        Map("command" -> "REORG")
      )
    }

    val targetIdentifier = visitTableIdentifier(ctx.table)
    val tableNameParts = targetIdentifier.database.toSeq :+ targetIdentifier.table
    val targetTable = UnresolvedTable(tableNameParts, "REORG")

    val reorgTableSpec = if (ctx.PURGE != null) {
      DeltaReorgTableSpec(DeltaReorgTableMode.PURGE, None)
    } else if (ctx.ICEBERG_COMPAT_VERSION != null) {
      DeltaReorgTableSpec(DeltaReorgTableMode.UNIFORM_ICEBERG, Option(ctx.version).map(_.getText.toInt))
    } else {
      throw new ParseException(
        "Invalid syntax: REORG TABLE only support PURGE/UPGRADE UNIFORM.",
        ctx)
    }

    DeltaReorgTable(targetTable, reorgTableSpec)(Option(ctx.partitionPredicate).map(extractRawText(_)).toSeq)
  }

  override def visitDescribeDeltaDetail(
      ctx: DescribeDeltaDetailContext): LogicalPlan = withOrigin(ctx) {
    DescribeDeltaDetailCommand(
      Option(ctx.path).map(string),
      Option(ctx.table).map(visitTableIdentifier),
      Map.empty)
  }

  override def visitDescribeDeltaHistory(
      ctx: DescribeDeltaHistoryContext): LogicalPlan = withOrigin(ctx) {
    DescribeDeltaHistory(
      Option(ctx.path).map(string),
      Option(ctx.table).map(visitTableIdentifier),
      Option(ctx.limit).map(_.getText.toInt))
  }

  override def visitGenerate(ctx: GenerateContext): LogicalPlan = withOrigin(ctx) {
    DeltaGenerateCommand(
      modeName = ctx.modeName.getText,
      tableId = visitTableIdentifier(ctx.table),
      Map.empty)
  }

  override def visitConvert(ctx: ConvertContext): LogicalPlan = withOrigin(ctx) {
    ConvertToDeltaCommand(
      visitTableIdentifier(ctx.table),
      Option(ctx.colTypeList).map(colTypeList => StructType(visitColTypeList(colTypeList))),
      ctx.STATISTICS() == null, None)
  }

  override def visitRestore(ctx: RestoreContext): LogicalPlan = withOrigin(ctx) {
    val tableRelation = UnresolvedRelation(visitTableIdentifier(ctx.table).nameParts)
    val timeTravelTableRelation = maybeTimeTravelChild(ctx.clause, tableRelation)
    RestoreTableStatement(timeTravelTableRelation.asInstanceOf[TimeTravel])
  }

  /**
   * Captures any CLUSTER BY clause and creates a [[ClusterByPlan]] logical plan.
   * The plan will be used as a sentinel for DeltaSqlParser to process it further.
   */
  override def visitClusterBy(ctx: ClusterByContext): LogicalPlan = withOrigin(ctx) {
    val clusterBySpecCtx = ctx.clusterBySpec.asScala.head
    checkDuplicateClauses(ctx.clusterBySpec, "CLUSTER BY", clusterBySpecCtx)
    val columnNames =
      clusterBySpecCtx.interleave.asScala
        .map(_.identifier.asScala.map(_.getText).toSeq)
        .map(_.asInstanceOf[Seq[String]]).toSeq
    // get CLUSTER BY clause positions.
    val startIndex = clusterBySpecCtx.getStart.getStartIndex
    val stopIndex = clusterBySpecCtx.getStop.getStopIndex

    // get CLUSTER BY parenthesis positions.
    val parenStartIndex = clusterBySpecCtx.LEFT_PAREN().getSymbol.getStartIndex
    val parenStopIndex = clusterBySpecCtx.RIGHT_PAREN().getSymbol.getStopIndex
    ClusterByPlan(
      ClusterBySpec(columnNames),
      startIndex,
      stopIndex,
      parenStartIndex,
      parenStopIndex,
      clusterBySpecCtx)
  }

  /**
   * Time travel the table to the given version or timestamp.
   */
  private def maybeTimeTravelChild(ctx: TemporalClauseContext, child: LogicalPlan): LogicalPlan = {
    if (ctx == null) return child
    TimeTravel(
      child,
      Option(ctx.timestamp).map(token => Literal(token.getText.replaceAll("^'|'$", ""))),
      Option(ctx.version).map(_.getText.toLong),
      Some("sql"))
  }

  override def visitSingleStatement(ctx: SingleStatementContext): LogicalPlan = withOrigin(ctx) {
    visit(ctx.statement).asInstanceOf[LogicalPlan]
  }

  protected def visitTableIdentifier(ctx: QualifiedNameContext): TableIdentifier = withOrigin(ctx) {
    ctx.identifier.asScala.toSeq match {
      case Seq(tbl) => TableIdentifier(tbl.getText)
      case Seq(db, tbl) => TableIdentifier(tbl.getText, Some(db.getText))
      case Seq(catalog, db, tbl) =>
        TableIdentifier(tbl.getText, Some(db.getText), Some(catalog.getText))
      case _ => throw new DeltaParseException(
        ctx,
        "DELTA_PARSING_ILLEGAL_TABLE_NAME",
        Map("table" -> ctx.getText))
    }
  }

  protected def visitMultipartIdentifier(ctx: QualifiedNameContext): Seq[String] = withOrigin(ctx) {
    ctx.identifier.asScala.map(_.getText).toSeq
  }

  override def visitPassThrough(ctx: PassThroughContext): LogicalPlan = null

  override def visitColTypeList(ctx: ColTypeListContext): Seq[StructField] = withOrigin(ctx) {
    ctx.colType().asScala.map(visitColType).toSeq
  }

  override def visitColType(ctx: ColTypeContext): StructField = withOrigin(ctx) {
    import ctx._

    val builder = new MetadataBuilder

    StructField(
      ctx.colName.getText,
      typedVisit[DataType](ctx.dataType),
      nullable = NOT == null,
      builder.build())
  }

  // Build the text of the CHECK constraint expression. The user-specified whitespace is in the
  // HIDDEN channel where we can't get to it, so we just paste together all the tokens with a single
  // space. This produces some strange spacing (e.g. `structCol . arr [ 0 ]`), but right now we
  // think that's preferable to the additional complexity involved in trying to produce cleaner
  // output.
  private def buildCheckConstraintText(tokens: Seq[ExprTokenContext]): String = {
    tokens.map(_.getText).mkString(" ")
  }

  private def extractRawText(exprContext: ParserRuleContext): String = {
    // Extract the raw expression which will be parsed later
    exprContext.getStart.getInputStream.getText(new Interval(
      exprContext.getStart.getStartIndex,
      exprContext.getStop.getStopIndex))
  }

  override def visitAddTableConstraint(
      ctx: AddTableConstraintContext): LogicalPlan = withOrigin(ctx) {
    val checkConstraint = ctx.constraint().asInstanceOf[CheckConstraintContext]

    AlterTableAddConstraint(
      UnresolvedTable(ctx.table.identifier.asScala.map(_.getText).toSeq,
        "ALTER TABLE ... ADD CONSTRAINT"),
      ctx.name.getText,
      buildCheckConstraintText(checkConstraint.exprToken().asScala.toSeq))
  }

  override def visitDropTableConstraint(
      ctx: DropTableConstraintContext): LogicalPlan = withOrigin(ctx) {
    AlterTableDropConstraint(
      UnresolvedTable(ctx.table.identifier.asScala.map(_.getText).toSeq,
        "ALTER TABLE ... DROP CONSTRAINT"),
      ctx.name.getText,
      ifExists = ctx.EXISTS != null)
  }

  /**
   * `ALTER TABLE ... ALTER (CHANGE) COLUMN ... SYNC IDENTITY` command.
   */
  override def visitAlterTableSyncIdentity(
      ctx: AlterTableSyncIdentityContext): LogicalPlan = withOrigin(ctx) {
    val verb = if (ctx.CHANGE != null) "CHANGE" else "ALTER"
    AlterColumnSyncIdentity(
      UnresolvedTable(ctx.table.identifier.asScala.map(_.getText).toSeq,
        s"ALTER TABLE ... $verb COLUMN"),
      UnresolvedFieldName(visitMultipartIdentifier(ctx.column))
    )
  }

  /**
   * A featureNameValue can either be String or an identifier. This function extracts
   * the featureNameValue based on whether its a string literal or an identifier.
   */
  override def visitFeatureNameValue(featureNameValue: FeatureNameValueContext): String = {
    if (featureNameValue.stringLit() != null) {
      string(visitStringLit(featureNameValue.stringLit()))
    } else {
      featureNameValue.getText
    }
  }

  /**
   * Parse an ALTER TABLE DROP FEATURE command.
   */
  override def visitAlterTableDropFeature(ctx: AlterTableDropFeatureContext): LogicalPlan = {
    val truncateHistory = ctx.TRUNCATE != null && ctx.HISTORY != null
    AlterTableDropFeature(
      UnresolvedTable(ctx.table.identifier.asScala.map(_.getText).toSeq,
        "ALTER TABLE ... DROP FEATURE"),
      visitFeatureNameValue(ctx.featureName),
      truncateHistory)
  }

  /**
   * Parse an ALTER TABLE CLUSTER BY command.
   */
  override def visitAlterTableClusterBy(ctx: AlterTableClusterByContext): LogicalPlan = {
    val table =
      UnresolvedTable(ctx.table.identifier.asScala.map(_.getText).toSeq,
      "ALTER TABLE ... CLUSTER BY")
    if (ctx.NONE() != null) {
      AlterTableClusterBy(table, None)
    } else {
      assert(ctx.clusterBySpec() != null)
      val columnNames =
        ctx.clusterBySpec().interleave.asScala
          .map(_.identifier.asScala.map(_.getText).toSeq)
          .map(_.asInstanceOf[Seq[String]]).toSeq
      AlterTableClusterBy(table, Some(ClusterBySpec(columnNames)))
    }
  }

  protected def typedVisit[T](ctx: ParseTree): T = {
    ctx.accept(this).asInstanceOf[T]
  }

  override def visitPrimitiveDataType(ctx: PrimitiveDataTypeContext): DataType = withOrigin(ctx) {
    val dataType = ctx.identifier.getText.toLowerCase(Locale.ROOT)
    (dataType, ctx.INTEGER_VALUE().asScala.toList) match {
      case ("boolean", Nil) => BooleanType
      case ("tinyint" | "byte", Nil) => ByteType
      case ("smallint" | "short", Nil) => ShortType
      case ("int" | "integer", Nil) => IntegerType
      case ("bigint" | "long", Nil) => LongType
      case ("float", Nil) => FloatType
      case ("double", Nil) => DoubleType
      case ("date", Nil) => DateType
      case ("timestamp", Nil) => TimestampType
      case ("string", Nil) => StringType
      case ("char", length :: Nil) => CharType(length.getText.toInt)
      case ("varchar", length :: Nil) => VarcharType(length.getText.toInt)
      case ("binary", Nil) => BinaryType
      case ("decimal", Nil) => DecimalType.USER_DEFAULT
      case ("decimal", precision :: Nil) => DecimalType(precision.getText.toInt, 0)
      case ("decimal", precision :: scale :: Nil) =>
        DecimalType(precision.getText.toInt, scale.getText.toInt)
      case ("interval", Nil) => CalendarIntervalType
      case (dt, params) =>
        val dtStr = if (params.nonEmpty) s"$dt(${params.mkString(",")})" else dt
        throw new DeltaParseException(
          ctx,
          "DELTA_PARSING_UNSUPPORTED_DATA_TYPE",
          Map("dataType" -> dtStr)
        )
    }
  }
}

// scalastyle:off line.size.limit
/**
 * Fork from `org.apache.spark.sql.catalyst.parser.UpperCaseCharStream`.
 *
 * @see https://github.com/apache/spark/blob/v2.4.4/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/parser/ParseDriver.scala#L157
 */
// scalastyle:on
class UpperCaseCharStream(wrapped: CodePointCharStream) extends CharStream {
  override def consume(): Unit = wrapped.consume
  override def getSourceName(): String = wrapped.getSourceName
  override def index(): Int = wrapped.index
  override def mark(): Int = wrapped.mark
  override def release(marker: Int): Unit = wrapped.release(marker)
  override def seek(where: Int): Unit = wrapped.seek(where)
  override def size(): Int = wrapped.size

  override def getText(interval: Interval): String = {
    // ANTLR 4.7's CodePointCharStream implementations have bugs when
    // getText() is called with an empty stream, or intervals where
    // the start > end. See
    // https://github.com/antlr/antlr4/commit/ac9f7530 for one fix
    // that is not yet in a released ANTLR artifact.
    if (size() > 0 && (interval.b - interval.a >= 0)) {
      wrapped.getText(interval)
    } else {
      ""
    }
  }

  override def LA(i: Int): Int = {
    val la = wrapped.LA(i)
    if (la == 0 || la == IntStream.EOF) la
    else Character.toUpperCase(la)
  }
}

// scalastyle:off line.size.limit
/**
 * Fork from `org.apache.spark.sql.catalyst.parser.PostProcessor`.
 *
 * @see https://github.com/apache/spark/blob/v2.4.4/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/parser/ParseDriver.scala#L248
 */
// scalastyle:on
case object PostProcessor extends DeltaSqlBaseBaseListener {

  /** Remove the back ticks from an Identifier. */
  override def exitQuotedIdentifier(ctx: QuotedIdentifierContext): Unit = {
    replaceTokenByIdentifier(ctx, 1) { token =>
      // Remove the double back ticks in the string.
      token.setText(token.getText.replace("``", "`"))
      token
    }
  }

  /** Treat non-reserved keywords as Identifiers. */
  override def exitNonReserved(ctx: NonReservedContext): Unit = {
    replaceTokenByIdentifier(ctx, 0)(identity)
  }

  private def replaceTokenByIdentifier(
    ctx: ParserRuleContext,
    stripMargins: Int)(
    f: CommonToken => CommonToken = identity): Unit = {
    val parent = ctx.getParent
    parent.removeLastChild()
    val token = ctx.getChild(0).getPayload.asInstanceOf[Token]
    val newToken = new CommonToken(
      new org.antlr.v4.runtime.misc.Pair(token.getTokenSource, token.getInputStream),
      DeltaSqlBaseParser.IDENTIFIER,
      token.getChannel,
      token.getStartIndex + stripMargins,
      token.getStopIndex - stripMargins)
    parent.addChild(new TerminalNodeImpl(f(newToken)))
  }
}
