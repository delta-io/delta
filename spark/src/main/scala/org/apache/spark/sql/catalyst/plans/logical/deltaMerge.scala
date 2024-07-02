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

package org.apache.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.delta.{DeltaAnalysisException, DeltaIllegalArgumentException, DeltaUnsupportedOperationException}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.types.{DataType, StructType}

/**
 * A copy of Spark SQL Unevaluable for cross-version compatibility. In 3.0, implementers of
 * the original Unevaluable must explicitly override foldable to false; in 3.1 onwards, this
 * explicit override is invalid.
 */
trait DeltaUnevaluable extends Expression {
  final override def foldable: Boolean = false

  final override def eval(input: InternalRow = null): Any = {
    throw new DeltaUnsupportedOperationException(
      errorClass = "DELTA_CANNOT_EVALUATE_EXPRESSION",
      messageParameters = Array(s"$this")
    )
  }

  final override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    throw new DeltaUnsupportedOperationException(
      errorClass = "DELTA_CANNOT_GENERATE_CODE_FOR_EXPRESSION",
      messageParameters = Array(s"$this")
    )
}

/**
 * Represents an action in MERGE's UPDATE or INSERT clause where a target columns is assigned the
 * value of an expression
 *
 * @param targetColNameParts The name parts of the target column. This is a sequence to support
 *                           nested fields as targets.
 * @param expr Expression to generate the value of the target column.
 * @param targetColNameResolved Whether the targetColNameParts have undergone resolution and checks
 *                              for validity.
 */
case class DeltaMergeAction(
    targetColNameParts: Seq[String],
    expr: Expression,
    targetColNameResolved: Boolean = false)
  extends UnaryExpression with DeltaUnevaluable {
  override def child: Expression = expr
  override def dataType: DataType = expr.dataType
  override lazy val resolved: Boolean = {
    childrenResolved && checkInputDataTypes().isSuccess && targetColNameResolved
  }
  override def sql: String = s"$targetColString = ${expr.sql}"
  override def toString: String = s"$targetColString = $expr"
  private lazy val targetColString: String = targetColNameParts.mkString("`", "`.`", "`")

  override protected def withNewChildInternal(newChild: Expression): DeltaMergeAction =
    copy(expr = newChild)
}


/**
 * Trait that represents a WHEN clause in MERGE. See [[DeltaMergeInto]]. It extends [[Expression]]
 * so that Catalyst can find all the expressions in the clause implementations.
 */
sealed trait DeltaMergeIntoClause extends Expression with DeltaUnevaluable {
  /** Optional condition of the clause */
  def condition: Option[Expression]

  /**
   * Sequence of actions represented as expressions. Note that this can be only be either
   * UnresolvedStar, or MergeAction.
   */
  def actions: Seq[Expression]

  /**
   * Sequence of resolved actions represented as Aliases. Actions, once resolved, must
   * be Aliases and not any other NamedExpressions. So it should be safe to do this casting
   * as long as this is called after the clause has been resolved.
   */
  def resolvedActions: Seq[DeltaMergeAction] = {
    assert(actions.forall(_.resolved), "all actions have not been resolved yet")
    actions.map(_.asInstanceOf[DeltaMergeAction])
  }

  /**
   * String representation of the clause type: Update, Delete or Insert.
   */
  def clauseType: String

  override def toString: String = {
    val condStr = condition.map { c => s"condition: $c" }
    val actionStr = if (actions.isEmpty) None else {
      Some("actions: " + actions.mkString("[", ", ", "]"))
    }
    s"$clauseType " + Seq(condStr, actionStr).flatten.mkString("[", ", ", "]")
  }

  override def nullable: Boolean = false
  override def dataType: DataType = null
  override def children: Seq[Expression] = condition.toSeq ++ actions

  /** Verify whether the expressions in the actions are of the right type */
  protected[logical] def verifyActions(): Unit = actions.foreach {
    case _: UnresolvedStar =>
    case _: DeltaMergeAction =>
    case a => throw new DeltaIllegalArgumentException(
      errorClass = "DELTA_UNEXPECTED_ACTION_EXPRESSION",
      messageParameters = Array(s"$a"))
  }
}


object DeltaMergeIntoClause {
  /**
   * Convert the parsed columns names and expressions into action for MergeInto. Note:
   * - Size of column names and expressions must be the same.
   * - If the sizes are zeros and `emptySeqIsStar` is true, this function assumes
   *   that query had `*` as an action, and therefore generates a single action
   *   with `UnresolvedStar`. This will be expanded later during analysis.
   * - Otherwise, this will convert the names and expressions to MergeActions.
   */
  def toActions(
      colNames: Seq[UnresolvedAttribute],
      exprs: Seq[Expression],
      isEmptySeqEqualToStar: Boolean = true): Seq[Expression] = {
    assert(colNames.size == exprs.size)
    if (colNames.isEmpty && isEmptySeqEqualToStar) {
      Seq(UnresolvedStar(None))
    } else {
      (colNames, exprs).zipped.map { (col, expr) => DeltaMergeAction(col.nameParts, expr) }
    }
  }

  def toActions(assignments: Seq[Assignment]): Seq[Expression] = {
    if (assignments.isEmpty) {
      Seq[Expression](UnresolvedStar(None))
    } else {
      assignments.map {
        case Assignment(key: UnresolvedAttribute, expr) => DeltaMergeAction(key.nameParts, expr)
        case Assignment(key: Attribute, expr) => DeltaMergeAction(Seq(key.name), expr)
        case other =>
          throw new DeltaAnalysisException(
            errorClass = "DELTA_MERGE_UNEXPECTED_ASSIGNMENT_KEY",
            messageParameters = Array(s"${other.getClass}", s"$other"))
      }
    }
  }
}

/** Trait that represents WHEN MATCHED clause in MERGE. See [[DeltaMergeInto]]. */
sealed trait DeltaMergeIntoMatchedClause extends DeltaMergeIntoClause

/** Represents the clause WHEN MATCHED THEN UPDATE in MERGE. See [[DeltaMergeInto]]. */
case class DeltaMergeIntoMatchedUpdateClause(
    condition: Option[Expression],
    actions: Seq[Expression])
  extends DeltaMergeIntoMatchedClause {

  def this(cond: Option[Expression], cols: Seq[UnresolvedAttribute], exprs: Seq[Expression]) =
    this(cond, DeltaMergeIntoClause.toActions(cols, exprs))

  override def clauseType: String = "Update"

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): DeltaMergeIntoMatchedUpdateClause = {
    if (condition.isDefined) {
      copy(condition = Some(newChildren.head), actions = newChildren.tail)
    } else {
      copy(condition = None, actions = newChildren)
    }
  }
}

/** Represents the clause WHEN MATCHED THEN DELETE in MERGE. See [[DeltaMergeInto]]. */
case class DeltaMergeIntoMatchedDeleteClause(condition: Option[Expression])
    extends DeltaMergeIntoMatchedClause {
  def this(condition: Option[Expression], actions: Seq[DeltaMergeAction]) = this(condition)

  override def clauseType: String = "Delete"
  override def actions: Seq[Expression] = Seq.empty

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): DeltaMergeIntoMatchedDeleteClause =
    copy(condition = if (condition.isDefined) Some(newChildren.head) else None)
}

/** Trait that represents WHEN NOT MATCHED clause in MERGE. See [[DeltaMergeInto]]. */
sealed trait DeltaMergeIntoNotMatchedClause extends DeltaMergeIntoClause

/** Represents the clause WHEN NOT MATCHED THEN INSERT in MERGE. See [[DeltaMergeInto]]. */
case class DeltaMergeIntoNotMatchedInsertClause(
    condition: Option[Expression],
    actions: Seq[Expression])
  extends DeltaMergeIntoNotMatchedClause {

  def this(cond: Option[Expression], cols: Seq[UnresolvedAttribute], exprs: Seq[Expression]) =
    this(cond, DeltaMergeIntoClause.toActions(cols, exprs))

  override def clauseType: String = "Insert"

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): DeltaMergeIntoNotMatchedInsertClause =
    if (condition.isDefined) {
      copy(condition = Some(newChildren.head), actions = newChildren.tail)
    } else {
      copy(condition = None, actions = newChildren)
    }
}

/** Trait that represents WHEN NOT MATCHED BY SOURCE clause in MERGE. See [[DeltaMergeInto]]. */
sealed trait DeltaMergeIntoNotMatchedBySourceClause extends DeltaMergeIntoClause

/** Represents the clause WHEN NOT MATCHED BY SOURCE THEN UPDATE in MERGE. See
 * [[DeltaMergeInto]]. */
case class DeltaMergeIntoNotMatchedBySourceUpdateClause(
    condition: Option[Expression],
    actions: Seq[Expression])
  extends DeltaMergeIntoNotMatchedBySourceClause {

  def this(cond: Option[Expression], cols: Seq[UnresolvedAttribute], exprs: Seq[Expression]) =
    this(cond, DeltaMergeIntoClause.toActions(cols, exprs))

  override def clauseType: String = "Update"

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): DeltaMergeIntoNotMatchedBySourceUpdateClause = {
    if (condition.isDefined) {
      copy(condition = Some(newChildren.head), actions = newChildren.tail)
    } else {
      copy(condition = None, actions = newChildren)
    }
  }
}

/** Represents the clause WHEN NOT MATCHED BY SOURCE THEN DELETE in MERGE. See
 * [[DeltaMergeInto]]. */
case class DeltaMergeIntoNotMatchedBySourceDeleteClause(condition: Option[Expression])
  extends DeltaMergeIntoNotMatchedBySourceClause {
  def this(condition: Option[Expression], actions: Seq[DeltaMergeAction]) = this(condition)

  override def clauseType: String = "Delete"
  override def actions: Seq[Expression] = Seq.empty

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): DeltaMergeIntoNotMatchedBySourceDeleteClause =
    copy(condition = if (condition.isDefined) Some(newChildren.head) else None)
}

/**
 * Merges changes specified in the source plan into a target table, based on the given search
 * condition and the actions to perform when the condition is matched or not matched by the rows.
 *
 * The syntax of the MERGE statement is as follows.
 * {{{
 *    MERGE [WITH SCHEMA EVOLUTION] INTO <target_table_with_alias>
 *    USING <source_table_with_alias>
 *    ON <search_condition>
 *    [ WHEN MATCHED [ AND <condition> ] THEN <matched_action> ]
 *    [ WHEN MATCHED [ AND <condition> ] THEN <matched_action> ]
 *    ...
 *    [ WHEN NOT MATCHED [BY TARGET] [ AND <condition> ] THEN <not_matched_action> ]
 *    [ WHEN NOT MATCHED [BY TARGET] [ AND <condition> ] THEN <not_matched_action> ]
 *    ...
 *    [ WHEN NOT MATCHED BY SOURCE [ AND <condition> ] THEN <not_matched_by_source_action> ]
 *    [ WHEN NOT MATCHED BY SOURCE [ AND <condition> ] THEN <not_matched_by_source_action> ]
 *    ...
 *
 *
 *    where
 *    <matched_action> =
 *      DELETE |
 *      UPDATE SET column1 = value1 [, column2 = value2 ...] |
 *      UPDATE SET * [EXCEPT (column1, ...)]
 *    <not_matched_action> = INSERT (column1 [, column2 ...]) VALUES (expr1 [, expr2 ...])
 *    <not_matched_by_source_action> =
 *      DELETE |
 *      UPDATE SET column1 = value1 [, column2 = value2 ...]
 * }}}
 *
 * - There can be any number of WHEN clauses.
 * - WHEN MATCHED clauses:
 *    - Each WHEN MATCHED clause can have an optional condition. However, if there are multiple
 * WHEN MATCHED clauses, only the last can omit the condition.
 *    - WHEN MATCHED clauses are dependent on their ordering; that is, the first clause that
 * satisfies the clause's condition has its corresponding action executed.
 * - WHEN NOT MATCHED clause:
 *    - Can only have the INSERT action. If present, they must follow the last WHEN MATCHED clause.
 *    - Each WHEN NOT MATCHED clause can have an optional condition. However, if there are multiple
 * clauses, only the last can omit the condition.
 *    - WHEN NOT MATCHED clauses are dependent on their ordering; that is, the first clause that
 * satisfies the clause's condition has its corresponding action executed.
 * - WHEN NOT MATCHED BY SOURCE clauses:
 *    - Each WHEN NOT MATCHED BY SOURCE clause can have an optional condition. However, if there are
 * multiple WHEN NOT MATCHED BY SOURCE clauses, only the last can omit the condition.
 *    - WHEN NOT MATCHED BY SOURCE clauses are dependent on their ordering; that is, the first
 * clause that satisfies the clause's condition has its corresponding action executed.
 */
case class DeltaMergeInto(
    target: LogicalPlan,
    source: LogicalPlan,
    condition: Expression,
    matchedClauses: Seq[DeltaMergeIntoMatchedClause],
    notMatchedClauses: Seq[DeltaMergeIntoNotMatchedClause],
    notMatchedBySourceClauses: Seq[DeltaMergeIntoNotMatchedBySourceClause],
    withSchemaEvolution: Boolean,
    finalSchema: Option[StructType])
  extends Command with SupportsSubquery {

  (matchedClauses ++ notMatchedClauses ++ notMatchedBySourceClauses).foreach(_.verifyActions())

  // TODO: extend BinaryCommand once the new Spark version is released
  override def children: Seq[LogicalPlan] = Seq(target, source)
  override def output: Seq[Attribute] = Seq.empty
  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[LogicalPlan]): DeltaMergeInto =
    copy(target = newChildren(0), source = newChildren(1))
}

object DeltaMergeInto {
  def apply(
      target: LogicalPlan,
      source: LogicalPlan,
      condition: Expression,
      whenClauses: Seq[DeltaMergeIntoClause],
      withSchemaEvolution: Boolean): DeltaMergeInto = {
    val notMatchedClauses = whenClauses.collect { case x: DeltaMergeIntoNotMatchedClause => x }
    val matchedClauses = whenClauses.collect { case x: DeltaMergeIntoMatchedClause => x }
    val notMatchedBySourceClauses =
      whenClauses.collect { case x: DeltaMergeIntoNotMatchedBySourceClause => x }

    // grammar enforcement goes here.
    if (whenClauses.isEmpty) {
      throw new DeltaAnalysisException(
        errorClass = "DELTA_MERGE_MISSING_WHEN",
        messageParameters = Array.empty
      )
    }

    // Check that only the last MATCHED clause omits the condition.
    if (matchedClauses.length > 1 && !matchedClauses.init.forall(_.condition.nonEmpty)) {
      throw new DeltaAnalysisException(
        errorClass = "NON_LAST_MATCHED_CLAUSE_OMIT_CONDITION",
        messageParameters = Array.empty)
    }

    // Check that only the last NOT MATCHED clause omits the condition.
    if (notMatchedClauses.length > 1 && !notMatchedClauses.init.forall(_.condition.nonEmpty)) {
      throw new DeltaAnalysisException(
        errorClass = "NON_LAST_NOT_MATCHED_BY_TARGET_CLAUSE_OMIT_CONDITION",
        messageParameters = Array.empty)
    }

    // Check that only the last NOT MATCHED BY SOURCE clause omits the condition.
    if (notMatchedBySourceClauses.length > 1 &&
      !notMatchedBySourceClauses.init.forall(_.condition.nonEmpty)) {
      throw new DeltaAnalysisException(
        errorClass = "NON_LAST_NOT_MATCHED_BY_SOURCE_CLAUSE_OMIT_CONDITION",
        messageParameters = Array.empty)
    }

    DeltaMergeInto(
      target,
      source,
      condition,
      matchedClauses,
      notMatchedClauses,
      notMatchedBySourceClauses,
      withSchemaEvolution,
      finalSchema = Some(target.schema))
  }
}
