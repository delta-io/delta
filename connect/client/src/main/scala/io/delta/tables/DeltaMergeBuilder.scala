/*
 * Copyright (2024) The Delta Lake Project Authors.
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

package io.delta.tables

import scala.collection.JavaConverters._
import scala.collection.Map

import io.delta.connect.proto

import org.apache.spark.annotation.Unstable
import org.apache.spark.sql.{functions, Column, DataFrame}
import org.apache.spark.sql.functions.expr

/**
 * Builder to specify how to merge data from source DataFrame into the target Delta table.
 * You can specify any number of `whenMatched` and `whenNotMatched` clauses.
 * Here are the constraints on these clauses.
 *
 *   - `whenMatched` clauses:
 *
 *     - The condition in a `whenMatched` clause is optional. However, if there are multiple
 *       `whenMatched` clauses, then only the last one may omit the condition.
 *
 *     - When there are more than one `whenMatched` clauses and there are conditions (or the lack
 *       of) such that a row satisfies multiple clauses, then the action for the first clause
 *       satisfied is executed. In other words, the order of the `whenMatched` clauses matters.
 *
 *     - If none of the `whenMatched` clauses match a source-target row pair that satisfy
 *       the merge condition, then the target rows will not be updated or deleted.
 *
 *     - If you want to update all the columns of the target Delta table with the
 *       corresponding column of the source DataFrame, then you can use the
 *       `whenMatched(...).updateAll()`. This is equivalent to
 *       <pre>
 *         whenMatched(...).updateExpr(Map(
 *           ("col1", "source.col1"),
 *           ("col2", "source.col2"),
 *           ...))
 *       </pre>
 *
 *   - `whenNotMatched` clauses:
 *
 *     - The condition in a `whenNotMatched` clause is optional. However, if there are
 *       multiple `whenNotMatched` clauses, then only the last one may omit the condition.
 *
 *     - When there are more than one `whenNotMatched` clauses and there are conditions (or the
 *       lack of) such that a row satisfies multiple clauses, then the action for the first clause
 *       satisfied is executed. In other words, the order of the `whenNotMatched` clauses matters.
 *
 *     - If no `whenNotMatched` clause is present or if it is present but the non-matching source
 *       row does not satisfy the condition, then the source row is not inserted.
 *
 *     - If you want to insert all the columns of the target Delta table with the
 *       corresponding column of the source DataFrame, then you can use
 *       `whenNotMatched(...).insertAll()`. This is equivalent to
 *       <pre>
 *         whenNotMatched(...).insertExpr(Map(
 *           ("col1", "source.col1"),
 *           ("col2", "source.col2"),
 *           ...))
 *       </pre>
 *
 *   - `whenNotMatchedBySource` clauses:
 *
 *     - The condition in a `whenNotMatchedBySource` clause is optional. However, if there are
 *       multiple `whenNotMatchedBySource` clauses, then only the last one may omit the condition.
 *
 *     - When there are more than one `whenNotMatchedBySource` clauses and there are conditions (or
 *       the lack of) such that a row satisfies multiple clauses, then the action for the first
 *       clause satisfied is executed. In other words, the order of the `whenNotMatchedBySource`
 *       clauses matters.
 *
 *     - If no `whenNotMatchedBySource` clause is present or if it is present but the
 *       non-matching target row does not satisfy any of the `whenNotMatchedBySource` clause
 *       condition, then the target row will not be updated or deleted.
 *
 *
 * Scala example to update a key-value Delta table with new key-values from a source DataFrame:
 * {{{
 *    deltaTable
 *     .as("target")
 *     .merge(
 *       source.as("source"),
 *       "target.key = source.key")
 *     .whenMatched()
 *     .updateExpr(Map(
 *       "value" -> "source.value"))
 *     .whenNotMatched()
 *     .insertExpr(Map(
 *       "key" -> "source.key",
 *       "value" -> "source.value"))
 *     .whenNotMatchedBySource()
 *     .updateExpr(Map(
 *       "value" -> "target.value + 1"))
 *     .execute()
 * }}}
 *
 * Java example to update a key-value Delta table with new key-values from a source DataFrame:
 * {{{
 *    deltaTable
 *     .as("target")
 *     .merge(
 *       source.as("source"),
 *       "target.key = source.key")
 *     .whenMatched()
 *     .updateExpr(
 *        new HashMap<String, String>() {{
 *          put("value", "source.value");
 *        }})
 *     .whenNotMatched()
 *     .insertExpr(
 *        new HashMap<String, String>() {{
 *         put("key", "source.key");
 *         put("value", "source.value");
 *       }})
 *     .whenNotMatchedBySource()
 *     .updateExpr(
 *        new HashMap<String, String>() {{
 *         put("value", "target.value + 1");
 *       }})
 *     .execute();
 * }}}
 *
 * @since 2.5.0
 */
class DeltaMergeBuilder private(
    private val targetTable: DeltaTable,
    private val source: DataFrame,
    private val onCondition: Column,
    private val whenMatchedClauses: Seq[proto.MergeIntoTable.Action],
    private val whenNotMatchedClauses: Seq[proto.MergeIntoTable.Action],
    private val whenNotMatchedBySourceClauses: Seq[proto.MergeIntoTable.Action]) {

  /**
   * Build the actions to perform when the merge condition was matched.  This returns
   * [[DeltaMergeMatchedActionBuilder]] object which can be used to specify how
   * to update or delete the matched target table row with the source row.
   *
   * @since 2.5.0
   */
  def whenMatched(): DeltaMergeMatchedActionBuilder = {
    DeltaMergeMatchedActionBuilder(this, None)
  }

  /**
   * Build the actions to perform when the merge condition was matched and
   * the given `condition` is true. This returns [[DeltaMergeMatchedActionBuilder]] object
   * which can be used to specify how to update or delete the matched target table row with the
   * source row.
   *
   * @param condition boolean expression as a SQL formatted string
   * @since 2.5.0
   */
  def whenMatched(condition: String): DeltaMergeMatchedActionBuilder = {
    whenMatched(expr(condition))
  }

  /**
   * Build the actions to perform when the merge condition was matched and
   * the given `condition` is true. This returns a [[DeltaMergeMatchedActionBuilder]] object
   * which can be used to specify how to update or delete the matched target table row with the
   * source row.
   *
   * @param condition boolean expression as a Column object
   * @since 2.5.0
   */
  def whenMatched(condition: Column): DeltaMergeMatchedActionBuilder = {
    DeltaMergeMatchedActionBuilder(this, Some(condition))
  }

  /**
   * Build the action to perform when the merge condition was not matched. This returns
   * [[DeltaMergeNotMatchedActionBuilder]] object which can be used to specify how
   * to insert the new sourced row into the target table.
   * @since 2.5.0
   */
  def whenNotMatched(): DeltaMergeNotMatchedActionBuilder = {
    DeltaMergeNotMatchedActionBuilder(this, None)
  }

  /**
   * Build the actions to perform when the merge condition was not matched and
   * the given `condition` is true. This returns [[DeltaMergeMatchedActionBuilder]] object
   * which can be used to specify how to insert the new sourced row into the target table.
   *
   * @param condition boolean expression as a SQL formatted string
   * @since 2.5.0
   */
  def whenNotMatched(condition: String): DeltaMergeNotMatchedActionBuilder = {
    whenNotMatched(expr(condition))
  }

  /**
   * Build the actions to perform when the merge condition was not matched and
   * the given `condition` is true. This returns [[DeltaMergeMatchedActionBuilder]] object
   * which can be used to specify how to insert the new sourced row into the target table.
   *
   * @param condition boolean expression as a Column object
   * @since 2.5.0
   */
  def whenNotMatched(condition: Column): DeltaMergeNotMatchedActionBuilder = {
    DeltaMergeNotMatchedActionBuilder(this, Some(condition))
  }

  /**
   * Build the actions to perform when the merge condition was not matched by the source. This
   * returns [[DeltaMergeNotMatchedBySourceActionBuilder]] object which can be used to specify how
   * to update or delete the target table row.
   * @since 2.3.0
   */
  def whenNotMatchedBySource(): DeltaMergeNotMatchedBySourceActionBuilder = {
    DeltaMergeNotMatchedBySourceActionBuilder(this, None)
  }

  /**
   * Build the actions to perform when the merge condition was not matched by the source and the
   * given `condition` is true. This returns [[DeltaMergeNotMatchedBySourceActionBuilder]] object
   * which can be used to specify how to update or delete the target table row.
   *
   * @param condition boolean expression as a SQL formatted string
   * @since 2.3.0
   */
  def whenNotMatchedBySource(condition: String): DeltaMergeNotMatchedBySourceActionBuilder = {
    whenNotMatchedBySource(expr(condition))
  }

  /**
   * Build the actions to perform when the merge condition was not matched by the source and the
   * given `condition` is true. This returns [[DeltaMergeNotMatchedBySourceActionBuilder]] object
   * which can be used to specify how to update or delete the target table row .
   *
   * @param condition boolean expression as a Column object
   * @since 2.3.0
   */
  def whenNotMatchedBySource(condition: Column): DeltaMergeNotMatchedBySourceActionBuilder = {
    DeltaMergeNotMatchedBySourceActionBuilder(this, Some(condition))
  }

  /**
   * Execute the merge operation based on the built matched and not matched actions.
   *
   * @since 2.5.0
   */
  def execute(): Unit = {
    val sparkSession = targetTable.toDF.sparkSession
    val merge = proto.MergeIntoTable
      .newBuilder()
      .setTarget(targetTable.toDF.plan.getRoot)
      .setSource(source.plan.getRoot)
      .setCondition(onCondition.expr)
      .addAllMatchedActions(whenMatchedClauses.asJava)
      .addAllNotMatchedActions(whenNotMatchedClauses.asJava)
      .addAllNotMatchedBySourceActions(whenNotMatchedBySourceClauses.asJava)
    val relation = proto.DeltaRelation.newBuilder().setMergeIntoTable(merge).build()
    val extension = com.google.protobuf.Any.pack(relation)
    sparkSession.newDataFrame(extension).collect()
  }

  /**
   * :: Unstable ::
   *
   * Private method for internal usage only. Do not call this directly.
   */
  @Unstable
  private[delta] def withWhenMatchedClause(
      clause: proto.MergeIntoTable.Action): DeltaMergeBuilder = {
    new DeltaMergeBuilder(
      this.targetTable,
      this.source,
      this.onCondition,
      this.whenMatchedClauses :+ clause,
      this.whenNotMatchedClauses,
      this.whenNotMatchedBySourceClauses)
  }

  /**
   * :: Unstable ::
   *
   * Private method for internal usage only. Do not call this directly.
   */
  @Unstable
  private[delta] def withWhenNotMatchedClause(
      clause: proto.MergeIntoTable.Action): DeltaMergeBuilder = {
    new DeltaMergeBuilder(
      this.targetTable,
      this.source,
      this.onCondition,
      this.whenMatchedClauses,
      this.whenNotMatchedClauses :+ clause,
      this.whenNotMatchedBySourceClauses)
  }

  /**
   * :: Unstable ::
   *
   * Private method for internal usage only. Do not call this directly.
   */
  @Unstable
  private[delta] def withWhenNotMatchedBySourceClause(
      clause: proto.MergeIntoTable.Action): DeltaMergeBuilder = {
    new DeltaMergeBuilder(
      this.targetTable,
      this.source,
      this.onCondition,
      this.whenMatchedClauses,
      this.whenNotMatchedClauses,
      this.whenNotMatchedBySourceClauses :+ clause)
  }
}

object DeltaMergeBuilder {
  /**
   * :: Unstable ::
   *
   * Private method for internal usage only. Do not call this directly.
   */
  @Unstable
  private[delta] def apply(
      targetTable: DeltaTable, source: DataFrame, onCondition: Column): DeltaMergeBuilder = {
    new DeltaMergeBuilder(targetTable, source, onCondition, Nil, Nil, Nil)
  }
}

/**
 * Builder class to specify the actions to perform when a target table row has matched a
 * source row based on the given merge condition and optional match condition.
 *
 * See [[DeltaMergeBuilder]] for more information.
 *
 * @since 2.5.0
 */
class DeltaMergeMatchedActionBuilder private(
    private val mergeBuilder: DeltaMergeBuilder,
    private val matchCondition: Option[Column]) {

  /**
   * Update the matched table rows based on the rules defined by `set`.
   *
   * @param set rules to update a row as a Scala map between target column names and
   *            corresponding update expressions as Column objects.
   * @since 2.5.0
   */
  def update(set: Map[String, Column]): DeltaMergeBuilder = {
    addUpdateClause(set)
  }

  /**
   * Update the matched table rows based on the rules defined by `set`.
   *
   * @param set rules to update a row as a Scala map between target column names and
   *            corresponding update expressions as SQL formatted strings.
   * @since 2.5.0
   */
  def updateExpr(set: Map[String, String]): DeltaMergeBuilder = {
    addUpdateClause(toStrColumnMap(set))
  }

  /**
   * Update a matched table row based on the rules defined by `set`.
   *
   * @param set rules to update a row as a Java map between target column names and
   *            corresponding expressions as Column objects.
   * @since 2.5.0
   */
  def update(set: java.util.Map[String, Column]): DeltaMergeBuilder = {
    addUpdateClause(set.asScala.toMap)
  }

  /**
   * Update a matched table row based on the rules defined by `set`.
   *
   * @param set rules to update a row as a Java map between target column names and
   *            corresponding expressions as SQL formatted strings.
   * @since 2.5.0
   */
  def updateExpr(set: java.util.Map[String, String]): DeltaMergeBuilder = {
    addUpdateClause(toStrColumnMap(set.asScala.toMap))
  }

  /**
   * Update all the columns of the matched table row with the values of the
   * corresponding columns in the source row.
   *
   * @since 2.5.0
   */
  def updateAll(): DeltaMergeBuilder = {
    val clause = proto.MergeIntoTable.Action
      .newBuilder()
      .setUpdateStarAction(proto.MergeIntoTable.Action.UpdateStarAction.newBuilder())
    matchCondition.foreach(c => clause.setCondition(c.expr))
    mergeBuilder.withWhenMatchedClause(clause.build())
  }

  /**
   * Delete a matched row from the table.
   *
   * @since 2.5.0
   */
  def delete(): DeltaMergeBuilder = {
    val clause = proto.MergeIntoTable.Action
      .newBuilder()
      .setDeleteAction(proto.MergeIntoTable.Action.DeleteAction.newBuilder())
    matchCondition.foreach(c => clause.setCondition(c.expr))
    mergeBuilder.withWhenMatchedClause(clause.build())
  }

  private def addUpdateClause(set: Map[String, Column]): DeltaMergeBuilder = {
    if (set.isEmpty && matchCondition.isEmpty) {
      // This is a catch all clause that doesn't update anything: we can ignore it.
      mergeBuilder
    } else {
      val assignments = set.map { case (field, value) =>
        proto.Assignment.newBuilder().setField(expr(field).expr).setValue(value.expr).build()
      }
      val action = proto.MergeIntoTable.Action.UpdateAction
        .newBuilder()
        .addAllAssignments(assignments.asJava)
      val clause = proto.MergeIntoTable.Action
        .newBuilder()
        .setUpdateAction(action)
      matchCondition.foreach(c => clause.setCondition(c.expr))
      mergeBuilder.withWhenMatchedClause(clause.build())
    }
  }

  private def toStrColumnMap(map: Map[String, String]): Map[String, Column] =
    map.mapValues(functions.expr).toMap
}

object DeltaMergeMatchedActionBuilder {
  /**
   * :: Unstable ::
   *
   * Private method for internal usage only. Do not call this directly.
   */
  @Unstable
  private[delta] def apply(
      mergeBuilder: DeltaMergeBuilder,
      matchCondition: Option[Column]): DeltaMergeMatchedActionBuilder = {
    new DeltaMergeMatchedActionBuilder(mergeBuilder, matchCondition)
  }
}


/**
 * Builder class to specify the actions to perform when a source row has not matched any target
 * Delta table row based on the merge condition, but has matched the additional condition
 * if specified.
 *
 * See [[DeltaMergeBuilder]] for more information.
 *
 * @since 2.5.0
 */
class DeltaMergeNotMatchedActionBuilder private(
    private val mergeBuilder: DeltaMergeBuilder,
    private val notMatchCondition: Option[Column]) {

  /**
   * Insert a new row to the target table based on the rules defined by `values`.
   *
   * @param values rules to insert a row as a Scala map between target column names and
   *               corresponding expressions as Column objects.
   * @since 2.5.0
   */
  def insert(values: Map[String, Column]): DeltaMergeBuilder = {
    addInsertClause(values)
  }

  /**
   * Insert a new row to the target table based on the rules defined by `values`.
   *
   * @param values rules to insert a row as a Scala map between target column names and
   *               corresponding expressions as SQL formatted strings.
   * @since 2.5.0
   */
  def insertExpr(values: Map[String, String]): DeltaMergeBuilder = {
    addInsertClause(toStrColumnMap(values))
  }

  /**
   * Insert a new row to the target table based on the rules defined by `values`.
   *
   * @param values rules to insert a row as a Java map between target column names and
   *               corresponding expressions as Column objects.
   * @since 2.5.0
   */
  def insert(values: java.util.Map[String, Column]): DeltaMergeBuilder = {
    addInsertClause(values.asScala)
  }

  /**
   * Insert a new row to the target table based on the rules defined by `values`.
   *
   * @param values rules to insert a row as a Java map between target column names and
   *               corresponding expressions as SQL formatted strings.
   *
   * @since 2.5.0
   */
  def insertExpr(values: java.util.Map[String, String]): DeltaMergeBuilder = {
    addInsertClause(toStrColumnMap(values.asScala))
  }

  /**
   * Insert a new target Delta table row by assigning the target columns to the values of the
   * corresponding columns in the source row.
   * @since 2.5.0
   */
  def insertAll(): DeltaMergeBuilder = {
    val clause = proto.MergeIntoTable.Action
      .newBuilder()
      .setInsertStarAction(proto.MergeIntoTable.Action.InsertStarAction.newBuilder())
    notMatchCondition.foreach(c => clause.setCondition(c.expr))
    mergeBuilder.withWhenNotMatchedClause(clause.build())
  }

  private def addInsertClause(setValues: Map[String, Column]): DeltaMergeBuilder = {
    val assignments = setValues.map { case (field, value) =>
      proto.Assignment.newBuilder().setField(expr(field).expr).setValue(value.expr).build()
    }
    val action = proto.MergeIntoTable.Action.InsertAction
      .newBuilder()
      .addAllAssignments(assignments.asJava)
    val clause = proto.MergeIntoTable.Action
      .newBuilder()
      .setInsertAction(action)
    notMatchCondition.foreach(c => clause.setCondition(c.expr))
    mergeBuilder.withWhenNotMatchedClause(clause.build())
  }

  private def toStrColumnMap(map: Map[String, String]): Map[String, Column] =
    map.mapValues(functions.expr).toMap
}

object DeltaMergeNotMatchedActionBuilder {
  /**
   * :: Unstable ::
   *
   * Private method for internal usage only. Do not call this directly.
   */
  @Unstable
  private[delta] def apply(
      mergeBuilder: DeltaMergeBuilder,
      notMatchCondition: Option[Column]): DeltaMergeNotMatchedActionBuilder = {
    new DeltaMergeNotMatchedActionBuilder(mergeBuilder, notMatchCondition)
  }
}

/**
 * Builder class to specify the actions to perform when a target table row has no match in the
 * source table based on the given merge condition and optional match condition.
 *
 * See [[DeltaMergeBuilder]] for more information.
 *
 * @since 2.5.0
 */
class DeltaMergeNotMatchedBySourceActionBuilder private(
    private val mergeBuilder: DeltaMergeBuilder,
    private val notMatchBySourceCondition: Option[Column]) {

  /**
   * Update an unmatched target table row based on the rules defined by `set`.
   *
   * @param set rules to update a row as a Scala map between target column names and
   *            corresponding update expressions as Column objects.
   * @since 2.5.0
   */
  def update(set: Map[String, Column]): DeltaMergeBuilder = {
    addUpdateClause(set)
  }

  /**
   * Update an unmatched target table row based on the rules defined by `set`.
   *
   * @param set rules to update a row as a Scala map between target column names and
   *            corresponding update expressions as SQL formatted strings.
   * @since 2.3.0
   */
  def updateExpr(set: Map[String, String]): DeltaMergeBuilder = {
    addUpdateClause(toStrColumnMap(set))
  }

  /**
   * Update an unmatched target table row based on the rules defined by `set`.
   *
   * @param set rules to update a row as a Java map between target column names and
   *            corresponding expressions as Column objects.
   * @since 2.5.0
   */
  def update(set: java.util.Map[String, Column]): DeltaMergeBuilder = {
    addUpdateClause(set.asScala)
  }

  /**
   * Update an unmatched target table row based on the rules defined by `set`.
   *
   * @param set rules to update a row as a Java map between target column names and
   *            corresponding expressions as SQL formatted strings.
   * @since 2.5.0
   */
  def updateExpr(set: java.util.Map[String, String]): DeltaMergeBuilder = {
    addUpdateClause(toStrColumnMap(set.asScala))
  }

  /**
   * Delete an unmatched row from the target table.
   * @since 2.5.0
   */
  def delete(): DeltaMergeBuilder = {
    val clause = proto.MergeIntoTable.Action
      .newBuilder()
      .setDeleteAction(proto.MergeIntoTable.Action.DeleteAction.newBuilder())
    notMatchBySourceCondition.foreach(c => clause.setCondition(c.expr))
    mergeBuilder.withWhenNotMatchedBySourceClause(clause.build())
  }

  private def addUpdateClause(set: Map[String, Column]): DeltaMergeBuilder = {
    if (set.isEmpty && notMatchBySourceCondition.isEmpty) {
      // This is a catch all clause that doesn't update anything: we can ignore it.
      mergeBuilder
    } else {
      val assignments = set.map { case (field, value) =>
        proto.Assignment.newBuilder().setField(expr(field).expr).setValue(value.expr).build()
      }
      val action = proto.MergeIntoTable.Action.UpdateAction
        .newBuilder()
        .addAllAssignments(assignments.asJava)
      val clause = proto.MergeIntoTable.Action
        .newBuilder()
        .setUpdateAction(action)
      notMatchBySourceCondition.foreach(c => clause.setCondition(c.expr))
      mergeBuilder.withWhenNotMatchedBySourceClause(clause.build())
    }
  }

  private def toStrColumnMap(map: Map[String, String]): Map[String, Column] =
    map.mapValues(functions.expr).toMap
}

object DeltaMergeNotMatchedBySourceActionBuilder {
  /**
   * :: Unstable ::
   *
   * Private method for internal usage only. Do not call this directly.
   */
  @Unstable
  private[delta] def apply(
      mergeBuilder: DeltaMergeBuilder,
      notMatchBySourceCondition: Option[Column]): DeltaMergeNotMatchedBySourceActionBuilder = {
    new DeltaMergeNotMatchedBySourceActionBuilder(mergeBuilder, notMatchBySourceCondition)
  }
}
