/*
 * Copyright 2019 Databricks, Inc.
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

package io.delta.execution

import org.apache.spark.sql.delta.DeltaErrors
import org.apache.spark.sql.delta.commands.DeleteCommand
import io.delta.DeltaTable

import org.apache.spark.sql.catalyst.expressions.{Expression, SubqueryExpression}
import org.apache.spark.sql.catalyst.plans.logical._

/**
 * Interface to provide operations that can be performed on a Delta Table.
 *    Delete: delete data from table with optional condition.
 *    Update:
 *    MergeInto:
 */
trait DeltaTableOperations { self: DeltaTable =>
  protected def executeDelete(condition: Option[Expression]): Unit = {
    val sparkSession = self.toDF.sparkSession
    val delete = Delete(self.toDF.queryExecution.analyzed, condition)
    delete.condition match {
      case Some(cond) if SubqueryExpression.hasSubquery(cond) =>
        throw DeltaErrors.subqueryNotSupportedException("DELETE", cond)
      case _ =>
    }
    val qe = sparkSession.sessionState.executePlan(delete)
    val resolvedDelete = qe.analyzed.asInstanceOf[Delete]
    val deleteCommand = DeleteCommand(resolvedDelete)
    deleteCommand.run(sparkSession)
  }
}
