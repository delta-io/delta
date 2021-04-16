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
 * Copyright (2020) The Delta Lake Project Authors.
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

import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, Cast, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project, View}
import org.apache.spark.sql.internal.SQLConf

object DeltaViewHelper {
  /**
   * Eliminate the view node from `plan`. Spark 3.1.1 introduces a View node in the plan when a
   * view is a SQL view. We need to eliminate it manually in Spark 3.1.1 and above. This View node
   * doesn't exist in Spark 3.0.x and below.
   */
  def stripTempView(plan: LogicalPlan, conf: SQLConf): LogicalPlan = plan transformUp {
    case v @ View(desc, true, output, child) if child.resolved && !v.sameOutput(child) =>
      val newOutput = makeNewOutput(desc, output, child, conf)
      Project(newOutput, child)

    case View(_, true, _, child) =>
      child
  }

  private def makeNewOutput(
      desc: CatalogTable,
      output: Seq[Attribute],
      child: LogicalPlan,
      conf: SQLConf): Seq[NamedExpression] = {
    val resolver = conf.resolver
    val queryColumnNames = desc.viewQueryColumnNames
    val queryOutput = if (queryColumnNames.nonEmpty) {
      // Find the attribute that has the expected attribute name from an attribute list, the names
      // are compared using conf.resolver.
      // `CheckAnalysis` already guarantees the expected attribute can be found for sure.
      desc.viewQueryColumnNames.map { colName =>
        child.output.find(attr => resolver(attr.name, colName)).get
      }
    } else {
      // For view created before Spark 2.2.0, the view text is already fully qualified, the plan
      // output is the same with the view output.
      child.output
    }
    // Map the attributes in the query output to the attributes in the view output by index.
    output.zip(queryOutput).map {
      case (attr, originAttr) if !attr.semanticEquals(originAttr) =>
        // `CheckAnalysis` already guarantees that the cast is a up-cast for sure.
        Alias(Cast(originAttr, attr.dataType), attr.name)(exprId = attr.exprId,
          qualifier = attr.qualifier, explicitMetadata = Some(attr.metadata))
      case (_, originAttr) => originAttr
    }
  }
}
