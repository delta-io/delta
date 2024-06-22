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

import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, Cast, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project, SubqueryAlias, View}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.internal.SQLConf

object DeltaViewHelper {
  def stripTempViewForMerge(plan: LogicalPlan, conf: SQLConf): LogicalPlan = {
    // Check that the two expression lists have the same names and types in the same order, and
    // are either attributes or direct casts of attributes.
    def attributesMatch(left: Seq[NamedExpression], right: Seq[NamedExpression]): Boolean = {
      if (left.length != right.length) return false

      val allowedExprs = (left ++ right).forall {
        case _: Attribute => true
        case Alias(Cast(_: Attribute, dataType, timeZone, _), name) => true
        case _ => false
      }

      val exprsMatch = left.zip(right).forall {
        case (a, b) => a.dataType == b.dataType && conf.resolver(a.name, b.name)
      }

      allowedExprs && exprsMatch
    }


    // We have to do a pretty complicated transformation here to support using two specific things
    // which are not a Delta table as the target of Delta DML commands:
    // A view defined as `SELECT * FROM underlying_tbl`
    // A view defined as `SELECT * FROM underlying_tbl as alias`
    // This requires stripping their intermediate nodes and pulling out just the scan, because
    // some of our internal attribute fiddling requires the target plan to have the same attribute
    // IDs as the underlying scan.
    object ViewPlan {
      def unapply(
          plan: LogicalPlan): Option[(CatalogTable, Seq[NamedExpression], LogicalRelation)] = {
        // A `SELECT * from underlying_table` view will have:
        // * A View node marking it as a view.
        // * An outer Project explicitly casting the scanned types to the types defined in the
        //   metastore for the view. We don't need this cast for Delta DML commands and it will
        //   end up being eliminated.
        // * An inner no-op project.
        // * A SubqueryAlias explicitly aliasing the scan to its own name (plus another if there's
        //   a user specified alias.
        // * The actual scan of the Delta table.
        // We check for these Projects by ensuring that the name lists are an exact match, and
        // produce a scan with the outer list's attribute IDs aliased to the view's name.
        plan match {
          case View(desc, true, // isTempView
          Project(outerList,
          Project(innerList,
          SubqueryAlias(innerAlias, scan: LogicalRelation))))
            if attributesMatch(outerList, innerList) && attributesMatch(outerList, scan.output) =>
            Some(desc, outerList, scan)
          case View(desc, true, // isTempView
          Project(outerList,
          Project(innerList,
          SubqueryAlias(innerAlias, SubqueryAlias(subalias, scan: LogicalRelation)))))
            if attributesMatch(outerList, innerList) && attributesMatch(outerList, scan.output) =>
            Some(desc, outerList, scan)
          case _ => None
        }
      }
    }

    plan.transformUp {
      case ViewPlan(desc, outerList, scan) =>
        val newOutput = scan.output.map { oldAttr =>
          val newId = outerList.collectFirst {
            case newAttr if conf.resolver(oldAttr.qualifiedName, newAttr.qualifiedName) =>
              newAttr.exprId
          }.getOrElse {
            throw DeltaErrors.noNewAttributeId(oldAttr)
          }
          oldAttr.withExprId(newId)
        }
        SubqueryAlias(desc.qualifiedName, scan.copy(output = newOutput))

      case v: View if v.isTempView =>
        v.child
    }
  }

  def stripTempView(plan: LogicalPlan, conf: SQLConf): LogicalPlan = {
    plan.transformUp {
      case v: View if v.isTempView => v.child
    }
  }
}
