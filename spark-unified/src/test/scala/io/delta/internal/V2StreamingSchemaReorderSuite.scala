/*
 * Copyright (2026) The Delta Lake Project Authors.
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

package io.delta.internal

import java.util.{HashMap => JHashMap}

import io.delta.spark.internal.v2.catalog.SparkTable
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.catalyst.streaming.StreamingRelationV2
import org.apache.spark.sql.catalyst.types.DataTypeUtils.toAttributes
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.delta.streaming.V2StreamingSchemaReorder
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class V2StreamingSchemaReorderSuite extends DeltaSQLCommandTest {

  private def buildPlan(
      tablePath: String,
      ddl: String,
      partitionedBy: String): StreamingRelationV2 = {
    spark.sql(s"CREATE TABLE delta.`$tablePath` ($ddl) USING delta PARTITIONED BY ($partitionedBy)")
    val ident = Identifier.of(Array("delta"), tablePath)
    val table = new SparkTable(ident, tablePath, new JHashMap[String, String]())
    StreamingRelationV2(
      source = None,
      sourceName = "delta",
      table = table,
      extraOptions = CaseInsensitiveStringMap.empty,
      output = toAttributes(table.schema),
      catalog = None,
      identifier = Some(ident),
      v1Relation = None)
  }

  test("rule is idempotent - second pass leaves the plan unchanged") {
    withTempDir { dir =>
      val plan = buildPlan(dir.getCanonicalPath, "id LONG, part LONG, col3 INT", "part")
      val once = V2StreamingSchemaReorder.apply(plan)
      val twice = V2StreamingSchemaReorder.apply(once)
      assert(twice == once)
    }
  }

  test("rule no-ops when partition column is last in DDL") {
    withTempDir { dir =>
      val plan = buildPlan(dir.getCanonicalPath, "id LONG, col3 INT, part LONG", "part")
      val result = V2StreamingSchemaReorder.apply(plan)
      assert(result == plan)
    }
  }

  test("Project list preserves rewritten StreamingRelationV2 attribute exprIds") {
    withTempDir { dir =>
      val plan = buildPlan(dir.getCanonicalPath, "id LONG, part LONG, col3 INT", "part")
      val result = V2StreamingSchemaReorder.apply(plan).asInstanceOf[Project]
      val rewrittenRel = result.child.asInstanceOf[StreamingRelationV2]
      val projectExprIds = result.projectList.map(_.asInstanceOf[AttributeReference].exprId).toSet
      val relationExprIds = rewrittenRel.output.map(_.exprId).toSet
      assert(projectExprIds == relationExprIds)
    }
  }
}
