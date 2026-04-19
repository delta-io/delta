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

package org.apache.spark.sql.delta.streaming

import java.util.Locale

import io.delta.spark.internal.v2.catalog.SparkTable

import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.streaming.StreamingRelationV2
import org.apache.spark.sql.connector.expressions.{FieldReference, IdentityTransform}

/**
 * Bridges DDL order (SparkTable.schema) and reader order (SparkScan.readSchema =
 * data ++ partitions) for V2 streaming reads of kernel-spark SparkTable. Without it, codegen
 * ordinals miss the right ColumnVector and NPE in OnHeapColumnVector.getLong whenever a
 * partition column isn't last. Mirrors V1's FileSourceStrategy ProjectExec injection and the
 * V2-batch Project wrap from V2ScanRelationPushDown.
 *
 * Paired with [[io.delta.internal.ApplyV2Streaming]], which produces the
 * StreamingRelationV2(SparkTable) this rule rewrites.
 */
object V2StreamingSchemaReorder extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperators {
    case s: StreamingRelationV2 if s.table.isInstanceOf[SparkTable] =>
      reorderIfNeeded(s, s.table.asInstanceOf[SparkTable], s.output)
  }

  private def reorderIfNeeded(
      s: StreamingRelationV2,
      table: SparkTable,
      output: Seq[AttributeReference]): LogicalPlan = {
    // SparkTable only emits identity transforms today. If kernel-spark ever adds bucket/year/etc.
    // or nested-field partitions, the rule's reader-order computation no longer matches the
    // physical scan layout - fail loudly instead of silently re-introducing the NPE.
    val partitionColumnNames: Seq[String] = table.partitioning.toSeq.map {
      case IdentityTransform(FieldReference(Seq(col))) => col
      case other =>
        throw new IllegalStateException(
          s"V2StreamingSchemaReorder only supports IdentityTransform partitioning on " +
            s"single-segment column names; got: $other")
    }
    if (partitionColumnNames.isEmpty) {
      return s
    }

    // Case-insensitive lookup matches SparkScanBuilder.partitionColumnSet so the rule still
    // fires if some upstream resolution path normalizes only one side's casing.
    def lower(name: String): String = name.toLowerCase(Locale.ROOT)
    val outputByName = output.map(a => lower(a.name) -> a).toMap
    val partitionNameSet = partitionColumnNames.map(lower).toSet

    val dataAttrs = output.filterNot(a => partitionNameSet.contains(lower(a.name)))
    val partitionAttrs = partitionColumnNames.flatMap(n => outputByName.get(lower(n)))
    val readerOrderOutput = dataAttrs ++ partitionAttrs

    if (readerOrderOutput.map(_.name) == output.map(_.name)) {
      return s
    }

    // output and readerOrderOutput share AttributeReference instances, so output resolves
    // as the project list against the reordered relation.
    Project(output, s.copy(output = readerOrderOutput))
  }
}
