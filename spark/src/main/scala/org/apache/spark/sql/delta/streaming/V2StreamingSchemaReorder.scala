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

import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.streaming.StreamingRelationV2
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.expressions.{FieldReference, IdentityTransform}

/**
 * Bridges the schema-ordering mismatch between Spark's V2 streaming analysis layer and the Delta
 * V2 scan reader for kernel-spark (`SparkTable`).
 *
 * Spark resolves `spark.readStream().table("...")` to a `StreamingRelationV2` whose `output` is
 * derived from [[Table.columns]] - i.e., the table's DDL-ordered schema. But the kernel-spark V2
 * scan (`SparkScan.readSchema`) produces columnar batches in reader order: data columns first,
 * then partition columns. When the DDL puts a partition column anywhere but last, those two
 * orders diverge and streaming codegen binds ordinals to the wrong `ColumnVector`, causing an
 * NPE in `OnHeapColumnVector.getLong` (or similar, depending on the type mismatch).
 *
 * This rule re-binds `StreamingRelationV2.output` to reader order (data columns followed by
 * partition columns) and wraps the relation in a [[Project]] that re-emits attributes in DDL
 * order, preserving the user-facing schema of the streaming DataFrame (matching V1 behavior).
 * It mirrors V1, where Spark's planner naturally inserts a `ProjectExec` above
 * `FileSourceScanExec` to bridge `LogicalRelation`'s DDL-ordered output to the physical scan's
 * data++partitions output.
 *
 * The outer `Project` may be collapsed by the optimizer, which is fine: the rebinding on the
 * inner `StreamingRelationV2.output` is what makes codegen correct. `DataFrame.schema` is read
 * from the analyzed plan (pre-optimization), so the user-facing schema order is preserved
 * regardless.
 *
 * Scoped to kernel-spark `SparkTable` via runtime class-name check because this module (sparkV1)
 * does not depend on sparkV2 and cannot reference the `SparkTable` class directly. A canary unit
 * test in spark-unified asserts the FQCN string matches the actual class so a future rename
 * breaks the build instead of silently disabling this rule.
 */
case class V2StreamingSchemaReorder() extends Rule[LogicalPlan] {

  private def isKernelSparkTable(table: Table): Boolean =
    table.getClass.getName == V2StreamingSchemaReorder.SparkTableClassName

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperators {
    case s @ StreamingRelationV2(_, _, table, _, output, _, _, _)
        if isKernelSparkTable(table) =>
      reorderIfNeeded(s, table, output)
  }

  private def reorderIfNeeded(
      s: StreamingRelationV2,
      table: Table,
      output: Seq[AttributeReference]): LogicalPlan = {
    // Delta's `SparkTable` only ever emits identity transforms over top-level columns
    // (see `SparkTable#ensureInitialized`). Other transform shapes are skipped — they are
    // not represented as physical partition columns by the V2 reader, so reclassifying them
    // here would re-introduce the same NPE this rule fixes.
    val partitionColumnNames: Seq[String] = table.partitioning.toSeq.collect {
      case IdentityTransform(FieldReference(Seq(col))) => col
    }
    if (partitionColumnNames.isEmpty) {
      return s
    }

    val partitionNameSet = partitionColumnNames.toSet
    val outputByName = output.map(a => a.name -> a).toMap

    val dataAttrs = output.filterNot(a => partitionNameSet.contains(a.name))
    val partitionAttrs = partitionColumnNames.flatMap(outputByName.get)
    val readerOrderOutput = dataAttrs ++ partitionAttrs

    if (readerOrderOutput.map(_.name) == output.map(_.name)) {
      return s
    }

    // `output` and `readerOrderOutput` contain the same `AttributeReference` instances (just
    // permuted), so `output` itself is a valid project list whose ExprIds resolve against the
    // reordered relation.
    Project(output, s.copy(output = readerOrderOutput))
  }
}

object V2StreamingSchemaReorder {
  // FQCN of kernel-spark `SparkTable`. String-matched at runtime because sparkV1 cannot
  // compile-time-depend on sparkV2. `V2StreamingSchemaReorderSuite` (in spark-unified, where
  // both modules are on the classpath) asserts this matches `classOf[SparkTable].getName`.
  val SparkTableClassName = "io.delta.spark.internal.v2.catalog.SparkTable"
}
