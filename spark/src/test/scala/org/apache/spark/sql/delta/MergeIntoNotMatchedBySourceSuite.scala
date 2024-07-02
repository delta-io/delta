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

// scalastyle:off import.ordering.noEmptyLine
import org.apache.spark.sql.delta.sources.DeltaSQLConf

import org.apache.spark.sql.Row

trait MergeIntoNotMatchedBySourceSuite extends MergeIntoSuiteBase {
  import testImplicits._

  /**
   * Variant of `testExtendedMerge` that runs a MERGE INTO command, checks the expected result and
   * additionally validate that the CDC produced is correct.
   */
  protected def testExtendedMergeWithCDC(
      name: String,
      namePrefix: String = "not matched by source")(
      source: Seq[(Int, Int)],
      target: Seq[(Int, Int)],
      mergeOn: String,
      mergeClauses: MergeClause*)(
      result: Seq[(Int, Int)],
      cdc: Seq[(Int, Int, String)]): Unit = {

    for {
      isPartitioned <- BOOLEAN_DOMAIN
      cdcEnabled <- BOOLEAN_DOMAIN
    } {
      test(s"$namePrefix - $name - isPartitioned: $isPartitioned - cdcEnabled: $cdcEnabled") {
        withSQLConf(
          DeltaConfigs.CHANGE_DATA_FEED.defaultTablePropertyKey -> cdcEnabled.toString) {
          withKeyValueData(source, target, isPartitioned) { case (sourceName, targetName) =>
            withSQLConf(DeltaSQLConf.MERGE_INSERT_ONLY_ENABLED.key -> "true") {
              executeMerge(s"$targetName t", s"$sourceName s", mergeOn, mergeClauses: _*)
            }
            val deltaPath = if (targetName.startsWith("delta.`")) {
              targetName.stripPrefix("delta.`").stripSuffix("`")
            } else targetName
            checkAnswer(readDeltaTable(deltaPath), result.map { case (k, v) => Row(k, v) })
          }
          if (cdcEnabled) {
            checkAnswer(getCDCForLatestOperation(deltaLog, DeltaOperations.OP_MERGE), cdc.toDF())
          }
        }
      }
    }
  }

  // Test analysis errors with NOT MATCHED BY SOURCE clauses.
  testMergeAnalysisException(
    "error on multiple not matched by source update clauses without condition")(
    mergeOn = "s.key = t.key",
    updateNotMatched(condition = "t.key == 3", set = "value = 2 * value"),
    updateNotMatched(set = "value = 3 * value"),
    updateNotMatched(set = "value = 4 * value"))(
    expectedErrorClass = "NON_LAST_NOT_MATCHED_BY_SOURCE_CLAUSE_OMIT_CONDITION",
    expectedMessageParameters = Map.empty)

  testMergeAnalysisException(
    "error on multiple not matched by source update/delete clauses without condition")(
    mergeOn = "s.key = t.key",
    updateNotMatched(condition = "t.key == 3", set = "value = 2 * value"),
    deleteNotMatched(),
    updateNotMatched(set = "value = 4 * value"))(
    expectedErrorClass = "NON_LAST_NOT_MATCHED_BY_SOURCE_CLAUSE_OMIT_CONDITION",
    expectedMessageParameters = Map.empty)

  testMergeAnalysisException(
    "error on non-empty condition following empty condition in not matched by source " +
      "update clauses")(
    mergeOn = "s.key = t.key",
    updateNotMatched(set = "value = 2 * value"),
    updateNotMatched(condition = "t.key < 3", set = "value = value"))(
    expectedErrorClass = "NON_LAST_NOT_MATCHED_BY_SOURCE_CLAUSE_OMIT_CONDITION",
    expectedMessageParameters = Map.empty)

  testMergeAnalysisException(
    "error on non-empty condition following empty condition in not matched by source " +
      "delete clauses")(
    mergeOn = "s.key = t.key",
    deleteNotMatched(),
    deleteNotMatched(condition = "t.key < 3"))(
    expectedErrorClass = "NON_LAST_NOT_MATCHED_BY_SOURCE_CLAUSE_OMIT_CONDITION",
    expectedMessageParameters = Map.empty)

  testMergeAnalysisException("update not matched condition - unknown reference")(
    mergeOn = "s.key = t.key",
    updateNotMatched(condition = "unknownAttrib > 1", set = "tgtValue = tgtValue + 1"))(
    expectedErrorClass = "DELTA_MERGE_UNRESOLVED_EXPRESSION",
    expectedMessageParameters = Map(
      "sqlExpr" -> "unknownAttrib",
      "clause" -> "UPDATE condition",
      "cols" -> "t.key, t.tgtValue"))

  testMergeAnalysisException("update not matched condition - aggregation function")(
    mergeOn = "s.key = t.key",
    updateNotMatched(condition = "max(0) > 0", set = "tgtValue = tgtValue + 1"))(
    expectedErrorClass = "DELTA_AGGREGATION_NOT_SUPPORTED",
    expectedMessageParameters = Map(
      "operation" -> "UPDATE condition of MERGE operation",
      "predicate" -> "(condition = (max(0) > 0))."))

  testMergeAnalysisException("update not matched condition - subquery")(
    mergeOn = "s.key = t.key",
    updateNotMatched(condition = "tgtValue in (select value from t)", set = "tgtValue = 1"))(
    expectedErrorClass = "TABLE_OR_VIEW_NOT_FOUND",
    expectedMessageParameters = Map("relationName" -> "`t`"))

  testMergeAnalysisException("delete not matched condition - unknown reference")(
    mergeOn = "s.key = t.key",
    deleteNotMatched(condition = "unknownAttrib > 1"))(
    expectedErrorClass = "DELTA_MERGE_UNRESOLVED_EXPRESSION",
    expectedMessageParameters = Map(
      "sqlExpr" -> "unknownAttrib",
      "clause" -> "DELETE condition",
      "cols" -> "t.key, t.tgtValue"))

  testMergeAnalysisException("delete not matched condition - aggregation function")(
    mergeOn = "s.key = t.key",
    deleteNotMatched(condition = "max(0) > 0"))(
    expectedErrorClass = "DELTA_AGGREGATION_NOT_SUPPORTED",
    expectedMessageParameters = Map(
      "operation" -> "DELETE condition of MERGE operation",
      "predicate" -> "(condition = (max(0) > 0))."))

  testMergeAnalysisException("delete not matched condition - subquery")(
    mergeOn = "s.key = t.key",
    deleteNotMatched(condition = "tgtValue in (select tgtValue from t)"))(
    expectedErrorClass = "TABLE_OR_VIEW_NOT_FOUND",
    expectedMessageParameters = Map("relationName" -> "`t`"))

  // Test correctness with NOT MATCHED BY SOURCE clauses.
  testExtendedMergeWithCDC("all 3 types of match clauses without conditions")(
    source = (0, 0) :: (1, 1) :: (5, 5) :: Nil,
    target = (2, 20) :: (1, 10) :: (5, 50) :: Nil,
    mergeOn = "s.key = t.key",
    update(set = "*"),
    insert(values = "*"),
    deleteNotMatched())(
    result = Seq(
      (0, 0), // No matched by target, inserted
      (1, 1), // Matched, updated
      // (2, 20) Not matched by source, deleted
      (5, 5) // Matched, updated
    ),
    cdc = Seq(
      (0, 0, "insert"),
      (1, 10, "update_preimage"),
      (1, 1, "update_postimage"),
      (2, 20, "delete"),
      (5, 50, "update_preimage"),
      (5, 5, "update_postimage")))

  testExtendedMergeWithCDC("all 3 types of match clauses with conditions")(
    source = (0, 0) :: (1, 1) :: (5, 5) :: (6, 6) :: Nil,
    target = (1, 10) :: (2, 20) :: (5, 50) :: (7, 70) :: Nil,
    mergeOn = "s.key = t.key",
    update(set = "*", condition = "t.value < 30"),
    insert(values = "*", condition = "s.value < 4"),
    deleteNotMatched(condition = "t.value > 40"))(
    result = Seq(
      (0, 0), // Not matched by target, inserted
      (1, 1), // Matched, updated
      (2, 20), // Not matched by source, no change
      (5, 50) // Matched, not updated
      // (6, 6) Not matched by target, no change
      // (7, 7) Not matched by source, deleted
    ),
    cdc = Seq(
      (0, 0, "insert"),
      (1, 10, "update_preimage"),
      (1, 1, "update_postimage"),
      (7, 70, "delete")))

  testExtendedMergeWithCDC("unconditional delete only when not matched by source")(
    source = (0, 0) :: (1, 1) :: (5, 5) :: Nil,
    target = (2, 20) :: (1, 10) :: (5, 50) :: (6, 60) :: Nil,
    mergeOn = "s.key = t.key",
    deleteNotMatched())(
    result = Seq(
      (1, 10), // Matched, no change
      // (2, 20) Not matched by source, deleted
      (5, 50) // Matched, no change
      // (6, 60) Not matched by source, deleted
    ),
    cdc = Seq((2, 20, "delete"), (6, 60, "delete")))

  testExtendedMergeWithCDC("conditional delete only when not matched by source")(
    source = (0, 0) :: (1, 1) :: (5, 5) :: Nil,
    target = (1, 10) :: (2, 20) :: (5, 50) :: (6, 60) :: Nil,
    mergeOn = "s.key = t.key",
    deleteNotMatched(condition = "t.value > 40"))(
    result = Seq(
      (1, 10), // Matched, no change
      (2, 20), // Not matched by source, no change
      (5, 50) // Matched, no change
      // (6, 60) Not matched by source, deleted
    ),
    cdc = Seq((6, 60, "delete")))

  testExtendedMergeWithCDC("delete only matched and not matched by source")(
    source = (1, 1) :: (2, 2) :: (5, 5) :: (6, 6) :: Nil,
    target = (1, 10) :: (2, 20) :: (3, 30) :: (4, 40) :: Nil,
    mergeOn = "s.key = t.key",
    delete("s.value % 2 = 0"),
    deleteNotMatched("t.value % 20 = 0"))(
    result = Seq(
      (1, 10), // Matched, no change
      // (2, 20) Matched, deleted
      (3, 30) // Not matched by source, no change
      // (4, 40) Not matched by source, deleted
    ),
    cdc = Seq((2, 20, "delete"), (4, 40, "delete")))

  testExtendedMergeWithCDC("unconditionally delete matched and not matched by source")(
    source = (0, 0) :: (1, 1) :: (5, 5) :: (6, 6) :: Nil,
    target = (1, 10) :: (2, 20) :: (5, 50) :: Nil,
    mergeOn = "s.key = t.key",
    delete(),
    deleteNotMatched())(
    result = Seq.empty,
    cdc = Seq((1, 10, "delete"), (2, 20, "delete"), (5, 50, "delete")))

  testExtendedMergeWithCDC("unconditional not matched by source update")(
    source = (0, 0) :: (1, 1) :: (5, 5) :: Nil,
    target = (1, 10) :: (2, 20) :: (4, 40) :: (5, 50) :: Nil,
    mergeOn = "s.key = t.key",
    updateNotMatched(set = "t.value = t.value + 1"))(
    result = Seq(
      (1, 10), // Matched, no change
      (2, 21), // Not matched by source, updated
      (4, 41), // Not matched by source, updated
      (5, 50) // Matched, no change
    ),
    cdc = Seq(
      (2, 20, "update_preimage"),
      (2, 21, "update_postimage"),
      (4, 40, "update_preimage"),
      (4, 41, "update_postimage")))

  testExtendedMergeWithCDC("conditional not matched by source update")(
    source = (0, 0) :: (1, 1) :: (5, 5) :: Nil,
    target = (1, 10) :: (2, 20) :: (4, 40) :: (5, 50) :: Nil,
    mergeOn = "s.key = t.key",
    updateNotMatched(condition = "t.value = 20", set = "t.value = t.value + 1"))(
    result = Seq(
      (1, 10), // Matched, no change
      (2, 21), // Not matched by source, updated
      (4, 40), // Not matched by source, no change
      (5, 50) // Matched, no change
    ),
    cdc = Seq((2, 20, "update_preimage"), (2, 21, "update_postimage")))

  testExtendedMergeWithCDC("not matched by source update and delete with skipping")(
    source = (0, 0) :: (1, 1) :: (2, 2) :: (5, 5) :: Nil,
    target = (1, 10) :: (2, 20) :: (4, 40) :: (5, 50) :: Nil,
    mergeOn = "s.key = t.key and t.key > 4",
    updateNotMatched(condition = "t.key = 1", set = "t.value = t.value + 1"),
    deleteNotMatched(condition = "t.key = 4"))(
    result = Seq(
      (1, 11), // Not matched by source based on merge condition, updated
      (2, 20), // Not matched by source based on merge condition, no change
      // (4, 40), Not matched by source, deleted
      (5, 50) // Matched, no change
    ),
    cdc = Seq(
      (1, 10, "update_preimage"),
      (1, 11, "update_postimage"),
      (4, 40, "delete")))

  testExtendedMergeWithCDC(
    "matched delete and not matched by source update with skipping")(
    source = (0, 0) :: (1, 1) :: (2, 2) :: (5, 5) :: (6, 6) :: Nil,
    target = (1, 10) :: (2, 20) :: (4, 40) :: (5, 50) :: (6, 60) :: Nil,
    mergeOn = "s.key = t.key and t.key > 4",
    delete(condition = "t.key = 5"),
    updateNotMatched(condition = "t.key = 1", set = "t.value = t.value + 1"))(
    result = Seq(
      (1, 11), // Not matched by source based on merge condition, updated
      (2, 20), // Not matched by source based on merge condition, no change
      (4, 40), // Not matched by source, no change
      // (5, 50), Matched, deleted
      (6, 60) // Matched, no change
    ),
    cdc = Seq(
      (1, 10, "update_preimage"),
      (1, 11, "update_postimage"),
      (5, 50, "delete")))

  testExtendedMergeWithCDC("not matched by source update + delete clauses")(
    source = (0, 0) :: (1, 1) :: (5, 5) :: Nil,
    target = (1, 10) :: (2, 20) :: (7, 70) :: Nil,
    mergeOn = "s.key = t.key",
    deleteNotMatched("t.value % 20 = 0"),
    updateNotMatched(set = "t.value = t.value + 1"))(
    result = Seq(
      (1, 10), // Matched, no change
      // (2, 20) Not matched by source, deleted
      (7, 71) // Not matched by source, updated
    ),
    cdc = Seq((2, 20, "delete"), (7, 70, "update_preimage"), (7, 71, "update_postimage")))

  testExtendedMergeWithCDC("unconditional not matched by source update + not matched insert")(
    source = (0, 0) :: (1, 1) :: (4, 4) :: (5, 5) :: Nil,
    target = (1, 10) :: (2, 20) :: (4, 40) :: (7, 70) :: Nil,
    mergeOn = "s.key = t.key",
    insert("*"),
    updateNotMatched(set = "t.value = t.value + 1"))(
    result = Seq(
      (0, 0), // Not matched by target, inserted
      (1, 10), // Matched, no change
      (2, 21), // Not matched by source, updated
      (4, 40), // Matched, no change
      (5, 5), // Not matched by target, inserted
      (7, 71) // Not matched by source, updated
    ),
    cdc = Seq(
      (0, 0, "insert"),
      (2, 20, "update_preimage"),
      (2, 21, "update_postimage"),
      (5, 5, "insert"),
      (7, 70, "update_preimage"),
      (7, 71, "update_postimage")))

  testExtendedMergeWithCDC("not matched by source delete + not matched insert")(
    source = (0, 0) :: (1, 1) :: (5, 5) :: Nil,
    target = (1, 10) :: (2, 20) :: (7, 70) :: Nil,
    mergeOn = "s.key = t.key",
    insert("*"),
    deleteNotMatched("t.value % 20 = 0"))(
    result = Seq(
      (0, 0), // Not matched by target, inserted
      (1, 10), // Matched, no change
      // (2, 20), Not matched by source, deleted
      (5, 5), // Not matched by target, inserted
      (7, 70) // Not matched by source, no change
    ),
    cdc = Seq((0, 0, "insert"), (2, 20, "delete"), (5, 5, "insert")))

  testExtendedMergeWithCDC("multiple not matched by source clauses")(
    source = (0, 0) :: (1, 1) :: (5, 5) :: Nil,
    target = (6, 6) :: (7, 7) :: (8, 8) :: (9, 9) :: (10, 10) :: (11, 11) :: Nil,
    mergeOn = "s.key = t.key",
    updateNotMatched(condition = "t.key % 6 = 0", set = "t.value = t.value + 5"),
    updateNotMatched(condition = "t.key % 6 = 1", set = "t.value = t.value + 4"),
    updateNotMatched(condition = "t.key % 6 = 2", set = "t.value = t.value + 3"),
    updateNotMatched(condition = "t.key % 6 = 3", set = "t.value = t.value + 2"),
    updateNotMatched(condition = "t.key % 6 = 4", set = "t.value = t.value + 1"),
    deleteNotMatched())(
    result = Seq(
      (6, 11), // Not matched by source, updated
      (7, 11), // Not matched by source, updated
      (8, 11), // Not matched by source, updated
      (9, 11), // Not matched by source, updated
      (10, 11) // Not matched by source, updated
      // (11, 11) Not matched by source, deleted
    ),
    cdc = Seq(
      (6, 6, "update_preimage"),
      (6, 11, "update_postimage"),
      (7, 7, "update_preimage"),
      (7, 11, "update_postimage"),
      (8, 8, "update_preimage"),
      (8, 11, "update_postimage"),
      (9, 9, "update_preimage"),
      (9, 11, "update_postimage"),
      (10, 10, "update_preimage"),
      (10, 11, "update_postimage"),
      (11, 11, "delete")))

  testExtendedMergeWithCDC("not matched by source update + conditional insert")(
    source = (1, 1) :: (0, 2) :: (5, 5) :: Nil,
    target = (2, 2) :: (1, 4) :: (7, 3) :: Nil,
    mergeOn = "s.key = t.key",
    insert(condition = "s.value % 2 = 0", values = "*"),
    updateNotMatched(set = "t.value = t.value + 1"))(
    result = Seq(
      (0, 2), // Not matched (by target), inserted
      (2, 3), // Not matched by source, updated
      (1, 4), // Matched, no change
      // (5, 5) // Not matched (by target), not inserted
      (7, 4) // Not matched by source, updated
    ),
    cdc = Seq(
      (0, 2, "insert"),
      (2, 2, "update_preimage"),
      (2, 3, "update_postimage"),
      (7, 3, "update_preimage"),
      (7, 4, "update_postimage")))

  testExtendedMergeWithCDC("not matched by source delete + conditional insert")(
    source = (1, 1) :: (0, 2) :: (5, 5) :: Nil,
    target = (2, 2) :: (1, 4) :: (7, 3) :: Nil,
    mergeOn = "s.key = t.key",
    insert(condition = "s.value % 2 = 0", values = "*"),
    deleteNotMatched(condition = "t.value > 2"))(
    result = Seq(
      (0, 2), // Not matched (by target), inserted
      (2, 2), // Not matched by source, no change
      (1, 4) // Matched, no change
      // (5, 5) // Not matched (by target), not inserted
      // (7, 3) Not matched by source, deleted
    ),
    cdc = Seq((0, 2, "insert"), (7, 3, "delete")))

  testExtendedMergeWithCDC("when not matched by source updates all rows")(
    source = (1, 1) :: (0, 2) :: (5, 5) :: Nil,
    target = (3, 3) :: (4, 4) :: (6, 6) :: (7, 7) :: (8, 8) :: (9, 9) :: Nil,
    mergeOn = "s.key = t.key",
    updateNotMatched(set = "t.value = t.value + 1"))(
    result = Seq(
      (3, 4), // Not matched by source, updated
      (4, 5), // Not matched by source, updated
      (6, 7), // Not matched by source, updated
      (7, 8), // Not matched by source, updated
      (8, 9), // Not matched by source, updated
      (9, 10) // Not matched by source, updated
    ),
    cdc = Seq(
      (3, 3, "update_preimage"),
      (3, 4, "update_postimage"),
      (4, 4, "update_preimage"),
      (4, 5, "update_postimage"),
      (6, 6, "update_preimage"),
      (6, 7, "update_postimage"),
      (7, 7, "update_preimage"),
      (7, 8, "update_postimage"),
      (8, 8, "update_preimage"),
      (8, 9, "update_postimage"),
      (9, 9, "update_preimage"),
      (9, 10, "update_postimage")))

  testExtendedMergeWithCDC("insert only with dummy not matched by source")(
    source = (1, 1) :: (0, 2) :: (5, 5) :: Nil,
    target = (2, 2) :: (1, 4) :: (7, 3) :: Nil,
    mergeOn = "s.key = t.key",
    insert(condition = "s.value % 2 = 0", values = "*"),
    deleteNotMatched(condition = "t.value > 10"))(
    result = Seq(
      (0, 2), // Not matched (by target), inserted
      (2, 2), // Not matched by source, no change
      (1, 4), // Matched, no change
      // (5, 5) // Not matched (by target), not inserted
      (7, 3) // Not matched by source, no change
    ),
    cdc = Seq((0, 2, "insert")))

  testExtendedMergeWithCDC("empty source")(
    source = Nil,
    target = (2, 2) :: (1, 4) :: (7, 3) :: Nil,
    mergeOn = "s.key = t.key",
    updateNotMatched(condition = "t.key = 2", set = "value = t.value + 1"),
    deleteNotMatched(condition = "t.key = 7"))(
    result = Seq(
      (2, 3), // Not matched by source, updated
      (1, 4) // Not matched by source, no change
      // (7, 3) Not matched by source, deleted
    ),
    cdc = Seq(
      (2, 2, "update_preimage"),
      (2, 3, "update_postimage"),
      (7, 3, "delete")))

  testExtendedMergeWithCDC("empty source delete only")(
    source = Nil,
    target = (2, 2) :: (1, 4) :: (7, 3) :: Nil,
    mergeOn = "s.key = t.key",
    deleteNotMatched(condition = "t.key = 7"))(
    result = Seq(
      (2, 2), // Not matched by source, no change
      (1, 4) // Not matched by source, no change
      // (7, 3) Not matched by source, deleted
    ),
    cdc = Seq((7, 3, "delete")))

  testExtendedMergeWithCDC("all 3 clauses - no changes")(
    source = (1, 1) :: (0, 2) :: (5, 5) :: Nil,
    target = (2, 2) :: (1, 4) :: (7, 3) :: Nil,
    mergeOn = "s.key = t.key",
    update(condition = "t.value > 10", set = "*"),
    insert(condition = "s.value > 10", values = "*"),
    deleteNotMatched(condition = "t.value > 10"))(
    result = Seq(
      (2, 2), // Not matched by source, no change
      (1, 4), // Matched, no change
      (7, 3) // Not matched by source, no change
    ),
    cdc = Seq.empty)

  test(s"special character in path - not matched by source delete") {
    val source = s"$tempDir/sou rce^"
    val target = s"$tempDir/tar get="
    spark.range(0, 10, 2).write.format("delta").save(source)
    spark.range(10).write.format("delta").save(target)
    executeMerge(
      tgt = s"delta.`$target` t",
      src = s"delta.`$source` s",
      cond = "t.id = s.id",
      clauses = deleteNotMatched())
    checkAnswer(readDeltaTable(target), Seq(0, 2, 4, 6, 8).toDF("id"))
  }

  test(s"special character in path - not matched by source update") {
    val source = s"$tempDir/sou rce@"
    val target = s"$tempDir/tar get#"
    spark.range(0, 10, 2).write.format("delta").save(source)
    spark.range(10).write.format("delta").save(target)
    executeMerge(
      tgt = s"delta.`$target` t",
      src = s"delta.`$source` s",
      cond = "t.id = s.id",
      clauses = updateNotMatched(set = "id = t.id * 10"))
    checkAnswer(readDeltaTable(target), Seq(0, 10, 2, 30, 4, 50, 6, 70, 8, 90).toDF("id"))
  }
}
