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

package org.apache.spark.sql.delta.deletionvectors

/**
 * Test data constants for deletion vector test tables.
 * These are used by multiple test suites across packages.
 */
object DeletionVectorsTestData {
  val table1Path = "src/test/resources/delta/table-with-dv-large"
  // Table at version 0: contains [0, 2000)
  val expectedTable1DataV0 = Seq.range(0, 2000)
  // Table at version 1: removes rows with id = 0, 180, 300, 700, 1800
  val v1Removed = Set(0, 180, 300, 700, 1800)
  val expectedTable1DataV1 = expectedTable1DataV0.filterNot(e => v1Removed.contains(e))
  // Table at version 2: inserts rows with id = 300, 700
  val v2Added = Set(300, 700)
  val expectedTable1DataV2 = expectedTable1DataV1 ++ v2Added
  // Table at version 3: removes rows with id = 300, 250, 350, 900, 1353, 1567, 1800
  val v3Removed = Set(300, 250, 350, 900, 1353, 1567, 1800)
  val expectedTable1DataV3 = expectedTable1DataV2.filterNot(e => v3Removed.contains(e))
  // Table at version 4: inserts rows with id = 900, 1567
  val v4Added = Set(900, 1567)
  val expectedTable1DataV4 = expectedTable1DataV3 ++ v4Added

  val table2Path = "src/test/resources/delta/table-with-dv-small"
  // Table at version 0: contains 0 - 9
  val expectedTable2DataV0 = Seq(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
  // Table at version 1: removes rows 0 and 9
  val expectedTable2DataV1 = Seq(1, 2, 3, 4, 5, 6, 7, 8)

  val table3Path = "src/test/resources/delta/partitioned-table-with-dv-large"
  // Table at version 0: contains [0, 2000)
  val expectedTable3DataV0 = Seq.range(0, 2000)
  // Table at version 1: removes rows with id = (0, 180, 308, 225, 756, 1007, 1503)
  val table3V1Removed = Set(0, 180, 308, 225, 756, 1007, 1503)
  val expectedTable3DataV1 = expectedTable3DataV0.filterNot(e => table3V1Removed.contains(e))
  // Table at version 2: inserts rows with id = 308, 756
  val table3V2Added = Set(308, 756)
  val expectedTable3DataV2 = expectedTable3DataV1 ++ table3V2Added
  // Table at version 3: removes rows with id = (300, 257, 399, 786, 1353, 1567, 1800)
  val table3V3Removed = Set(300, 257, 399, 786, 1353, 1567, 1800)
  val expectedTable3DataV3 = expectedTable3DataV2.filterNot(e => table3V3Removed.contains(e))
  // Table at version 4: inserts rows with id = 1353, 1567
  val table3V4Added = Set(1353, 1567)
  val expectedTable3DataV4 = expectedTable3DataV3 ++ table3V4Added
}
