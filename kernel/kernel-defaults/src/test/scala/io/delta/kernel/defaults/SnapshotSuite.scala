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

package io.delta.kernel.defaults

import io.delta.kernel.{Operation, Table}
import io.delta.kernel.defaults.utils.TestUtils
import io.delta.kernel.types.{IntegerType, StructField, StructType}
import io.delta.kernel.utils.CloseableIterable
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.JavaConverters._

class SnapshotSuite extends AnyFunSuite with TestUtils {

  Seq(
    Seq("part1"), // simple case
    Seq("part1", "part2", "part3"), // multiple partition columns
    Seq(), // non-partitioned
    Seq("PART1", "part2") // case-sensitive
  ).foreach { partCols =>
    test(s"Snapshot getPartitionColumnNames - partCols=$partCols") {
      withTempDir { dir =>
        // Step 1: Create a table with the given partition columns
        val table = Table.forPath(defaultEngine, dir.getCanonicalPath)

        val columns = (partCols ++ Seq("col1", "col2")).map { colName =>
          new StructField(colName, IntegerType.INTEGER, true /* nullable */)
        }

        val schema = new StructType(columns.asJava)

        var txnBuilder = table
          .createTransactionBuilder(defaultEngine, "engineInfo", Operation.CREATE_TABLE)
          .withSchema(defaultEngine, schema)

        if (partCols.nonEmpty) {
          txnBuilder = txnBuilder.withPartitionColumns(defaultEngine, partCols.asJava)
        }

        txnBuilder.build(defaultEngine).commit(defaultEngine, CloseableIterable.emptyIterable())

        // Step 2: Check the partition columns
        val tablePartCols =
          table.getLatestSnapshot(defaultEngine).getPartitionColumnNames()

        assert(partCols.asJava === tablePartCols)
      }
    }
  }

}
