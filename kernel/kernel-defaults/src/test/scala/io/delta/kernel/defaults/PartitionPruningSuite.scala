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
package io.delta.kernel.defaults

import scala.collection.JavaConverters._

import io.delta.golden.GoldenTableUtils.goldenTablePath
import io.delta.kernel.defaults.utils.{TestRow, TestUtils}
import io.delta.kernel.expressions.{Column, Expression, Predicate}
import io.delta.kernel.expressions.Literal.ofInt
import org.scalatest.funsuite.AnyFunSuite

class PartitionPruningSuite extends AnyFunSuite with TestUtils {
  test("partition pruning: simple filter") {
    val filter = predicate("=", col("as_int"), ofInt(5))
    val selectedColumns = Seq("as_int", "as_long", "value")
    val expectedResult = Seq((5, 5L, "5"))

    checkTable(
      path = goldenTablePath("data-reader-partition-values"),
      expectedAnswer = expectedResult.map(TestRow.fromTuple(_)),
      readCols = selectedColumns,
      filter = filter
    )
  }

  private def col(names: String*): Column = {
    new Column(names.toArray)
  }

  private def predicate(name: String, children: Expression*): Predicate = {
    new Predicate(name, children.asJava)
  }
}
