/*
 * Copyright (2025) The Delta Lake Project Authors.
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
import io.delta.kernel.exceptions.KernelException
import io.delta.kernel.types.IntegerType.INTEGER
import io.delta.kernel.types.StructType
import io.delta.kernel.utils.CloseableIterable
import org.scalatest.funsuite.AnyFunSuite

class ScanBuilderSuite extends AnyFunSuite with TestUtils {
  test("readSchema must be a subset of the snapshot schema") {
    withTempDir { tempDir =>
      val tableSchema = new StructType().add("a", INTEGER).add("b", INTEGER)
      val readSchema = new StructType().add("c", INTEGER)
      val path = tempDir.getAbsolutePath
      val table = Table.forPath(defaultEngine, path)
      table.createTransactionBuilder(defaultEngine, "engineInfo", Operation.CREATE_TABLE)
        .withSchema(defaultEngine, tableSchema)
        .build(defaultEngine)
        .commit(defaultEngine, CloseableIterable.emptyIterable())

      val exMsg = intercept[KernelException] {
        table
          .getLatestSnapshot(defaultEngine)
          .getScanBuilder(defaultEngine)
          .withReadSchema(defaultEngine, readSchema)
      }.getMessage
      assert(exMsg.contains("Read schema is not a subset of the table schema."))
    }
  }
}
