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

package io.delta.kernel.defaults.catalogManaged

import scala.collection.JavaConverters._

import io.delta.kernel.{Operation, Table}
import io.delta.kernel.data.Row
import io.delta.kernel.defaults.utils.TestUtils
import io.delta.kernel.exceptions.KernelException
import io.delta.kernel.types.{IntegerType, StructType}
import io.delta.kernel.utils.CloseableIterable.emptyIterable

import org.scalatest.funsuite.AnyFunSuite

class CatalogManagedEnablementSuite extends AnyFunSuite with TestUtils {

  // TODO: [delta-io/delta#4764] This test uses the legacy Table and Snapshot APIs and so does not
  //       commit to any catalog. For now, this test exists to ensure that setting
  //       `delta.feature.catalogOwned-preview` correctly enables the catalogManaged table feature.
  //       When writer support is added, we will extend this test to check that the right properties
  //       and table features (e.g. ICT) were enabled.
  test("setting delta.feature.catalogOwned-preview enables the catalogManaged table feature") {
    withTempDir { tempDir =>
      val table = Table.forPath(defaultEngine, tempDir.getAbsolutePath)

      val tblProperties = Map("delta.feature.catalogOwned-preview" -> "supported").asJava

      val exMsg = intercept[KernelException] {
        table
          .createTransactionBuilder(defaultEngine, "engineInfo", Operation.CREATE_TABLE)
          .withSchema(defaultEngine, new StructType().add("id", IntegerType.INTEGER))
          .withTableProperties(defaultEngine, tblProperties)
          .build(defaultEngine)
          .commit(defaultEngine, emptyIterable[Row])
      }.getMessage

      assert(exMsg.contains("Unsupported Delta writer feature"))
      assert(exMsg.contains("requires writer table feature \"[catalogOwned-preview]\""))
    }
  }
}
