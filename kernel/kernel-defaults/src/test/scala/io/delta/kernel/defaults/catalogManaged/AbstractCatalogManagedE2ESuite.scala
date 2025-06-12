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

import io.delta.kernel.defaults.catalogManaged.client.{AbstractCatalogManagedTestClient, InMemoryCatalogManagedTestClient}
import io.delta.kernel.defaults.catalogManaged.utils.{CatalogManagedTestFixtures, CatalogManagedTestUtils}
import io.delta.kernel.engine.Engine

import org.scalatest.funsuite.AnyFunSuite

class InMemoryCatalogManagedE2ESuite
    extends AbstractCatalogMangedE2ESuite
    with InMemoryCatalogManagedTestClient {
  override val engine: Engine = defaultEngine
}

trait AbstractCatalogMangedE2ESuite
    extends AnyFunSuite
    with AbstractCatalogManagedTestClient
    with CatalogManagedTestFixtures
    with CatalogManagedTestUtils {

  test("basic read") {
    withTempDir { tempDir =>
      val path = tempDir.getCanonicalPath
      val logPath = s"$path/_delta_log"
      createTable(path)
      publish(logPath, 0)
      appendData(path, commitVersion = 1, partitionValues = Seq(0))
      appendData(path, commitVersion = 2, partitionValues = Seq(1))
    }
  }
}
