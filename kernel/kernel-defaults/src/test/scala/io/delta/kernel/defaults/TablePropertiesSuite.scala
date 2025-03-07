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

import scala.collection.immutable.Seq

import io.delta.kernel.Table
import io.delta.kernel.exceptions.UnknownConfigurationException
import io.delta.kernel.internal.SnapshotImpl
import io.delta.kernel.utils.CloseableIterable.emptyIterable

/**
 * Suite to set or get table properties.
 * TODO: for now we just have the support for `set`. API `get` will be added in the next PRs.
 */
class TablePropertiesSuite extends DeltaTableWriteSuiteBase {
  test("create/update table - allow arbitrary properties") {
    withTempDir { tempFile =>
      val tablePath = tempFile.getAbsolutePath

      // create table with arbitrary properties and check if they are set
      createUpdateTableWithProps(
        tablePath,
        createTable = true,
        props = Map("my key" -> "10", "my key2" -> "20"))
      assertHasProp(tablePath, expProps = Map("my key" -> "10", "my key2" -> "20"))

      // update table by modifying the arbitrary properties and check if they are updated
      createUpdateTableWithProps(tablePath, props = Map("my key" -> "30"))
      assertHasProp(tablePath, expProps = Map("my key" -> "30", "my key2" -> "20"))

      // update table without any new properties and check if the existing properties are retained
      createUpdateTableWithProps(tablePath)
      assertHasProp(tablePath, expProps = Map("my key" -> "30", "my key2" -> "20"))

      // update table by adding new arbitrary properties and check if they are set
      createUpdateTableWithProps(tablePath, props = Map("new key3" -> "str"))
      assertHasProp(
        tablePath,
        expProps = Map("my key" -> "30", "my key2" -> "20", "new key3" -> "str"))
    }
  }

  test("create/update table - disallow unknown delta.* properties to Kernel") {
    withTempDir { tempFile =>
      val tablePath = tempFile.getAbsolutePath
      val ex1 = intercept[UnknownConfigurationException] {
        createUpdateTableWithProps(tablePath, createTable = true, Map("delta.unknown" -> "str"))
      }
      assert(ex1.getMessage.contains("Unknown configuration was specified: delta.unknown"))

      // Try updating in an existing table
      createUpdateTableWithProps(tablePath, createTable = true)
      val ex2 = intercept[UnknownConfigurationException] {
        createUpdateTableWithProps(tablePath, props = Map("Delta.unknown" -> "str"))
      }
      assert(ex2.getMessage.contains("Unknown configuration was specified: Delta.unknown"))
    }
  }

  test("create/update table - delta configs are stored with same case as defined in TableConfig") {
    withTempDir { tempFile =>
      val tablePath = tempFile.getAbsolutePath
      createUpdateTableWithProps(
        tablePath,
        createTable = true,
        Map("delta.CHECKPOINTINTERVAL" -> "20"))
      assertHasProp(tablePath, expProps = Map("delta.checkpointInterval" -> "20"))

      // Try updating in an existing table
      createUpdateTableWithProps(
        tablePath,
        props = Map("DELTA.CHECKPOINTINTERVAL" -> "30"))
      assertHasProp(tablePath, expProps = Map("delta.checkpointInterval" -> "30"))
    }
  }

  def createUpdateTableWithProps(
      tablePath: String,
      createTable: Boolean = false,
      props: Map[String, String] = null): Unit = {
    createTxn(defaultEngine, tablePath, createTable, testSchema, Seq.empty, props)
      .commit(defaultEngine, emptyIterable())
  }

  // TODO: this will be replaced with get API in the next PRs.
  def assertHasProp(tablePath: String, expProps: Map[String, String]): Unit = {
    val snapshot = Table.forPath(defaultEngine, tablePath)
      .getLatestSnapshot(defaultEngine).asInstanceOf[SnapshotImpl]
    expProps.foreach { case (key, value) =>
      assert(snapshot.getMetadata.getConfiguration.get(key) === value, key)
    }
  }
}
