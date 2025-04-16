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
import scala.collection.immutable.Seq

import io.delta.kernel.Table
import io.delta.kernel.exceptions.{KernelException, UnknownConfigurationException}
import io.delta.kernel.internal.{SnapshotImpl, TableConfig}
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

  test("Case is preserved for user properties and is case sensitive") {
    // This aligns with Spark's behavior
    withTempDir { tempFile =>
      val tablePath = tempFile.getAbsolutePath
      createUpdateTableWithProps(
        tablePath,
        createTable = true,
        Map("user.facing.PROP" -> "20"))
      assertHasProp(tablePath, expProps = Map("user.facing.PROP" -> "20"))

      // Try updating in an existing table
      createUpdateTableWithProps(
        tablePath,
        props = Map("user.facing.prop" -> "30"))
      assertHasProp(
        tablePath,
        expProps = Map("user.facing.PROP" -> "20", "user.facing.prop" -> "30"))
    }
  }

  test("Cannot unset delta table properties") {
    withTempDir { tablePath =>
      Seq("delta.checkpointInterval", "DELTA.checkpointInterval").foreach { key =>
        val e = intercept[IllegalArgumentException] {
          createWriteTxnBuilder(Table.forPath(defaultEngine, tablePath.getAbsolutePath))
            .withTablePropertiesRemoved(Set(key).asJava)
        }
        assert(
          e.getMessage.contains("Unsetting 'delta.' table properties is currently unsupported"))
      }
    }
  }

  test("Cannot set and unset the same table property in same txn - new property") {
    withTempDir { tempFile =>
      val tablePath = tempFile.getAbsolutePath
      createEmptyTable(tablePath = tablePath, schema = testSchema)

      val e = intercept[KernelException] {
        createWriteTxnBuilder(Table.forPath(defaultEngine, tablePath))
          .withTablePropertiesRemoved(Set("foo.key").asJava)
          .withTableProperties(defaultEngine, Map("foo.key" -> "value").asJava)
          .build(defaultEngine)
      }
      assert(e.getMessage.contains(
        "Cannot set and unset the same table property in the same transaction. "
          + "Properties set and unset: [foo.key]"))
    }
  }

  test("Cannot set and unset the same table property in same txn - existing property") {
    // i.e. we don't only check against the new properties
    withTempDir { tempFile =>
      val tablePath = tempFile.getAbsolutePath
      // Create initial table with the property
      createEmptyTable(
        tablePath = tablePath,
        schema = testSchema,
        tableProperties = Map("foo.key" -> "value"))

      // Try to set and unset the existing property
      val e = intercept[KernelException] {
        createWriteTxnBuilder(Table.forPath(defaultEngine, tablePath))
          .withTablePropertiesRemoved(Set("foo.key").asJava)
          .withTableProperties(defaultEngine, Map("foo.key" -> "value").asJava)
          .build(defaultEngine)
      }
      assert(e.getMessage.contains(
        "Cannot set and unset the same table property in the same transaction. "
          + "Properties set and unset: [foo.key]"))
    }
  }

  test("Unset valid cases - properties are removed from the table") {
    withTempDir { tempFile =>
      val tablePath = tempFile.getAbsolutePath
      // Create initial table with properties
      // This test also validates the operation is case-sensitive
      createEmptyTable(
        tablePath = tablePath,
        schema = testSchema,
        tableProperties = Map("foo.key" -> "value", "FOO.KEY" -> "VALUE"))
      assertHasProp(tablePath, Map("foo.key" -> "value", "FOO.KEY" -> "VALUE"))

      // Remove 1 of the properties set
      createWriteTxnBuilder(Table.forPath(defaultEngine, tablePath))
        .withTablePropertiesRemoved(Set("foo.key").asJava)
        .build(defaultEngine)
        .commit(defaultEngine, emptyIterable())
      assertPropsDNE(tablePath, Set("foo.key"))
      // Check that the other property is not touched
      assertHasProp(tablePath, Map("FOO.KEY" -> "VALUE"))

      // Can unset a property that DNE
      createWriteTxnBuilder(Table.forPath(defaultEngine, tablePath))
        .withTablePropertiesRemoved(Set("not.a.key").asJava)
        .build(defaultEngine)
        .commit(defaultEngine, emptyIterable())
      assertPropsDNE(tablePath, Set("not.a.key"))
      // Check that the other property is not touched
      assertHasProp(tablePath, Map("FOO.KEY" -> "VALUE"))

      // Can be simultaneous with setTblProps as long as no overlap
      createWriteTxnBuilder(Table.forPath(defaultEngine, tablePath))
        .withTablePropertiesRemoved(Set("FOO.KEY").asJava)
        .withTableProperties(defaultEngine, Map("foo.key" -> "value-new").asJava)
        .build(defaultEngine)
        .commit(defaultEngine, emptyIterable())
      assertPropsDNE(tablePath, Set("FOO.KEY"))
      // Check that the other property is added successfully
      assertHasProp(tablePath, Map("foo.key" -> "value-new"))
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

  def assertPropsDNE(tablePath: String, keys: Set[String]): Unit = {
    val metadata = getMetadata(defaultEngine, tablePath)
    assert(keys.forall(!metadata.getConfiguration.containsKey(_)))
  }
}
