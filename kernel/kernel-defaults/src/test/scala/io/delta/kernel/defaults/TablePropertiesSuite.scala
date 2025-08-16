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
import io.delta.kernel.defaults.utils.{AbstractWriteUtils, WriteUtils, WriteUtilsWithV2Builders}
import io.delta.kernel.exceptions.{KernelException, UnknownConfigurationException}
import io.delta.kernel.utils.CloseableIterable.emptyIterable

import org.scalatest.funsuite.AnyFunSuite

class TablePropertiesTransactionBuilderV1Suite extends TablePropertiesSuiteBase with WriteUtils {}

class TablePropertiesTransactionBuilderV2Suite extends TablePropertiesSuiteBase
    with WriteUtilsWithV2Builders {}

/**
 * Suite to set or get table properties.
 */
trait TablePropertiesSuiteBase extends AnyFunSuite with AbstractWriteUtils {
  test("create/update/replace table - allow arbitrary properties") {
    withTempDir { tempFile =>
      val tablePath = tempFile.getAbsolutePath

      // create table with arbitrary properties and check if they are set
      createUpdateTableWithProps(
        tablePath,
        createTable = true,
        propsAdded = Map("my key" -> "10", "my key2" -> "20"))
      assertHasProp(tablePath, expProps = Map("my key" -> "10", "my key2" -> "20"))

      // update table by modifying the arbitrary properties and check if they are updated
      createUpdateTableWithProps(tablePath, propsAdded = Map("my key" -> "30"))
      assertHasProp(tablePath, expProps = Map("my key" -> "30", "my key2" -> "20"))

      // update table without any new properties and check if the existing properties are retained
      createUpdateTableWithProps(tablePath)
      assertHasProp(tablePath, expProps = Map("my key" -> "30", "my key2" -> "20"))

      // update table by adding new arbitrary properties and check if they are set
      createUpdateTableWithProps(tablePath, propsAdded = Map("new key3" -> "str"))
      assertHasProp(
        tablePath,
        expProps = Map("my key" -> "30", "my key2" -> "20", "new key3" -> "str"))

      // replace table and set new arbitrary properties and check if they are set (and old ones are
      // removed)
      getReplaceTxn(
        defaultEngine,
        tablePath,
        testSchema,
        tableProperties = Map("my key" -> "40", "my replace key" -> "0"))
        .commit(defaultEngine, emptyIterable())
      assertHasProp(
        tablePath,
        expProps = Map("my key" -> "40", "my replace key" -> "0"))
      assertPropsDNE(tablePath, Set("my key2", "my key3"))
    }
  }

  test("create/update/replace table - disallow unknown delta.* properties to Kernel") {
    withTempDir { tempFile =>
      val tablePath = tempFile.getAbsolutePath
      val ex1 = intercept[UnknownConfigurationException] {
        createUpdateTableWithProps(tablePath, createTable = true, Map("delta.unknown" -> "str"))
      }
      assert(ex1.getMessage.contains("Unknown configuration was specified: delta.unknown"))

      // Try updating in an existing table
      createUpdateTableWithProps(tablePath, createTable = true)
      val ex2 = intercept[UnknownConfigurationException] {
        createUpdateTableWithProps(tablePath, propsAdded = Map("Delta.unknown" -> "str"))
      }
      assert(ex2.getMessage.contains("Unknown configuration was specified: Delta.unknown"))

      // Try replacing an existing table
      val ex3 = intercept[UnknownConfigurationException] {
        getReplaceTxn(
          defaultEngine,
          tablePath,
          testSchema,
          tableProperties = Map("Delta.unknown" -> "str"))
      }
      assert(ex3.getMessage.contains("Unknown configuration was specified: Delta.unknown"))
    }
  }

  test("create/update/replace table - delta configs are stored with same case as " +
    "defined in TableConfig") {
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
        propsAdded = Map("DELTA.CHECKPOINTINTERVAL" -> "30"))
      assertHasProp(tablePath, expProps = Map("delta.checkpointInterval" -> "30"))

      // Try replacing an existing table
      getReplaceTxn(
        defaultEngine,
        tablePath,
        testSchema,
        tableProperties = Map("DELTA.CHECKPOINTINTERVAL" -> "30"))
        .commit(defaultEngine, emptyIterable())
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
        propsAdded = Map("user.facing.prop" -> "30"))
      assertHasProp(
        tablePath,
        expProps = Map("user.facing.PROP" -> "20", "user.facing.prop" -> "30"))

      // Try replacing an existing table
      getReplaceTxn(
        defaultEngine,
        tablePath,
        testSchema,
        tableProperties = Map("user.facing.prop" -> "30", "user.facing.PROP" -> "20"))
        .commit(defaultEngine, emptyIterable())
      assertHasProp(
        tablePath,
        expProps = Map("user.facing.PROP" -> "20", "user.facing.prop" -> "30"))
    }
  }

  test("Cannot unset delta table properties") {
    withTempDir { tablePath =>
      // Create empty table with delta props
      createUpdateTableWithProps(
        tablePath.getAbsolutePath,
        createTable = true,
        propsAdded = Map("delta.checkpointInterval" -> "10"))
      Seq("delta.checkpointInterval", "DELTA.checkpointInterval").foreach { key =>
        val e = intercept[IllegalArgumentException] {
          createUpdateTableWithProps(
            tablePath.getAbsolutePath,
            propsRemoved = Set(key))
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
        createUpdateTableWithProps(
          tablePath,
          propsAdded = Map("foo.key" -> "value"),
          propsRemoved = Set("foo.key"))
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
        createUpdateTableWithProps(
          tablePath,
          propsAdded = Map("foo.key" -> "value"),
          propsRemoved = Set("foo.key"))
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
      createUpdateTableWithProps(
        tablePath,
        propsRemoved = Set("foo.key"))
      assertPropsDNE(tablePath, Set("foo.key"))
      // Check that the other property is not touched
      assertHasProp(tablePath, Map("FOO.KEY" -> "VALUE"))

      // Can unset a property that DNE
      createUpdateTableWithProps(
        tablePath,
        propsRemoved = Set("not.a.key"))
      assertPropsDNE(tablePath, Set("not.a.key"))
      // Check that the other property is not touched
      assertHasProp(tablePath, Map("FOO.KEY" -> "VALUE"))

      // Can be simultaneous with setTblProps as long as no overlap
      createUpdateTableWithProps(
        tablePath,
        propsAdded = Map("foo.key" -> "value-new"),
        propsRemoved = Set("FOO.KEY"))
      assertPropsDNE(tablePath, Set("FOO.KEY"))
      // Check that the other property is added successfully
      assertHasProp(tablePath, Map("foo.key" -> "value-new"))
    }
  }

  def createUpdateTableWithProps(
      tablePath: String,
      createTable: Boolean = false,
      propsAdded: Map[String, String] = null,
      propsRemoved: Set[String] = null): Unit = {
    val txn = if (createTable) {
      getCreateTxn(
        defaultEngine,
        tablePath,
        testSchema,
        tableProperties = propsAdded)
    } else {
      getUpdateTxn(
        defaultEngine,
        tablePath,
        tableProperties = propsAdded,
        tablePropertiesRemoved = propsRemoved)
    }
    txn.commit(defaultEngine, emptyIterable())
  }

  def assertHasProp(tablePath: String, expProps: Map[String, String]): Unit = {
    val snapshot = Table.forPath(defaultEngine, tablePath)
      .getLatestSnapshot(defaultEngine)
    expProps.foreach { case (key, value) =>
      assert(snapshot.getTableProperties.get(key) === value, key)
    }
  }

  def assertPropsDNE(tablePath: String, keys: Set[String]): Unit = {
    val metadata = getMetadata(defaultEngine, tablePath)
    assert(keys.forall(!metadata.getConfiguration.containsKey(_)))
  }
}
