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

import scala.collection.JavaConverters._
import scala.collection.immutable.Seq

import io.delta.kernel.exceptions.InvalidConfigurationValueException
import io.delta.kernel.internal.TableConfig
import io.delta.kernel.internal.util.ColumnMappingSuiteBase
import io.delta.kernel.types.{IntegerType, StringType, StructType}

class DeltaColumnMappingSuite extends DeltaTableWriteSuiteBase with ColumnMappingSuiteBase {

  val simpleTestSchema = new StructType()
    .add("a", StringType.STRING, true)
    .add("b", IntegerType.INTEGER, true)

  test("create table with unsupported column mapping mode") {
    withTempDirAndEngine { (tablePath, engine) =>
      val ex = intercept[InvalidConfigurationValueException] {
        val props = Map(TableConfig.COLUMN_MAPPING_MODE.getKey -> "invalid")
        createTable(engine, tablePath, simpleTestSchema, tableProperties = props)
      }
      assert(ex.getMessage.contains("Invalid value for table property " +
        "'delta.columnMapping.mode': 'invalid'. Needs to be one of: [none, id, name]."))
    }
  }

  test("create table with column mapping mode = none") {
    withTempDirAndEngine { (tablePath, engine) =>
      val props = Map(TableConfig.COLUMN_MAPPING_MODE.getKey -> "none")
      createTable(engine, tablePath, simpleTestSchema, tableProperties = props)

      assert(getMetadata(engine, tablePath).getSchema.equals(simpleTestSchema))
    }
  }

  test("cannot update table with unsupported column mapping mode") {
    withTempDirAndEngine { (tablePath, engine) =>
      createTable(engine, tablePath, simpleTestSchema)

      val ex = intercept[InvalidConfigurationValueException] {
        val props = Map(TableConfig.COLUMN_MAPPING_MODE.getKey -> "invalid")
        updateTable(engine, tablePath, tableProperties = props)
      }
      assert(ex.getMessage.contains("Invalid value for table property " +
        "'delta.columnMapping.mode': 'invalid'. Needs to be one of: [none, id, name]."))
    }
  }

  test("new table with column mapping mode = name") {
    withTempDirAndEngine { (tablePath, engine) =>
      val props = Map(TableConfig.COLUMN_MAPPING_MODE.getKey -> "name")
      createTable(engine, tablePath, simpleTestSchema, tableProperties = props)

      val structType = getMetadata(engine, tablePath).getSchema
      assertColumnMapping(structType.get("a"), 1)
      assertColumnMapping(structType.get("b"), 2)

      val protocol = getProtocol(engine, tablePath)
      assert(protocol.getMinReaderVersion == 2 && protocol.getMinWriterVersion == 7)
    }
  }

  test("new table with column mapping mode = id") {
    withTempDirAndEngine { (tablePath, engine) =>
      val props = Map(TableConfig.COLUMN_MAPPING_MODE.getKey -> "id")
      createTable(engine, tablePath, simpleTestSchema, tableProperties = props)

      val structType = getMetadata(engine, tablePath).getSchema
      assertColumnMapping(structType.get("a"), 1)
      assertColumnMapping(structType.get("b"), 2)

      val protocol = getProtocol(engine, tablePath)
      assert(protocol.getMinReaderVersion == 2 && protocol.getMinWriterVersion == 7)
    }
  }

  test("can update existing table to column mapping mode = name") {
    withTempDirAndEngine { (tablePath, engine) =>
      createTable(engine, tablePath, simpleTestSchema)
      val structType = getMetadata(engine, tablePath).getSchema
      assert(structType.equals(simpleTestSchema))

      val props = Map(TableConfig.COLUMN_MAPPING_MODE.getKey -> "name")
      updateTable(engine, tablePath, tableProperties = props)

      val updatedSchema = getMetadata(engine, tablePath).getSchema
      assertColumnMapping(updatedSchema.get("a"), 1, "a")
      assertColumnMapping(updatedSchema.get("b"), 2, "b")
    }
  }

  Seq("name", "id").foreach { startingCMMode =>
    test(s"cannot update table with unsupported column mapping mode change: $startingCMMode") {
      withTempDirAndEngine { (tablePath, engine) =>
        val props = Map(TableConfig.COLUMN_MAPPING_MODE.getKey -> startingCMMode)
        createTable(engine, tablePath, simpleTestSchema, tableProperties = props)

        val structType = getMetadata(engine, tablePath).getSchema
        assertColumnMapping(structType.get("a"), 1)
        assertColumnMapping(structType.get("b"), 2)

        val ex = intercept[IllegalArgumentException] {
          val props = Map(TableConfig.COLUMN_MAPPING_MODE.getKey -> "none")
          updateTable(engine, tablePath, tableProperties = props)
        }
        assert(ex.getMessage.contains(s"Changing column mapping mode " +
          s"from '$startingCMMode' to 'none' is not supported"))
      }
    }
  }

  test("cannot update column mapping mode from name to id on existing table") {
    withTempDirAndEngine { (tablePath, engine) =>
      val props = Map(TableConfig.COLUMN_MAPPING_MODE.getKey -> "name")
      createTable(engine, tablePath, simpleTestSchema, tableProperties = props)

      val structType = getMetadata(engine, tablePath).getSchema
      assertColumnMapping(structType.get("a"), 1)
      assertColumnMapping(structType.get("b"), 2)

      val ex = intercept[IllegalArgumentException] {
        val props = Map(TableConfig.COLUMN_MAPPING_MODE.getKey -> "id")
        updateTable(engine, tablePath, tableProperties = props)
      }
      assert(ex.getMessage.contains("Changing column mapping mode " +
        "from 'name' to 'id' is not supported"))
    }
  }

  test("cannot update column mapping mode from none to id on existing table") {
    withTempDirAndEngine { (tablePath, engine) =>
      createTable(engine, tablePath, simpleTestSchema)

      val structType = getMetadata(engine, tablePath).getSchema
      assert(structType.equals(simpleTestSchema))

      val ex = intercept[IllegalArgumentException] {
        val props = Map(TableConfig.COLUMN_MAPPING_MODE.getKey -> "id")
        updateTable(engine, tablePath, tableProperties = props)
      }
      assert(ex.getMessage.contains("Changing column mapping mode " +
        "from 'none' to 'id' is not supported"))
    }
  }

  test("update table properties on a column mapping enabled table") {
    withTempDirAndEngine { (tablePath, engine) =>
      val props = Map(TableConfig.COLUMN_MAPPING_MODE.getKey -> "name")
      createTable(engine, tablePath, simpleTestSchema, tableProperties = props)

      val metadata = getMetadata(engine, tablePath)
      assertColumnMapping(metadata.getSchema.get("a"), 1)
      assertColumnMapping(metadata.getSchema.get("b"), 2)

      val newProps = Map("key" -> "value")
      updateTable(engine, tablePath, tableProperties = newProps)

      assert(getMetadata(engine, tablePath).getConfiguration.get("key") == "value")
    }
  }

  Seq(true, false).foreach { withIcebergCompatV2 =>
    test(s"new table with column mapping mode = name and nested schema, " +
      s"enableIcebergCompatV2 = $withIcebergCompatV2") {
      withTempDirAndEngine { (tablePath, engine) =>
        val props = Map(
          TableConfig.COLUMN_MAPPING_MODE.getKey -> "name",
          TableConfig.ICEBERG_COMPAT_V2_ENABLED.getKey -> withIcebergCompatV2.toString)

        createTable(engine, tablePath, cmTestSchema(), tableProperties = props)

        verifyCMTestSchemaHasValidColumnMappingInfo(
          getMetadata(engine, tablePath),
          isNewTable = true,
          enableIcebergComaptV2 = withIcebergCompatV2)
      }
    }
  }
}
