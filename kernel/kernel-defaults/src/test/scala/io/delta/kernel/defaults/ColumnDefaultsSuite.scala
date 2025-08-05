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

import scala.collection.immutable.Seq

import io.delta.kernel.TransactionCommitResult
import io.delta.kernel.defaults.utils.WriteUtils
import io.delta.kernel.engine.Engine
import io.delta.kernel.exceptions.KernelException
import io.delta.kernel.expressions.{Column, Literal}
import io.delta.kernel.internal.util.Clock
import io.delta.kernel.types._

import org.scalatest.funsuite.AnyFunSuite

class ColumnDefaultsSuite extends AnyFunSuite with WriteUtils {
  private val schemaWithDefault = new StructType()
    .add("a", StringType.STRING, true, fieldMeta(1, null))
    .add("b", IntegerType.INTEGER, true, fieldMeta(2, "127"))

  private def fieldMeta(fieldId: Int, defaultVal: String) = {
    var builder = FieldMetadata.builder()
    if (fieldId != -1) {
      builder = builder
        .putLong("delta.columnMapping.id", fieldId)
        .putString("delta.columnMapping.physicalName", s"col-$fieldId")
    }
    if (defaultVal != null) {
      builder = builder.putString("CURRENT_DEFAULT", defaultVal)
    }
    builder.build()
  }

  val goodTblProperties = Map(
    "delta.feature.allowColumnDefaults" -> "supported",
    "delta.enableIcebergCompatV3" -> "true",
    "delta.columnMapping.mode" -> "id")

  test("allow default value in schema when the table feature is enabled") {
    withTempDirAndEngine { (tablePath, engine) =>
      createEmptyTable(
        engine,
        tablePath,
        schemaWithDefault,
        tableProperties = goodTblProperties)
    }
    withTempDirAndEngine { (tablePath, engine) =>
      createEmptyTable(
        engine,
        tablePath,
        schemaWithDefault,
        tableProperties = Map(
          "delta.feature.allowColumnDefaults" -> "supported",
          "delta.enableIcebergCompatV3" -> "true"))
    }
    withTempDirAndEngine { (tablePath, engine) =>
      val e = intercept[KernelException] {
        createEmptyTable(
          engine,
          tablePath,
          schemaWithDefault,
          tableProperties = Map(
            "delta.feature.allowColumnDefaults" -> "supported"))
      }
      assert(e.getMessage ==
        "This table does not enable table features for setting column defaults")
    }
    withTempDirAndEngine { (tablePath, engine) =>
      val e = intercept[KernelException] {
        createEmptyTable(engine, tablePath, schemaWithDefault)
      }
      assert(e.getMessage ==
        "This table does not enable table features for setting column defaults")
    }
  }

  test("block writing to tables with default values") {
    withTempDirAndEngine { (tablePath, engine) =>
      createEmptyTable(
        engine,
        tablePath,
        schemaWithDefault,
        tableProperties = goodTblProperties)
      val e = intercept[UnsupportedOperationException] {
        appendData(engine, tablePath, data = Seq(Map.empty[String, Literal] -> dataBatches1))
      }
      assert(e.getMessage == "Writing into column mapping enabled table is not supported yet.")
    }
  }

  Seq(
    ("remove col", new StructType().add("a", StringType.STRING, true, fieldMeta(1, null))),
    (
      "add col",
      new StructType()
        .add("a", StringType.STRING, true, fieldMeta(1, null))
        .add("add1", StringType.STRING, true, fieldMeta(3, "'Tom'"))
        .add("add2", DateType.DATE, true, fieldMeta(4, "2025-01-01"))
        .add("add3", DoubleType.DOUBLE, true, fieldMeta(5, "'3.21'"))
        .add("b", IntegerType.INTEGER, true, fieldMeta(2, "127"))),
    (
      "update value",
      new StructType()
        .add("a", StringType.STRING, true, fieldMeta(1, null))
        .add("b", IntegerType.INTEGER, true, fieldMeta(2, "350"))),
    (
      "rename column",
      new StructType()
        .add("a", StringType.STRING, true, fieldMeta(1, null))
        .add("xxx", IntegerType.INTEGER, true, fieldMeta(2, "350"))),
    (
      "add renamed column",
      new StructType()
        .add("a", StringType.STRING, true, fieldMeta(1, null))
        .add("b", LongType.LONG, true, fieldMeta(220, "350")))).foreach { case (name, schema) =>
    test(s"allow valid default values - $name") {
      // Create tables
      withTempDirAndEngine { (tablePath, engine) =>
        createEmptyTable(
          engine,
          tablePath,
          schema,
          tableProperties = goodTblProperties)
      }
      // Schema Evolutions
      withTempDirAndEngine { (tablePath, engine) =>
        createEmptyTable(
          engine,
          tablePath,
          schemaWithDefault,
          tableProperties = goodTblProperties)
        updateTableMetadata(engine, tablePath, schema)
      }
    }
  }

  Seq(
    (StringType.STRING, "CURRENT_TIMESTAMP()"),
    (IntegerType.INTEGER, "313.55"),
    (DoubleType.DOUBLE, "Good boy"),
    (new DecimalType(10, 5), "234243243243243234.234"),
    (DateType.DATE, "2022/01/05"),
    (TimestampType.TIMESTAMP, "2025-01-01"),
    (TimestampNTZType.TIMESTAMP_NTZ, "2025-01-01T00:00:00+02:00")).foreach {
    case (dataType, value) =>
      test(s"block invalid default values - $value") {
        // Create tables
        val schema = new StructType().add("col1", dataType, true, fieldMeta(100, value))
        withTempDirAndEngine { (tablePath, engine) =>
          intercept[KernelException] {
            createEmptyTable(
              engine,
              tablePath,
              schema,
              tableProperties = goodTblProperties)
          }
        }
        // Schema Evolutions -> change type
        withTempDirAndEngine { (tablePath, engine) =>
          createEmptyTable(
            engine,
            tablePath,
            schemaWithDefault,
            tableProperties = goodTblProperties)
          intercept[KernelException] {
            val schema = new StructType()
              .add("a", StringType.STRING, true, fieldMeta(1, null))
              .add("b", IntegerType.INTEGER, true, fieldMeta(2, null))
              .add("col1", dataType, true, fieldMeta(100, value))
            updateTableMetadata(engine, tablePath, schema)
          }
          intercept[KernelException] {
            val schema = new StructType().add("col1", dataType, true, fieldMeta(3, value))
            updateTableMetadata(engine, tablePath, schema)
          }
        }
      }
  }
}
