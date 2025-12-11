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
package io.delta.kernel.spark.snapshot

import java.net.URI

import io.delta.storage.commit.uccommitcoordinator.UCCommitCoordinatorClient
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.scalatest.funsuite.AnyFunSuite

class DeltaSnapshotManagerFactorySuite extends AnyFunSuite {

  private def nonUcTable(location: String): CatalogTable = {
    CatalogTable(
      identifier = TableIdentifier("tbl", Some("default")),
      tableType = CatalogTableType.EXTERNAL,
      storage = CatalogStorageFormat.empty.copy(locationUri = Some(new URI(location))),
      schema = new org.apache.spark.sql.types.StructType(),
      provider = Some("delta"))
  }

  private def ucTable(location: String, tableId: String): CatalogTable = {
    CatalogTable(
      identifier = TableIdentifier("tbl", Some("uc"), Some("main")),
      tableType = CatalogTableType.EXTERNAL,
      storage = CatalogStorageFormat.empty.copy(
        locationUri = Some(new URI(location)),
        properties = Map(
          UCCommitCoordinatorClient.UC_TABLE_ID_KEY -> tableId,
          "delta.feature.catalogManaged" -> "supported")),
      schema = new org.apache.spark.sql.types.StructType(),
      provider = Some("delta"))
  }

  test("fromPath returns path-based manager") {
    val mgr = DeltaSnapshotManagerFactory.fromPath("/tmp/test", new Configuration())
    assert(mgr.isInstanceOf[PathBasedSnapshotManager])
  }

  test("fromCatalogTable falls back to path-based for non-UC tables") {
    val spark = SparkSession.builder().master("local[1]").appName("factory-non-uc").getOrCreate()
    try {
      val table = nonUcTable("file:/tmp/non-uc")
      val mgr = DeltaSnapshotManagerFactory.fromCatalogTable(table, spark, new Configuration())
      assert(mgr.isInstanceOf[PathBasedSnapshotManager])
    } finally {
      spark.stop()
    }
  }

  test("fromCatalogTable throws when UC table is missing UC config") {
    val spark = SparkSession.builder().master("local[1]").appName("factory-uc-missing-config").getOrCreate()
    try {
      val table = ucTable("file:/tmp/uc", tableId = "abc123")
      assertThrows[IllegalArgumentException] {
        DeltaSnapshotManagerFactory.fromCatalogTable(table, spark, new Configuration())
      }
    } finally {
      spark.stop()
    }
  }

  // Null parameter validation tests

  test("fromPath throws on null tablePath") {
    assertThrows[NullPointerException] {
      DeltaSnapshotManagerFactory.fromPath(null, new Configuration())
    }
  }

  test("fromPath throws on null hadoopConf") {
    assertThrows[NullPointerException] {
      DeltaSnapshotManagerFactory.fromPath("/tmp/test", null)
    }
  }

  test("fromCatalogTable throws on null catalogTable") {
    val spark = SparkSession.builder().master("local[1]").appName("factory-null-table").getOrCreate()
    try {
      assertThrows[NullPointerException] {
        DeltaSnapshotManagerFactory.fromCatalogTable(null, spark, new Configuration())
      }
    } finally {
      spark.stop()
    }
  }

  test("fromCatalogTable throws on null spark") {
    val table = nonUcTable("file:/tmp/test")
    assertThrows[NullPointerException] {
      DeltaSnapshotManagerFactory.fromCatalogTable(table, null, new Configuration())
    }
  }

  test("fromCatalogTable throws on null hadoopConf") {
    val spark = SparkSession.builder().master("local[1]").appName("factory-null-conf").getOrCreate()
    try {
      val table = nonUcTable("file:/tmp/test")
      assertThrows[NullPointerException] {
        DeltaSnapshotManagerFactory.fromCatalogTable(table, spark, null)
      }
    } finally {
      spark.stop()
    }
  }

  // NOTE: Testing fromCatalogTable returning CatalogManagedSnapshotManager for valid UC tables
  // requires full SparkSession integration with UC catalog configuration (UCSingleCatalog,
  // endpoint, token). This is covered by integration tests rather than unit tests.
}
