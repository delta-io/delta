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

import io.delta.kernel.spark.utils.CatalogTableTestUtils

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.test.SharedSparkSession

/** Tests for [[DeltaSnapshotManagerFactory]]. */
class DeltaSnapshotManagerFactorySuite extends SparkFunSuite with SharedSparkSession {

  private def createTestCatalogTable() = {
    CatalogTableTestUtils.createCatalogTable(locationUri = Some(new URI("s3://test/path")))
  }

  test("fromPath throws NullPointerException for null tablePath") {
    assertThrows[NullPointerException] {
      DeltaSnapshotManagerFactory.fromPath(null, new Configuration())
    }
  }

  test("fromPath throws NullPointerException for null hadoopConf") {
    assertThrows[NullPointerException] {
      DeltaSnapshotManagerFactory.fromPath("/tmp/test", null)
    }
  }

  test("fromCatalogTable throws NullPointerException for null catalogTable") {
    assertThrows[NullPointerException] {
      DeltaSnapshotManagerFactory.fromCatalogTable(null, spark, new Configuration())
    }
  }

  test("fromCatalogTable throws NullPointerException for null spark") {
    val table = createTestCatalogTable()
    assertThrows[NullPointerException] {
      DeltaSnapshotManagerFactory.fromCatalogTable(table, null, new Configuration())
    }
  }

  test("fromCatalogTable throws NullPointerException for null hadoopConf") {
    val table = createTestCatalogTable()
    assertThrows[NullPointerException] {
      DeltaSnapshotManagerFactory.fromCatalogTable(table, spark, null)
    }
  }

  test("fromCatalogTable throws UnsupportedOperationException (skeleton)") {
    val table = createTestCatalogTable()
    assertThrows[UnsupportedOperationException] {
      DeltaSnapshotManagerFactory.fromCatalogTable(table, spark, new Configuration())
    }
  }
}
