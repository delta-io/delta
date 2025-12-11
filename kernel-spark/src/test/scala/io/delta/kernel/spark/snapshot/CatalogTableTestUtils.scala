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

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.types.StructType

/** Test helpers for constructing Spark catalog metadata. */
object CatalogTableTestUtils {

  def createMockCatalogTable(catalogName: String): CatalogTable = {
    CatalogTable(
      identifier = TableIdentifier("test_table", Some("test_schema"), Some(catalogName)),
      tableType = CatalogTableType.MANAGED,
      storage = CatalogStorageFormat(
        locationUri = Some(new Path("/test/location").toUri),
        inputFormat = None,
        outputFormat = None,
        serde = None,
        compressed = false,
        properties = Map.empty
      ),
      schema = new StructType()
    )
  }
}
