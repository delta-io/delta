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
package io.delta.kernel.spark.catalog;

import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.catalog.CatalogStorageFormat;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.catalog.CatalogTableType;
import org.apache.spark.sql.types.StructType;
import scala.Option;

/** Utility class for SparkTable tests. */
public class SparkTableTestUtils {

  /**
   * Creates a CatalogTable from a file system path.
   *
   * <p>Simplified implementation inspired by Unity Catalog.
   *
   * @param tableName the name of the table
   * @param path the file system path to the Delta table
   * @return a CatalogTable instance
   */
  public static CatalogTable createCatalogTableFromPath(String tableName, String path) {
    scala.collection.immutable.Map<String, String> emptyMap =
        scala.collection.immutable.Map$.MODULE$.empty();
    scala.collection.immutable.Seq<String> emptySeq =
        scala.collection.immutable.Seq$.MODULE$.empty();

    return CatalogTable.apply(
        new TableIdentifier(tableName, Option.apply("default")),
        CatalogTableType.EXTERNAL(),
        CatalogStorageFormat.apply(
            Option.apply(new java.io.File(path).toURI()),
            Option.empty(),
            Option.empty(),
            Option.empty(),
            false,
            emptyMap),
        new StructType(),
        Option.apply("delta"),
        emptySeq,
        Option.empty(),
        "",
        System.currentTimeMillis(),
        -1L,
        "",
        emptyMap,
        Option.empty(),
        Option.empty(),
        Option.empty(),
        emptySeq,
        false,
        true,
        emptyMap,
        Option.empty());
  }
}
