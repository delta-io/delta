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
package io.delta.kernel.spark.utils

import scala.collection.immutable.Seq

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.types.StructType

/**
 * Helpers for constructing [[CatalogTable]] instances inside Java tests.
 *
 * Spark's [[CatalogTable]] is defined in Scala and its constructor signature shifts between Spark
 * releases. Centralising the construction in Scala keeps the kernel tests insulated from those
 * binary changes and saves Java tests from manually wiring the many optional parameters.
 */
object CatalogTableTestUtils {

  def catalogTableWithProperties(
      properties: java.util.Map[String, String],
      storageProperties: java.util.Map[String, String]): CatalogTable = {
    val scalaProps = ScalaUtils.toScalaMap(properties)
    val scalaStorageProps = ScalaUtils.toScalaMap(storageProperties)

    CatalogTable(
      identifier = TableIdentifier("tbl"),
      tableType = CatalogTableType.MANAGED,
      storage = CatalogStorageFormat(
        locationUri = None,
        inputFormat = None,
        outputFormat = None,
        serde = None,
        compressed = false,
        properties = scalaStorageProps),
      schema = new StructType(),
      provider = None,
      partitionColumnNames = Seq.empty,
      bucketSpec = None,
      properties = scalaProps)
  }

  def catalogTableWithNullStorage(
      properties: java.util.Map[String, String]): CatalogTable = {
    val scalaProps = ScalaUtils.toScalaMap(properties)

    CatalogTable(
      identifier = TableIdentifier("tbl"),
      tableType = CatalogTableType.MANAGED,
      storage = null,
      schema = new StructType(),
      provider = None,
      partitionColumnNames = Seq.empty,
      bucketSpec = None,
      properties = scalaProps)
  }
}
