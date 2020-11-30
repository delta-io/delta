/*
 * Copyright (2020) The Delta Lake Project Authors.
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

package org.apache.spark.sql.delta.catalog

import java.net.URI

import io.delta.hive.{DeltaInputFormat, DeltaOutputFormat, DeltaStorageHandler}
import org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogUtils}
import org.apache.spark.sql.execution.datasources.DataSource

class HiveDeltaCatalog extends DeltaCatalog {

  override protected def getProvider : String = "hive"

  override protected def getStorage(tableProperties : Map[String, String],
                                    locationUri : Option[URI]) : CatalogStorageFormat = {
    val properties : Map[String, String] = if (locationUri.isDefined) {
      Map("path" -> CatalogUtils.URIToString(locationUri.get))
    } else {
      Map()
    }
    DataSource.buildStorageFormatFromOptions(tableProperties)
      .copy(locationUri = locationUri,
        serde = Some(classOf[ParquetHiveSerDe].getCanonicalName),
        inputFormat = Some(classOf[DeltaInputFormat].getCanonicalName),
        outputFormat = Some(classOf[DeltaOutputFormat].getCanonicalName),
        properties = properties)
  }


  override protected def getTableProperties(
                      tableProperties : Map[String, String]) : Map[String, String] =
    tableProperties +
      ("storage_handler" -> classOf[DeltaStorageHandler].getCanonicalName)
}
