/*
 * Copyright (2026) The Delta Lake Project Authors.
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
package org.apache.spark.sql.delta.util

import org.apache.spark.sql.delta.DeltaTableUtils
import org.apache.spark.sql.delta.sources.DeltaSQLConf

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogTable

/**
 * Shared helpers for deriving Hadoop filesystem
 * options from DataFrame options and catalog table
 * storage properties.
 */
private[delta] object DeltaFileSystemOptions {

  /**
   * Extracts file-system-relevant storage properties
   * from a catalog table, filtering to keys that match
   * [[DeltaTableUtils.validDeltaTableHadoopPrefixes]].
   */
  def extractCatalogTableFsOptions(
      catalogTableOpt: Option[CatalogTable])
      : Map[String, String] = {
    catalogTableOpt
      .map(_.storage.properties.filter {
        case (k, _) =>
          DeltaTableUtils
            .validDeltaTableHadoopPrefixes
            .exists(k.startsWith)
      })
      .getOrElse(Map.empty)
  }

  /**
   * Constructs filesystem options by combining catalog
   * table storage properties with DataFrame reader/writer
   * options. Only keys matching valid Hadoop prefixes are
   * picked up so that parquet or json options are not
   * passed to the code that reads Delta transaction logs.
   *
   * Gated by
   * [[DeltaSQLConf.LOAD_FILE_SYSTEM_CONFIGS_FROM_DATAFRAME_OPTIONS]].
   */
  def fileSystemOptionsFromDataFrameOptions(
      spark: SparkSession,
      options: Map[String, String],
      catalogTableOpt: Option[CatalogTable] = None)
      : Map[String, String] = {
    val catalogStorageProps =
      extractCatalogTableFsOptions(catalogTableOpt)
    if (spark.sessionState.conf.getConf(
        DeltaSQLConf
          .LOAD_FILE_SYSTEM_CONFIGS_FROM_DATAFRAME_OPTIONS)) {
      catalogStorageProps ++ options.filterKeys {
        key =>
          DeltaTableUtils
            .validDeltaTableHadoopPrefixes
            .exists(key.startsWith)
      }.toMap
    } else {
      catalogStorageProps
    }
  }
}
