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
package org.apache.spark.sql.delta.util

import org.apache.spark.sql.delta.DeltaTableUtils
import org.apache.spark.sql.delta.sources.DeltaSQLConf

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogTable

/**
 * Shared helpers for deriving Hadoop filesystem options from DataFrame options and catalog table
 * storage properties.
 */
private[delta] object DeltaFileSystemOptions {

  /**
   * Retains only entries whose key starts with a recognised Hadoop filesystem
   * prefix ([[DeltaTableUtils.validDeltaTableHadoopPrefixes]]).
   */
  private[delta] def filterHadoopOptions(
      options: Map[String, String]): Map[String, String] = {
    options.filter { case (k, _) =>
      DeltaTableUtils.validDeltaTableHadoopPrefixes.exists(k.startsWith)
    }
  }

  /**
   * Extracts file-system-relevant storage properties from a catalog table.
   */
  def extractCatalogTableFsOptions(
      catalogTableOpt: Option[CatalogTable]): Map[String, String] = {
    catalogTableOpt
      .map(ct => filterHadoopOptions(ct.storage.properties))
      .getOrElse(Map.empty)
  }

  /**
   * Constructs filesystem options by combining catalog table storage properties with DataFrame
   * reader/writer options. Only keys matching valid Hadoop prefixes are picked up so that parquet
   * or json options are not passed to the code that reads Delta transaction logs.
   *
   * Gated by [[DeltaSQLConf.LOAD_FILE_SYSTEM_CONFIGS_FROM_DATAFRAME_OPTIONS]].
   */
  def buildFsOptions(
      spark: SparkSession,
      options: Map[String, String],
      catalogTableOpt: Option[CatalogTable] = None): Map[String, String] = {
    val catalogStorageProps = extractCatalogTableFsOptions(catalogTableOpt)
    if (spark.sessionState.conf.getConf(
        DeltaSQLConf.LOAD_FILE_SYSTEM_CONFIGS_FROM_DATAFRAME_OPTIONS)) {
      catalogStorageProps ++ filterHadoopOptions(options)
    } else {
      catalogStorageProps
    }
  }
}
