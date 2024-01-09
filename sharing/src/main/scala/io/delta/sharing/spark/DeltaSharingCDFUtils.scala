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

package io.delta.sharing.spark

import java.lang.ref.WeakReference
import java.nio.charset.StandardCharsets.UTF_8

import org.apache.spark.sql.delta.catalog.DeltaTableV2
import com.google.common.hash.Hashing
import io.delta.sharing.client.DeltaSharingClient
import io.delta.sharing.client.model.{Table => DeltaSharingTable}
import org.apache.hadoop.fs.Path

import org.apache.spark.delta.sharing.CachedTableManager
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.BaseRelation

object DeltaSharingCDFUtils extends Logging {

  private def getDuration(start: Long): Double = {
    (System.currentTimeMillis() - start) / 1000.0
  }

  private[sharing] def prepareCDCRelation(
      sqlContext: SQLContext,
      options: DeltaSharingOptions,
      table: DeltaSharingTable,
      client: DeltaSharingClient): BaseRelation = {
    val startTime = System.currentTimeMillis()
    // 1. Get all files with DeltaSharingClient.
    // includeHistoricalMetadata is always set to true, to get the metadata at the startingVersion
    // and also any metadata changes between [startingVersion, endingVersion], to put them in the
    // delta log. This is to allow delta library to check the metadata change and handle it
    // properly -- currently it throws error for column mapping changes.
    val deltaTableFiles =
      client.getCDFFiles(table, options.cdfOptions, includeHistoricalMetadata = true)
    logInfo(
      s"Fetched ${deltaTableFiles.lines.size} lines with cdf options ${options.cdfOptions} " +
      s"for table ${table} from delta sharing server, took ${getDuration(startTime)}s."
    )

    val path = options.options.getOrElse("path", throw DeltaSharingErrors.pathNotSpecifiedException)
    // 2. Prepare local delta log
    val queryCustomTablePath = client.getProfileProvider.getCustomTablePath(path)
    val queryParamsHashId = DeltaSharingUtils.getQueryParamsHashId(options.cdfOptions)
    val tablePathWithHashIdSuffix =
      DeltaSharingUtils.getTablePathWithIdSuffix(queryCustomTablePath, queryParamsHashId)
    val deltaLogMetadata = DeltaSharingLogFileSystem.constructLocalDeltaLogAcrossVersions(
      lines = deltaTableFiles.lines,
      customTablePath = tablePathWithHashIdSuffix,
      startingVersionOpt = None,
      endingVersionOpt = None
    )

    // 3. Register parquet file id to url mapping
    CachedTableManager.INSTANCE.register(
      // Using path instead of queryCustomTablePath because it will be customized within
      // CachedTableManager.
      tablePath = DeltaSharingUtils.getTablePathWithIdSuffix(path, queryParamsHashId),
      idToUrl = deltaLogMetadata.idToUrl,
      refs = Seq(new WeakReference(this)),
      profileProvider = client.getProfileProvider,
      refresher = DeltaSharingUtils.getRefresherForGetCDFFiles(
        client = client,
        table = table,
        cdfOptions = options.cdfOptions
      ),
      expirationTimestamp =
        if (CachedTableManager.INSTANCE
            .isValidUrlExpirationTime(deltaLogMetadata.minUrlExpirationTimestamp)) {
          deltaLogMetadata.minUrlExpirationTimestamp.get
        } else {
          System.currentTimeMillis() + CachedTableManager.INSTANCE.preSignedUrlExpirationMs
        },
      refreshToken = None
    )

    // 4. return Delta
    val localDeltaCdfOptions = Map(
      DeltaSharingOptions.CDF_START_VERSION -> deltaLogMetadata.minVersion.toString,
      DeltaSharingOptions.CDF_END_VERSION -> deltaLogMetadata.maxVersion.toString,
      DeltaSharingOptions.CDF_READ_OPTION -> "true"
    )
    DeltaTableV2(
      spark = sqlContext.sparkSession,
      path = DeltaSharingLogFileSystem.encode(tablePathWithHashIdSuffix),
      options = localDeltaCdfOptions
    ).toBaseRelation
  }
}
