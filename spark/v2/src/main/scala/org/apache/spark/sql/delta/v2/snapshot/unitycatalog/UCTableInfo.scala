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
package org.apache.spark.sql.delta.v2.snapshot.unitycatalog

import java.util.{Collections, Map => JMap}
import java.util.Objects.requireNonNull

import scala.collection.JavaConverters._
import scala.collection.mutable

import io.delta.storage.commit.uccommitcoordinator.UCConfigUtils
import io.delta.kernel.unitycatalog.{UCTableIdentifier => KernelUCTableIdentifier}

/**
 * Table information for Unity Catalog managed tables.
 *
 * This object encapsulates all the information needed to interact with a Unity Catalog table
 * without requiring Spark dependencies.
 */
private[delta]
final class UCTableInfo(
    tableId: String,
    tablePath: String,
    kernelTableIdentifier: KernelUCTableIdentifier,
    ucUri: String,
    authConfig: JMap[String, String],
    optInFlags: JMap[String, String]) {

  private val validatedTableId = requireNonNull(tableId, "tableId is null")
  private val validatedTablePath = requireNonNull(tablePath, "tablePath is null")
  private val validatedTableIdentifier =
    requireNonNull(kernelTableIdentifier, "tableIdentifier is null")
  private val validatedUcUri = requireNonNull(ucUri, "ucUri is null")
  private val validatedAuthConfig =
    Collections.unmodifiableMap[String, String](requireNonNull(authConfig, "authConfig is null"))
  private val validatedOptInFlags =
    Collections.unmodifiableMap[String, String](requireNonNull(optInFlags, "optInFlags is null"))

  def this(
      tableId: String,
      tablePath: String,
      kernelTableIdentifier: KernelUCTableIdentifier,
      ucUri: String,
      authConfig: JMap[String, String]) = {
    this(
      tableId = tableId,
      tablePath = tablePath,
      kernelTableIdentifier = kernelTableIdentifier,
      ucUri = ucUri,
      authConfig = authConfig,
      optInFlags = Collections.emptyMap[String, String]())
  }

  def getTableId(): String = validatedTableId

  def getTablePath(): String = validatedTablePath

  def getTableIdentifier(): KernelUCTableIdentifier = validatedTableIdentifier

  def getUcUri(): String = validatedUcUri

  def getAuthConfig(): JMap[String, String] = validatedAuthConfig

  def getOptInFlags(): JMap[String, String] = validatedOptInFlags

  /**
   * Builds a flat config map suitable for `UCTokenBasedRestClientFactory.createUCClient`.
   * Re-adds the `auth.` prefix to auth config keys, includes `uri`, and forwards any
   * per-catalog opt-in flags ([[UCConfigUtils.DELTA_REST_API_ENABLED_KEY]] etc.) so the factory
   * picks the correct client implementation.
   */
  def toUcConfig(): JMap[String, String] = {
    val ucConfig = mutable.HashMap.empty[String, String]
    ucConfig.put(UCConfigUtils.URI_KEY, validatedUcUri)
    validatedAuthConfig.forEach { (key, value) =>
      ucConfig.put(UCConfigUtils.AUTH_PREFIX + key, value)
    }
    validatedOptInFlags.forEach { (key, value) => ucConfig.put(key, value) }
    ucConfig.asJava
  }
}
