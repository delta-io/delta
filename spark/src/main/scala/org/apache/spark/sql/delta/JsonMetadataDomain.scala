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

package org.apache.spark.sql.delta

import org.apache.spark.sql.delta.actions.DomainMetadata
import org.apache.spark.sql.delta.util.JsonUtils

/**
 * A trait for capturing metadata domain of type T.
 */
trait JsonMetadataDomain[T] {
  val domainName: String

  /**
   * Creates [[DomainMetadata]] with configuration set as a JSON-serialized value of
   * the metadata domain of type T.
   */
  def toDomainMetadata[T: Manifest]: DomainMetadata =
    DomainMetadata(domainName, JsonUtils.toJson(this.asInstanceOf[T]), removed = false)
}

abstract class JsonMetadataDomainUtils[T: Manifest] {
  protected val domainName: String

  /**
   * Returns the metadata domain's configuration as type T for domain metadata that
   * matches "domainName" in the given snapshot. Returns None if there is no matching
   * domain metadata.
   */
  def fromSnapshot(snapshot: Snapshot): Option[T] = {
    snapshot.domainMetadata
      .find(_.domain == domainName)
      .map(m => fromJsonConfiguration(m))
  }

  protected def fromJsonConfiguration(domain: DomainMetadata): T =
    JsonUtils.fromJson[T](domain.configuration)
}

