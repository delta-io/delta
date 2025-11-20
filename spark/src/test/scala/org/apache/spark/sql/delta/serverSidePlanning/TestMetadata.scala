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

package org.apache.spark.sql.delta.serverSidePlanning

/**
 * Test implementation of ServerSidePlanningMetadata with injectable values.
 * Used in unit tests to mock UC metadata without a real UC instance.
 */
case class TestMetadata(
    catalogName: String,
    endpointUri: String,
    token: String,
    ucUri: String = "",
    ucToken: String = "",
    props: Map[String, String] = Map.empty) extends ServerSidePlanningMetadata {

  override def planningEndpointUri: Option[String] = Some(endpointUri)
  override def authToken: Option[String] = Some(token)
  override def unityCatalogUri: Option[String] = if (ucUri.nonEmpty) Some(ucUri) else None
  override def unityCatalogToken: Option[String] = if (ucToken.nonEmpty) Some(ucToken) else None
  override def tableProperties: Map[String, String] = props
}
