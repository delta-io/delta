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

package org.apache.spark.sql.delta.catalog

import scala.collection.JavaConverters._

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.connector.catalog.{Table, TableCapability}
import org.apache.spark.sql.types.StructType

/** A place holder used to resolve Iceberg table as a relation during analysis */
case class IcebergTablePlaceHolder(tableIdentifier: TableIdentifier) extends Table {

  override def name(): String = tableIdentifier.unquotedString

  override def schema(): StructType = new StructType()

  override def capabilities(): java.util.Set[TableCapability] = Set.empty[TableCapability].asJava
}
