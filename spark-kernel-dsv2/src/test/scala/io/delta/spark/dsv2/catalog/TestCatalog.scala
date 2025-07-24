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
package io.delta.spark.dsv2.catalog

import java.util

import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * A [[TableCatalog]] implementation that is only used for facilitating testing for spark-dsv2
 * code path.
 */
class TestCatalog extends TableCatalog {

  private var catalogName: String = _

  override def listTables(namespace: Array[String]): Array[Identifier] = {
    throw new UnsupportedOperationException("listTables method is not implemented")
  }

  override def loadTable(ident: Identifier): Table = {
    throw new UnsupportedOperationException("loadTable method is not implemented")
  }

  override def createTable(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: util.Map[String, String]): Table = {
    throw new UnsupportedOperationException("createTable method is not implemented")
  }

  override def alterTable(ident: Identifier, changes: TableChange*): Table = {
    throw new UnsupportedOperationException("alterTable method is not implemented")
  }

  override def dropTable(ident: Identifier): Boolean = {
    throw new UnsupportedOperationException("dropTable method is not implemented")
  }

  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit = {
    throw new UnsupportedOperationException("renameTable method is not implemented")
  }

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    this.catalogName = name
  }

  override def name(): String = catalogName
}
