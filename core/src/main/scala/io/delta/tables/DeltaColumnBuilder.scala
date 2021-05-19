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

package io.delta.tables

import org.apache.spark.sql.delta.sources.DeltaSourceUtils.GENERATION_EXPRESSION_METADATA_KEY

import org.apache.spark.annotation._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataType, MetadataBuilder, StructField}

/**
 * :: Evolving ::
 *
 * Builder to specify a table column.
 *
 * See [[DeltaTableBuilder]] for examples.
 * @since 1.0.0
 */
@Evolving
class DeltaColumnBuilder private[tables](
    private val spark: SparkSession,
    private val colName: String) {
  private var dataType: DataType = _
  private var nullable: Boolean = true
  private var generationExpr: Option[String] = None
  private var comment: Option[String] = None

  /**
   * :: Evolving ::
   *
   * Specify the column data type.
   *
   * @param dataType string column data type
   * @since 1.0.0
   */
  @Evolving
  def dataType(dataType: String): DeltaColumnBuilder = {
    this.dataType = spark.sessionState.sqlParser.parseDataType(dataType)
    this
  }

  /**
   * :: Evolving ::
   *
   * Specify the column data type.
   *
   * @param dataType DataType column data type
   * @since 1.0.0
   */
  @Evolving
  def dataType(dataType: DataType): DeltaColumnBuilder = {
    this.dataType = dataType
    this
  }

  /**
   * :: Evolving ::
   *
   * Specify whether the column can be null.
   *
   * @param nullable boolean whether the column can be null or not.
   * @since 1.0.0
   */
  @Evolving
  def nullable(nullable: Boolean): DeltaColumnBuilder = {
    this.nullable = nullable
    this
  }

  /**
   * :: Evolving ::
   *
   * Specify a expression if the column is always generated as a function of other columns.
   *
   * @param expr string the the generation expression
   * @since 1.0.0
   */
  @Evolving
  def generatedAlwaysAs(expr: String): DeltaColumnBuilder = {
    this.generationExpr = Option(expr)
    this
  }

  /**
   * :: Evolving ::
   *
   * Specify a column comment.
   *
   * @param comment string column description
   * @since 1.0.0
   */
  @Evolving
  def comment(comment: String): DeltaColumnBuilder = {
    this.comment = Option(comment)
    this
  }

  /**
   * :: Evolving ::
   *
   * Build the column as a structField.
   *
   * @since 1.0.0
   */
  @Evolving
  def build(): StructField = {
    val metadataBuilder = new MetadataBuilder()
    if (generationExpr.nonEmpty) {
      metadataBuilder.putString(GENERATION_EXPRESSION_METADATA_KEY, generationExpr.get)
    }
    if (comment.nonEmpty) {
      metadataBuilder.putString("comment", comment.get)
    }
    val fieldMetadata = metadataBuilder.build()
    StructField(
      colName,
      dataType,
      nullable = nullable,
      metadata = fieldMetadata)
  }
}
