/*
 * Copyright (2024) The Delta Lake Project Authors.
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

import org.apache.spark.annotation.Evolving
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.parser.DataTypeParser
import org.apache.spark.sql.types.{DataType, MetadataBuilder, StructField}

/**
 * :: Evolving ::
 *
 * Builder to specify a table column.
 *
 * See [[DeltaTableBuilder]] for examples.
 *
 * @since 2.5.0
 */
@Evolving
class DeltaColumnBuilder private[tables](private val colName: String) {
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
   * @since 2.5.0
   */
  @Evolving
  def dataType(dataType: String): DeltaColumnBuilder = {
    this.dataType = DataTypeParser.parseDataType(dataType)
    this
  }

  /**
   * :: Evolving ::
   *
   * Specify the column data type.
   *
   * @param dataType DataType column data type
   * @since 2.5.0
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
   * @since 2.5.0
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
   * @since 2.5.0
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
   * @since 2.5.0
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
   * @since 2.5.0
   */
  @Evolving
  def build(): StructField = {
    val metadataBuilder = new MetadataBuilder()
    if (generationExpr.nonEmpty) {
      metadataBuilder.putString("delta.generationExpression", generationExpr.get)
    }
    if (comment.nonEmpty) {
      metadataBuilder.putString("comment", comment.get)
    }
    val fieldMetadata = metadataBuilder.build()
    if (dataType == null) {
      // throw new AnalysisException(s"The data type of the column $colName is not provided")
    }
    StructField(
      colName,
      dataType,
      nullable = nullable,
      metadata = fieldMetadata)
  }
}
