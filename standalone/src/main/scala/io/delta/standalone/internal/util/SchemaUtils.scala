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

package io.delta.standalone.internal.util

import io.delta.standalone.internal.exception.DeltaErrors

private[internal] object SchemaUtils {

  /**
   * Verifies that the column names are acceptable by Parquet and henceforth Delta. Parquet doesn't
   * accept the characters ' ,;{}()\n\t'. We ensure that neither the data columns nor the partition
   * columns have these characters.
   */
  def checkFieldNames(names: Seq[String]): Unit = {
    ParquetSchemaConverter.checkFieldNames(names)

    // The method checkFieldNames doesn't have a valid regex to search for '\n'. That should be
    // fixed in Apache Spark, and we can remove this additional check here.
    names.find(_.contains("\n")).foreach(col => throw DeltaErrors.invalidColumnName(col))
  }

  private object ParquetSchemaConverter {
    def checkFieldName(name: String): Unit = {
      // ,;{}()\n\t= and space are special characters in Parquet schema
      checkConversionRequirement(
        !name.matches(".*[ ,;{}()\n\t=].*"),
        s"""Attribute name "$name" contains invalid character(s) among " ,;{}()\\n\\t=".
           |Please use alias to rename it.
       """.stripMargin.split("\n").mkString(" ").trim)
    }

    def checkFieldNames(names: Seq[String]): Unit = {
      names.foreach(checkFieldName)
    }

    def checkConversionRequirement(f: => Boolean, message: String): Unit = {
      if (!f) {
        // TODO: AnalysisException ?
        throw new RuntimeException(message)
      }
    }
  }
}
