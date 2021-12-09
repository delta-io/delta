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
package org.apache.spark.sql.interface.system.unstructured.commonFile.datasource

import org.apache.spark.sql.Row
import org.apache.spark.sql.interface.system.util.MultiSourceException
import org.apache.spark.sql.types._



/**
 * Author: CHEN ZHI LING
 * Date: 2021/8/17
 * Description:
 */
case class RemoteFileFilter(attr: String, value: Any, filter: String)
object RemoteFileFilter {



  def castTo(value: Any, dataType: DataType): Any = {
    dataType match {
      case _: IntegerType => value.asInstanceOf[Int]
      case _: LongType => value.asInstanceOf[Long]
      case _: StringType => value.asInstanceOf[String]
      case _: BinaryType => value.asInstanceOf[Byte]
      case _: StructType => value.asInstanceOf[Row]
    }
  }



  def applyFilters(filters: List[RemoteFileFilter], value: Any, schema: StructType): Boolean = {
    var includeInResultSet = true
    val schemaFields: Array[StructField] = schema.fields
    val index: Int = schema.fieldIndex(filters.head.attr)
    val dataType: DataType = schemaFields(index).dataType
    val castedValue: Any = RemoteFileFilter.castTo(value, dataType)
    filters.foreach((f: RemoteFileFilter) => {
      val givenValue: Any = RemoteFileFilter.castTo(f.value, dataType)
      f.filter match {
          // only support equal to now
        case "equalTo" =>
          includeInResultSet = castedValue == givenValue
        case _ => throw new MultiSourceException("this filter is not supported!!")
      }
    })
    includeInResultSet
  }
}
