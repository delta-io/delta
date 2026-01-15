/*
 *  Copyright (2021) The Delta Lake Project Authors.
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

package io.delta.flink.table

import java.net.URI
import java.util.{Collections, Optional}

import scala.jdk.CollectionConverters.{IterableHasAsJava, MapHasAsJava}

import io.delta.flink.TestHelper
import io.delta.kernel.data.{ColumnVector, FilteredColumnarBatch}
import io.delta.kernel.defaults.internal.data.DefaultColumnarBatch
import io.delta.kernel.expressions.Literal
import io.delta.kernel.internal.util.Utils.toCloseableIterator
import io.delta.kernel.types.{DataType, IntegerType, StructType}
import io.delta.kernel.utils.CloseableIterable

import org.scalatest.funsuite.AnyFunSuite

class CCv2TableSuite extends AnyFunSuite with TestHelper {

  val CATALOG_ENDPOINT = URI.create("https://e2-dogfood.staging.cloud.databricks.com/")
  val CATALOG_TOKEN = "<PAT>"
  val TABLE_ID = "main.hao.writetest"

  ignore("load table from e2dogfood") {
    val table = new CCv2Table(
      new UnityCatalog("main", CATALOG_ENDPOINT, CATALOG_TOKEN),
      TABLE_ID,
      Map.empty[String, String].asJava,
      CATALOG_ENDPOINT,
      CATALOG_TOKEN)
    table.open()

    assert(table.getId == "main.hao.writetest")
    assert(table.getTablePath == URI.create("s3://us-west-2-extstaging-managed-" +
      "catalog-test-bucket-1/" +
      "19a85dee-54bc-43a2-87ab-023d0ec16013/tables/b7c3e881-4f7f-40f2-88c1-dff715835a81/"))
    assert(table.getSchema.equivalent(new StructType().add("id", IntegerType.INTEGER)))
  }

  ignore("commit data to e2dogfood via ccv2") {
    val table = new CCv2Table(
      new UnityCatalog("main", CATALOG_ENDPOINT, CATALOG_TOKEN),
      TABLE_ID,
      Map.empty[String, String].asJava,
      CATALOG_ENDPOINT,
      CATALOG_TOKEN)
    table.open()

    for (i <- 0 until 100) {
      val values = (0 until 10)
      val colVector = new ColumnVector() {
        override def getDataType: DataType = IntegerType.INTEGER

        override def getSize: Int = values.length

        override def close(): Unit = {}

        override def isNullAt(rowId: Int): Boolean = values(rowId) == null

        override def getInt(rowId: Int): Int = values(rowId)
      }

      val columnarBatchData =
        new DefaultColumnarBatch(values.size, table.getSchema, Array(colVector))
      val filteredColumnarBatchData = new FilteredColumnarBatch(columnarBatchData, Optional.empty())
      val partitionValues = Collections.emptyMap[String, Literal]()

      val data = toCloseableIterator(Seq(filteredColumnarBatchData).asJava.iterator())
      val rows = table.writeParquet("abc", data, partitionValues)

      table.commit(CloseableIterable.inMemoryIterable(rows), "a", i, Map("a" -> "b").asJava)
    }
  }

  ignore("serializablity") {
    val table = new CCv2Table(
      new UnityCatalog("main", CATALOG_ENDPOINT, CATALOG_TOKEN),
      TABLE_ID,
      Map.empty[String, String].asJava,
      CATALOG_ENDPOINT,
      CATALOG_TOKEN)

    checkSerializability(table)
  }
}
