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

import scala.jdk.CollectionConverters.{MapHasAsJava, SeqHasAsJava}

import io.delta.flink.TestHelper
import io.delta.kernel.data.{ColumnVector, FilteredColumnarBatch, Row}
import io.delta.kernel.defaults.engine.DefaultEngine
import io.delta.kernel.defaults.internal.data.DefaultColumnarBatch
import io.delta.kernel.expressions.Literal
import io.delta.kernel.internal.util.Utils
import io.delta.kernel.internal.util.Utils.toCloseableIterator
import io.delta.kernel.types.{DataType, IntegerType, StringType, StructType}
import io.delta.kernel.utils.{CloseableIterable, CloseableIterator}

import org.apache.hadoop.conf.Configuration
import org.scalatest.funsuite.AnyFunSuite

class HadoopTableSuite extends AnyFunSuite with TestHelper {

  val CATALOG_ENDPOINT = URI.create("https://e2-dogfood.staging.cloud.databricks.com/")
  val CATALOG_TOKEN = "<PAT>"
  val TABLE_ID = "main.hao.writetest"

  test("commit with same txn id") {
    withTempDir { dir =>
      val tablePath = dir.getAbsolutePath
      val schema = new StructType()
        .add("id", IntegerType.INTEGER)
        .add("part", StringType.STRING)

      val table = new HadoopTable(
        URI.create(tablePath),
        Map.empty[String, String].asJava,
        schema,
        List("part").asJava)
      table.open()

      for (i <- 0 until 10) {
        val actions = (0 until 5).map { i =>
          dummyAddFileRow(schema, 10 + i, Map("part" -> Literal.ofString("p" + i)))
        }.toList.asJava
        val dataActions = new CloseableIterable[Row]() {
          override def iterator: CloseableIterator[Row] =
            Utils.toCloseableIterator(actions.iterator())
          override def close(): Unit = {
            // Nothing to close
          }
        }
        table.commit(dataActions, "a", 100, Map.empty[String, String].asJava)
      }
      // There should be only one version
      assert(table.loadLatestSnapshot().getVersion == 1)
    }
  }

  ignore("commit to s3 path") {
    val tablePath = "s3://hao-extstaging/flinksink/dest2"
    val schema = new StructType()
      .add("id", IntegerType.INTEGER)
      .add("part", StringType.STRING)

    val table = new HadoopTable(
      URI.create(tablePath),
      Map.empty[String, String].asJava,
      schema,
      List("part").asJava)
    table.open()

    for (i <- 0 until 10) {
      val actions = (0 until 5).map { i =>
        dummyAddFileRow(schema, 10 + i, Map("part" -> Literal.ofString("p" + i)))
      }.toList.asJava
      val dataActions = new CloseableIterable[Row]() {
        override def iterator: CloseableIterator[Row] =
          Utils.toCloseableIterator(actions.iterator())
        override def close(): Unit = {
          // Nothing to close
        }
      }
      table.commit(dataActions, "a", 100, Map.empty[String, String].asJava)
    }
    // There should be only one version
    assert(table.loadLatestSnapshot().getVersion == 1)
  }

  ignore("commit data to e2dogfood via path") {
    val table = new HadoopTable(
      new UnityCatalog("main", CATALOG_ENDPOINT, CATALOG_TOKEN),
      TABLE_ID,
      Map.empty[String, String].asJava)
    table.open()

    val values = (0 until 100)
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

    table.commit(CloseableIterable.inMemoryIterable(rows), "a", 1000L, Map("a" -> "b").asJava)
  }

  ignore("load table latency") {
    val engine = DefaultEngine.create(new Configuration())
    var counter = 0L
    var phase1 = 0L
    var phase2 = 0L
    var phase3 = 0L
    val times = 20

    for (i <- 0 until times) {
      val start = System.currentTimeMillis()
      val hadoopTable = new HadoopTable(
        URI.create("file:///Users/hajiang/test/big_table"),
        Map.empty[String, String].asJava)
      hadoopTable.open()
      val p1 = System.currentTimeMillis()
      phase1 += p1 - start
      hadoopTable.refresh()
      val p2 = System.currentTimeMillis()
      phase2 += p2 - p1
      hadoopTable.loadLatestSnapshot().getScanBuilder.build().getScanFiles(engine).toInMemoryList
      phase3 += System.currentTimeMillis() - p2
      counter += System.currentTimeMillis() - start
    }

    assert((counter / times, phase1 / times, phase2 / times, phase3 / times)
      == 0)
  }

  ignore("connect to s3") {
    val conf = Map(
      "fs.s3a.access.key" -> "aaa",
      "fs.s3a.secret.key" -> "bbb",
      "fs.s3a.session.token" -> "ccc")
    val table = new HadoopTable(URI.create("s3://east1-cred/"), conf.asJava)
  }
}
