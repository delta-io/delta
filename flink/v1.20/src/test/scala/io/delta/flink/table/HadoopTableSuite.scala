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

import io.delta.flink.TestHelper
import io.delta.kernel.data.Row
import io.delta.kernel.defaults.engine.DefaultEngine
import io.delta.kernel.expressions.Literal
import io.delta.kernel.internal.util.Utils
import io.delta.kernel.types.{IntegerType, StringType, StructType}
import io.delta.kernel.utils.{CloseableIterable, CloseableIterator}
import org.apache.hadoop.conf.Configuration
import org.scalatest.funsuite.AnyFunSuite

import java.net.URI
import scala.jdk.CollectionConverters.{MapHasAsJava, SeqHasAsJava}

class HadoopTableSuite extends AnyFunSuite with TestHelper {

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
        table.commit(dataActions, 100, Map.empty[String, String].asJava)
      }
      // There should be only one version
      assert(table.loadLatestSnapshot().getVersion == 1)
    }
  }

  test("cache tables by path") {
    withTempDir { dir =>
      // Create a huge table
      val schema = new StructType()
        .add("id", IntegerType.INTEGER)
        .add("part", StringType.STRING)
      val partcol = List("part")
      val engine = DefaultEngine.create(new Configuration());
      createNonEmptyTable(engine, dir.getAbsolutePath, schema, partcol)
      val hadoopTable = new HadoopTable(
        dir.toURI,
        Map.empty[String, String].asJava,
        schema,
        partcol.asJava)
      for (i <- 0 until 5000) {
        writeTable(engine, dir.getAbsolutePath, schema, partcol)
      }
      assert(true)
    }
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
