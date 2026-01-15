/*
 *  Copyright (2021) The Delta Lake Project Authors.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.delta.flink.table

import java.net.URI

import scala.jdk.CollectionConverters.{MapHasAsJava, MapHasAsScala, SeqHasAsJava}

import io.delta.flink.TestHelper
import io.delta.flink.table.MetricListener.StatsListener
import io.delta.kernel.data.Row
import io.delta.kernel.expressions.Literal
import io.delta.kernel.internal.util.Utils
import io.delta.kernel.types.{IntegerType, StringType, StructType}
import io.delta.kernel.utils.{CloseableIterable, CloseableIterator}

import org.scalatest.funsuite.AnyFunSuite
import org.slf4j.LoggerFactory

class BenchmarkSuite extends AnyFunSuite with TestHelper {

  val CATALOG_ENDPOINT = URI.create("https://e2-dogfood.staging.cloud.databricks.com/")
  val CATALOG_TOKEN = "<PAT>"

  ignore("benchmark the local fs write") {
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

      val statsListener = new StatsListener
      table.addMetricListener(statsListener)

      for (i <- 0 until 100) {
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
        table.commit(dataActions, "a", i, Map.empty[String, String].asJava)
      }

      val logger = LoggerFactory.getLogger(classOf[HadoopTableSuite])

      statsListener.report().asScala.foreach { case (key, values) =>
        logger.error("{}, [{}, {}, {}, {}]", key, values(0), values(1), values(2), values(3))
      }
    }
  }

  ignore("benchmark the s3 fs write") {
    val schema = new StructType()
      .add("id", IntegerType.INTEGER)
      .add("part", StringType.STRING)

    val TABLE_ID = "main.hao.writetest_path"

    val table = new HadoopTable(
      new UnityCatalog("main", CATALOG_ENDPOINT, CATALOG_TOKEN),
      TABLE_ID,
      Map.empty[String, String].asJava)
    table.open()

    val statsListener = new StatsListener
    table.addMetricListener(statsListener)

    for (i <- 0 until 100) {
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
      table.commit(dataActions, "a", i, Map.empty[String, String].asJava)
    }

    val logger = LoggerFactory.getLogger(classOf[HadoopTableSuite])

    statsListener.report().asScala.foreach { case (key, values) =>
      logger.error("{}, [{}, {}, {}, {}]", key, values(0), values(1), values(2), values(3))
    }
  }

  ignore("benchmark the ccv2 write") {
    val schema = new StructType()
      .add("id", IntegerType.INTEGER)
      .add("part", StringType.STRING)

    val TABLE_ID = "main.hao.writetest"

    val table = new CCv2Table(
      new UnityCatalog("main", CATALOG_ENDPOINT, CATALOG_TOKEN),
      TABLE_ID,
      Map.empty[String, String].asJava,
      CATALOG_ENDPOINT,
      CATALOG_TOKEN)

    val statsListener = new StatsListener
    table.addMetricListener(statsListener)

    for (i <- 0 until 100) {
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
      table.commit(dataActions, "a", i, Map.empty[String, String].asJava)
    }

    val logger = LoggerFactory.getLogger(classOf[HadoopTableSuite])

    statsListener.report().asScala.foreach { case (key, values) =>
      logger.error("{}, [{}, {}, {}, {}]", key, values(0), values(1), values(2), values(3))
    }
  }
}
