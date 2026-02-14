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

import scala.jdk.CollectionConverters.{MapHasAsJava, SeqHasAsJava}

import io.delta.kernel.types._

import org.scalatest.funsuite.AnyFunSuite

class UnityCatalogSuite extends AnyFunSuite {

  val CATALOG_ENDPOINT = URI.create("https://e2-dogfood.staging.cloud.databricks.com/")
  val CATALOG_TOKEN = ""

  ignore("create table") {
    val catalog = new UnityCatalog("main", CATALOG_ENDPOINT, CATALOG_TOKEN)

    val nested = new StructType().add("nested", IntegerType.INTEGER)
      .add("nested_id", StringType.STRING)

    val schema =
      new StructType().add("id", IntegerType.INTEGER)
        .add("name", StringType.STRING)
        .add("b", BooleanType.BOOLEAN, true)
        .add("i", IntegerType.INTEGER, true)
        .add("l", LongType.LONG, true)
        .add("f", FloatType.FLOAT, true)
        .add("d", DoubleType.DOUBLE, true)
        .add("s", StringType.STRING, true)
        .add("bin", BinaryType.BINARY, true)
        .add("dec", new DecimalType(10, 2), true)
        .add("ids", new ArrayType(IntegerType.INTEGER, true), true)
        .add("names", new ArrayType(nested, true), true)
        .add("map", new MapType(IntegerType.INTEGER, StringType.STRING, true), true)
        .add("map2", new MapType(IntegerType.INTEGER, nested, true), true)

    catalog.createTable(
      "main.hao.testcreate",
      schema,
      List.empty[String].asJava,
      Map("a" -> "b").asJava)
  }
}
