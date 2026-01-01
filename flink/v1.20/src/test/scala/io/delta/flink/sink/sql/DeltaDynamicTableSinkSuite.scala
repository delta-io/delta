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

package io.delta.flink.sink.sql

import scala.jdk.CollectionConverters.MapHasAsJava

import io.delta.flink.TestHelper
import io.delta.flink.sink.dynamic.TestDynamicTableSinkContext

import org.apache.flink.table.api.{DataTypes, Schema}
import org.apache.flink.table.catalog.{CatalogTable, ResolvedCatalogTable, ResolvedSchema}
import org.scalatest.funsuite.AnyFunSuite

class DeltaDynamicTableSinkSuite extends AnyFunSuite with TestHelper {

  test("load table") {
    withTempDir { dir =>
      val options = Map(
        "connector" -> "delta",
        "table_path" -> s"${dir.getPath}")

      val table = CatalogTable.of(
        Schema.newBuilder
          .column("id", DataTypes.BIGINT)
          .column("dt", DataTypes.STRING).build,
        "test table",
        java.util.List.of,
        options.asJava)

      val resolvedTable = new ResolvedCatalogTable(
        table,
        ResolvedSchema.physical(
          Array("id", "dt"),
          Array(DataTypes.BIGINT, DataTypes.STRING)))

      val context = new TestDynamicTableSinkContext(resolvedTable);

      val factory = new DeltaDynamicTableSinkFactory
      val sink = factory.createDynamicTableSink(context)

      assert(sink.isInstanceOf[DeltaDynamicTableSink])
    }
  }
}
