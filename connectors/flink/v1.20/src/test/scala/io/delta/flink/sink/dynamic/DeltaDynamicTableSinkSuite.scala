package io.delta.flink.sink.dynamic

import scala.jdk.CollectionConverters.MapHasAsJava

import io.delta.flink.TestHelper

import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, Schema, TableEnvironment}
import org.apache.flink.table.catalog.{CatalogTable, ResolvedCatalogTable, ResolvedSchema}
import org.scalatest.funsuite.AnyFunSuite

class DeltaDynamicTableSinkSuite extends AnyFunSuite with TestHelper {

  test("load table") {
    withTempDir { dir =>
      val options = Map(
        "connector" -> "delta-connector",
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

  test("use sql to load table") {
    withTempDir { dir =>
      val settings = EnvironmentSettings.newInstance.inStreamingMode.build

      val tEnv = TableEnvironment.create(settings)

      tEnv.executeSql(
        """
           CREATE TEMPORARY TABLE src (
           id BIGINT,
           dt STRING
           ) WITH (
           'connector' = 'values',
           'bounded' = 'true',
           'data-id' = 'my_test'
           )""".stripMargin)

      tEnv.executeSql(
        s"""
          CREATE TEMPORARY TABLE sink (
            id BIGINT,
            dt STRING
          ) WITH (
            'connector' = 'delta-connector',
            'table_path' = '${dir.getPath}'
          )
        """.stripMargin)

      tEnv.executeSql("INSERT INTO sink SELECT id, dt FROM src").await();
    }
  }
}
