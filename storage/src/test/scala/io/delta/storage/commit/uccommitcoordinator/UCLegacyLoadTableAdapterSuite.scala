/*
 * Copyright (2026) The Delta Lake Project Authors.
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

package io.delta.storage.commit.uccommitcoordinator

import scala.jdk.CollectionConverters._

import io.unitycatalog.client.delta.model
import io.unitycatalog.client.model.{ColumnInfo, DataSourceFormat, TableInfo, TableType}

import org.scalatest.funsuite.AnyFunSuite

class UCLegacyLoadTableAdapterSuite extends AnyFunSuite {

  private def baseTableInfo: TableInfo = new TableInfo()
    .name("tbl")
    .catalogName("main")
    .schemaName("default")
    .tableType(TableType.MANAGED)
    .dataSourceFormat(DataSourceFormat.DELTA)
    .tableId("11111111-1111-1111-1111-111111111111")
    .storageLocation("s3://bucket/path/to/table")
    .createdAt(10L)
    .updatedAt(11L)
    .columns(new java.util.ArrayList[ColumnInfo]())

  private def primitiveColumn(name: String, typeText: String): ColumnInfo = new ColumnInfo()
    .name(name)
    .typeText(typeText)
    .typeJson(s"""{"name":"$name","type":"$typeText","nullable":true,"metadata":{}}""")
    .nullable(true)
    .position(0)

  test("legacy fallback leaves etag unset instead of inventing a value") {
    val metadata = UCLegacyLoadTableAdapter
      .loadTableViaLegacyApi(
        new StubTablesApi(baseTableInfo.addColumnsItem(primitiveColumn("id", "string"))),
        "main",
        "default",
        "tbl")
      .getMetadata

    assert(metadata.getEtag == null)
  }

  test("legacy fallback rejects missing metadata fields instead of inventing defaults") {
    val missingCreatedAt = baseTableInfo
      .addColumnsItem(primitiveColumn("id", "string"))
      .createdAt(null)

    val e = intercept[java.io.IOException] {
      UCLegacyLoadTableAdapter.loadTableViaLegacyApi(
        new StubTablesApi(missingCreatedAt),
        "main",
        "default",
        "tbl")
    }

    assert(e.getMessage.contains("created_at"))
  }

  test("legacy fallback rejects invalid table UUID") {
    val invalidTableId = baseTableInfo
      .addColumnsItem(primitiveColumn("id", "string"))
      .tableId("not-a-uuid")

    val e = intercept[java.io.IOException] {
      UCLegacyLoadTableAdapter.loadTableViaLegacyApi(
        new StubTablesApi(invalidTableId),
        "main",
        "default",
        "tbl")
    }

    assert(e.getMessage.contains("Invalid legacy table_id"))
  }

  test("legacy fallback rejects missing type_json") {
    val missingType = baseTableInfo.addColumnsItem(
      new ColumnInfo().name("id").typeText("string").nullable(true))

    val e = intercept[java.io.IOException] {
      UCLegacyLoadTableAdapter.loadTableViaLegacyApi(
        new StubTablesApi(missingType),
        "main",
        "default",
        "tbl")
    }

    assert(e.getMessage.contains("missing type_json"))
  }

  test("legacy fallback rejects malformed type_json") {
    val malformedTypeJson = baseTableInfo.addColumnsItem(
      new ColumnInfo()
        .name("payload")
        .nullable(true)
        .typeJson("""{"type":"struct","fields":["""))

    val e = intercept[java.io.IOException] {
      UCLegacyLoadTableAdapter.loadTableViaLegacyApi(
        new StubTablesApi(malformedTypeJson),
        "main",
        "default",
        "tbl")
    }

    assert(e.getMessage.contains("Failed to parse legacy column type JSON"))
  }

  test("legacy fallback rejects duplicate non-negative partition indexes") {
    val duplicatePartitions = baseTableInfo
      .columns(
        Seq(
          primitiveColumn("p1", "string").partitionIndex(0),
          primitiveColumn("p2", "string").partitionIndex(0),
          primitiveColumn("value", "string")).asJava)

    val e = intercept[java.io.IOException] {
      UCLegacyLoadTableAdapter.loadTableViaLegacyApi(
        new StubTablesApi(duplicatePartitions),
        "main",
        "default",
        "tbl")
    }

    assert(e.getMessage.contains("duplicate partition_index"))
  }

  test("legacy fallback ignores negative partition indexes") {
    val tableInfo = baseTableInfo
      .columns(
        Seq(
          primitiveColumn("ignored", "string").partitionIndex(-1),
          primitiveColumn("date", "date").partitionIndex(0),
          primitiveColumn("region", "string").partitionIndex(1)).asJava)

    val metadata = UCLegacyLoadTableAdapter
      .loadTableViaLegacyApi(new StubTablesApi(tableInfo), "main", "default", "tbl")
      .getMetadata

    assert(metadata.getPartitionColumns.asScala.toSeq === Seq("date", "region"))
  }

  test("legacy fallback parses camelCase Spark type_json and preserves field metadata") {
    val tableInfo = baseTableInfo.columns(
      Seq(
        new ColumnInfo()
          .name("payload")
          .nullable(false)
          .position(0)
          .typeJson(
            """{"name":"payload","type":{"type":"struct","fields":[{"name":"tags","type":{"type":"array","elementType":"string","containsNull":true},"nullable":true,"metadata":{"comment":"nested tags"}},{"name":"attributes","type":{"type":"map","keyType":"string","valueType":"long","valueContainsNull":false},"nullable":false,"metadata":{}}]},"nullable":false,"metadata":{"delta.columnMapping.id":1}}"""),
        primitiveColumn("value", "string")).asJava)

    val metadata = UCLegacyLoadTableAdapter
      .loadTableViaLegacyApi(new StubTablesApi(tableInfo), "main", "default", "tbl")
      .getMetadata

    val payloadField = metadata.getColumns.getFields.get(0)
    val payloadType = payloadField.getType.asInstanceOf[model.StructType]
    val tagsField = payloadType.getFields.get(0)
    val tagsType = tagsField.getType.asInstanceOf[model.ArrayType]
    val attributesField = payloadType.getFields.get(1)
    val attributesType = attributesField.getType.asInstanceOf[model.MapType]

    assert(!payloadField.getNullable)
    assert(payloadField.getMetadata.get("delta.columnMapping.id") === Integer.valueOf(1))
    assert(tagsField.getMetadata.get("comment") === "nested tags")
    assert(tagsType.getContainsNull)
    assert(tagsType.getElementType.asInstanceOf[model.PrimitiveType].getType === "string")
    assert(attributesType.getKeyType.asInstanceOf[model.PrimitiveType].getType === "string")
    assert(attributesType.getValueType.asInstanceOf[model.PrimitiveType].getType === "long")
    assert(!attributesType.getValueContainsNull)
  }

  private class StubTablesApi(tableInfo: TableInfo)
    extends io.unitycatalog.client.api.TablesApi {
    override def getTable(
        fullName: String,
        readStreamingTableAsManaged: java.lang.Boolean,
        readMaterializedViewAsManaged: java.lang.Boolean): TableInfo = tableInfo
  }
}
