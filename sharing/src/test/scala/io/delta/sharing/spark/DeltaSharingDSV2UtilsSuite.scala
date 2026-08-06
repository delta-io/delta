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

package io.delta.sharing.spark

import org.apache.spark.sql.delta.actions.{Metadata, Protocol}
import io.delta.sharing.client.model.{Table => DeltaSharingTable}
import io.delta.sharing.spark.model.{DeltaSharingMetadata, DeltaSharingProtocol}

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.connector.catalog.TableCatalog
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
 * Pure-function unit tests for [[DeltaSharingDSV2Utils]].
 */
class DeltaSharingDSV2UtilsSuite extends SparkFunSuite {

  private val tablePath = "uc-deltasharing://main.default.t#main.default.t"

  /**
   * Build a [[DeltaSharingV2TableContext]] whose delta metadata carries the given schema,
   * partition columns, configuration and description. `partitionSchema` is derived by Metadata from
   * `schemaString` + `partitionColumns`, so a partition column must also appear in `schema`. The
   * client is null: none of the functions under test touch it.
   */
  private def contextWith(
      schema: StructType = new StructType(),
      partitionColumns: Seq[String] = Nil,
      configuration: Map[String, String] = Map.empty,
      description: String = null): DeltaSharingV2TableContext = {
    val deltaMetadata = Metadata(
      schemaString = schema.json,
      partitionColumns = partitionColumns,
      configuration = configuration,
      description = description)
    val dsMeta = DeltaSharingUtils.DeltaSharingTableMetadata(
      version = 1L,
      protocol = DeltaSharingProtocol(Protocol()),
      metadata = DeltaSharingMetadata(deltaMetadata = deltaMetadata))
    new DeltaSharingV2TableContext(
      client = null,
      dsTable = DeltaSharingTable(share = "share", schema = "schema", name = "t"),
      dsMeta = dsMeta,
      tablePath = tablePath)
  }

  test("tableProperties: passes through delta.* configuration and adds provider + location") {
    val props = DeltaSharingDSV2Utils.tableProperties(
      contextWith(configuration = Map(
        "delta.enableDeletionVectors" -> "true",
        "delta.columnMapping.mode" -> "name")))

    // Provider is always "deltasharing"; location is the shared table path.
    assert(props.get(TableCatalog.PROP_PROVIDER) == "deltasharing")
    assert(props.get(TableCatalog.PROP_LOCATION) == tablePath)
    // The shared table's delta.* configuration is surfaced verbatim.
    assert(props.get("delta.enableDeletionVectors") == "true")
    assert(props.get("delta.columnMapping.mode") == "name")
  }

  test("tableProperties: empty configuration yields just provider + location, no comment") {
    val props = DeltaSharingDSV2Utils.tableProperties(contextWith(configuration = Map.empty))

    assert(props.keySet().size() == 2)
    assert(props.get(TableCatalog.PROP_PROVIDER) == "deltasharing")
    assert(props.get(TableCatalog.PROP_LOCATION) == tablePath)
    assert(!props.containsKey(TableCatalog.PROP_COMMENT))
  }

  test("tableProperties: a non-null description surfaces as the comment property") {
    val props = DeltaSharingDSV2Utils.tableProperties(
      contextWith(configuration = Map.empty, description = "a shared table"))

    assert(props.get(TableCatalog.PROP_COMMENT) == "a shared table")
  }

  test("tableProperties: a null description omits the comment property (no null value)") {
    val props = DeltaSharingDSV2Utils.tableProperties(
      contextWith(configuration = Map.empty, description = null))

    assert(!props.containsKey(TableCatalog.PROP_COMMENT))
  }

  test("tableProperties: the returned map is unmodifiable") {
    val props = DeltaSharingDSV2Utils.tableProperties(contextWith(configuration = Map.empty))

    intercept[UnsupportedOperationException] {
      props.put("delta.foo", "bar")
    }
  }

  // prunedNonPartitionColumns: the required (column-projected) schema minus the table's partition
  // columns -- the data columns Kernel reads out of the Parquet files. Partition columns are
  // dropped because their values come from the file path, not the file contents. SparkScan
  // re-appends them, so a bug here surfaces as reading a partition column out of the file (wrong).

  private val fullSchema = new StructType()
    .add(StructField("id", IntegerType))
    .add(StructField("value", StringType))
    .add(StructField("part", StringType))

  test("prunedNonPartitionColumns: drops the partition column and preserves data field order") {
    val ctx = contextWith(schema = fullSchema, partitionColumns = Seq("part"))
    val pruned = DeltaSharingDSV2Utils.prunedNonPartitionColumns(
      ctx, requiredSchema = fullSchema)

    // Field order of the required schema is preserved; only the partition column is removed.
    assert(pruned.fieldNames.toSeq == Seq("id", "value"))
  }

  test("prunedNonPartitionColumns: partition matching is case-insensitive") {
    // The table's partition column is "part"; the required schema references it as "PART" (e.g.
    // after case-insensitive analysis). It must still be recognized as a partition column and
    // dropped -- the method lower-cases both sides.
    val ctx = contextWith(schema = fullSchema, partitionColumns = Seq("part"))
    val requiredSchema = new StructType()
      .add(StructField("id", IntegerType))
      .add(StructField("PART", StringType))
    val pruned = DeltaSharingDSV2Utils.prunedNonPartitionColumns(ctx, requiredSchema)

    assert(pruned.fieldNames.toSeq == Seq("id"))
  }

  test("prunedNonPartitionColumns: drops every partition column when there are multiple") {
    val schema = fullSchema.add(StructField("part2", StringType))
    val ctx = contextWith(schema = schema, partitionColumns = Seq("part", "part2"))
    val pruned = DeltaSharingDSV2Utils.prunedNonPartitionColumns(ctx, requiredSchema = schema)

    assert(pruned.fieldNames.toSeq == Seq("id", "value"))
  }

  test("prunedNonPartitionColumns: an unpartitioned table returns the required schema unchanged") {
    val ctx = contextWith(schema = fullSchema, partitionColumns = Nil)
    val pruned = DeltaSharingDSV2Utils.prunedNonPartitionColumns(
      ctx, requiredSchema = fullSchema)

    assert(pruned == fullSchema)
  }

  test("prunedNonPartitionColumns: projecting only the partition column yields an empty schema") {
    val ctx = contextWith(schema = fullSchema, partitionColumns = Seq("part"))
    val requiredSchema = new StructType().add(StructField("part", StringType))
    val pruned = DeltaSharingDSV2Utils.prunedNonPartitionColumns(ctx, requiredSchema)

    assert(pruned.isEmpty)
  }
}
