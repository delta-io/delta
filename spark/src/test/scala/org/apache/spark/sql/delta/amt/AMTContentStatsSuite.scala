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

package org.apache.spark.sql.delta.amt

import org.apache.spark.sql.delta.{CatalogOwnedTableFeature, ColumnMappingTableFeature, DeletionVectorsTableFeature, DeltaColumnMapping, DeltaConfigs, DomainMetadataTableFeature, RowTrackingFeature}
import org.apache.spark.sql.delta.actions.{AddFile, Metadata, Protocol}
import org.apache.spark.sql.delta.util.JsonUtils
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.datasources.parquet.ParquetUtils
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.{BooleanType, ByteType, DataType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, StructField, StructType, TimestampNTZType, TimestampType, VariantType}

/**
 * Tests for [[AMTContentStats]] -- the conversion between Delta's stats JSON string and the typed
 * Iceberg V4 `content_stats` struct. `forWrite` and `forRead` are exercised end to end against a
 * real all-types table (see [[withAllTypesTable]]); the edge cases -- unsupported bound types, a
 * table that collects no statistics, and the `tight_stats` / `tightBounds` mapping -- run on
 * synthesized `(Metadata, Protocol)` fixtures (see [[metadataWithColumnIds]]).
 */
class AMTContentStatsSuite extends AMTCheckpointTestBase {

  import testImplicits._

  /**
   * The `content_stats` sub-struct field name for logical column `name`. Sub-structs are named by
   * the logical name with the (unique) field id attached (see `StatsLeaf.fieldName`) -- e.g.
   * `d_int_1` -- so a test refers to a column by its logical name and this resolves the id suffix.
   * The logical names used here are unique, so exactly one sub-struct matches.
   */
  private def contentStatsField(persisted: DataFrame, name: String): String = {
    val matches = persisted.schema("content_stats").dataType.asInstanceOf[StructType]
      .fieldNames.filter(_.matches(java.util.regex.Pattern.quote(name) + "_\\d+"))
    assert(matches.length == 1,
      s"expected exactly one content_stats sub-struct for $name, got ${matches.toSeq}")
    matches.head
  }

  /** The logical column names of a persisted `content_stats` struct, id suffix stripped. */
  private def contentStatsLogicalNames(persisted: DataFrame): Seq[String] =
    persisted.schema("content_stats").dataType.asInstanceOf[StructType]
      .fieldNames.toSeq.map(_.replaceAll("_\\d+$", ""))

  /**
   * Asserts the `content_stats` sub-struct `forWrite` produced for column `name` has exactly the
   * `expected` stat fields: each maps to its Iceberg type and the string form of its value.
   */
  private def assertStatFields(
      persistedDf: DataFrame,
      columnName: String,
      expectedSubFieldToValueMapping: Map[String, (DataType, String)]): Unit = {
    val fieldName = contentStatsField(persistedDf, columnName)
    val sub = persistedDf.schema("content_stats").dataType.asInstanceOf[StructType](fieldName)
      .dataType.asInstanceOf[StructType]
    val row = persistedDf.select(s"content_stats.$fieldName.*").head()
    val actual = sub.fields.zipWithIndex.map { case (field, i) =>
      field.name -> (field.dataType, String.valueOf(row.get(i)))
    }.toMap
    assert(actual == expectedSubFieldToValueMapping,
      s"content_stats.$columnName\n  expected=$expectedSubFieldToValueMapping\n  actual=$actual")
  }

  test("forWrite persists content_stats as the typed struct the delta log's stats fill") {
    withAllTypesTable("amt_content_stats_write") { deltaLog =>
      appendRowsAsSeparateFiles(
        "amt_content_stats_write", numFiles = 1, columnExprs = allTypesTableColumnExprs)
      val snapshot = deltaLog.update()
      val metadata = snapshot.metadata
      val protocol = snapshot.protocol
      val add = liveAddFiles(snapshot).head
      val entry = DataEntry.fromAddFile(
        add, AMTWriteHelper.addedTrackingForDataEntry(), deltaLog.dataPath)
      val input = Seq(entry.wrap).toDS().toDF()
      val persisted = AMTContentStats.forWrite(input, metadata, protocol)

      assert(contentStatsLogicalNames(persisted) == Seq(
        "d_int", "d_long", "d_short", "d_byte", "d_str", "d_date", "d_ts", "d_ts_ntz", "d_dec",
        "d_bool", "d_float", "d_double", "d_binary", "d_arr", "d_nested_inner"))

      def bounded(name: String, dataType: DataType, value: String): Unit =
        assertStatFields(persisted, name, Map(
          "lower_bound" -> (dataType, value),
          "upper_bound" -> (dataType, value),
          "null_value_count" -> (LongType, "0")))
      def boundless(name: String): Unit =
        assertStatFields(persisted, name, Map("null_value_count" -> (LongType, "0")))

      bounded("d_int", IntegerType, "0")
      bounded("d_long", LongType, "0")
      bounded("d_short", ShortType, "0")
      bounded("d_byte", ByteType, "0")
      bounded("d_str", StringType, "row0")
      bounded("d_date", DateType, "2026-07-25")
      bounded("d_dec", DecimalType(9, 3), "0.000")
      bounded("d_float", FloatType, "0.0")
      bounded("d_double", DoubleType, "0.0")
      bounded("d_nested_inner", IntegerType, "0")
      boundless("d_ts")
      boundless("d_ts_ntz")
      boundless("d_bool")
      boundless("d_binary")
      boundless("d_arr")
      // Delta's tightBounds is not carried over to the entry's file-level tight_stats yet.
      assert(persistedTightStats(persisted).contains(false))
    }
  }

  test("forRead reconstructs the stats JSON the delta log holds, for every type") {
    withAllTypesTable("amt_content_stats_roundtrip") { deltaLog =>
      appendRowsAsSeparateFiles(
        "amt_content_stats_roundtrip", numFiles = leafPackedFiles,
        columnExprs = allTypesTableColumnExprs)
      commitCheckpoint(deltaLog, incremental = false)
      val snapshot = deltaLog.update()
      val provider = amtProvider(snapshot).getOrElse(fail("expected AMTCheckpointProvider"))
      val metadata = snapshot.metadata
      val protocol = snapshot.protocol
      val leaves = provider.liveLeafManifestAbsolutePaths.map(_.toString)
      assert(leaves.nonEmpty, "Expected at least one leaf manifest.")

      // Read the stats straight from the commit json rather than the snapshot: an AMT snapshot
      // reconstructs its AddFiles through `forRead`, so comparing against it would compare it with
      // itself. Compare as parsed JSON trees so field order and formatting do not matter.
      val loggedSchema = StructType(Seq(StructField("add", StructType(Seq(
        StructField("stats", StringType))))))
      // Two things do not survive AMT yet, so adjust the logged stats to what forRead reconstructs:
      // AMT persists no timestamp bounds (see `isSupportedBoundType`), so the timestamp min/max are
      // dropped; and Delta's tightBounds is written to `tight_stats` as false, so it reads back as
      // false.
      val timestampColumns = metadata.dataSchema.filter(f =>
        f.dataType.isInstanceOf[TimestampType] || f.dataType.isInstanceOf[TimestampNTZType])
      def asReconstructed(stats: JsonNode): JsonNode = {
        Seq("minValues", "maxValues").foreach { stat =>
          val values = stats.get(stat).asInstanceOf[ObjectNode]
          timestampColumns.foreach(f => values.remove(DeltaColumnMapping.getPhysicalName(f)))
        }
        stats.asInstanceOf[ObjectNode].put("tightBounds", false)
        stats
      }
      val logged = spark.read.schema(loggedSchema)
        .json(new Path(deltaLog.logPath, "*.json").toString)
        .where(col("add").isNotNull)
        .select(col("add.stats"))
        .as[String].collect()
        .map(json => asReconstructed(JsonUtils.mapper.readTree(json))).toSet
      assert(logged.size == leafPackedFiles,
        s"Expected one logged stats per file, got ${logged.size}.")

      // forRead reconstructs the Delta stats JSON from the leaves' DATA entries; compare as parsed
      // JSON trees so field order and formatting do not matter.
      val reconstructed = withManifestDataEntries(leaves) { entries =>
        AMTContentStats.forRead(entries, metadata, protocol)
          .select(col("content_stats"))
          .as[String].collect()
          .map(JsonUtils.mapper.readTree).toSet
      }
      // Precompute the comparison and message: ScalaTest's `assert` macro fails to expand on Scala
      // 2.12 when its arguments interpolate `Set[JsonNode]` values, so hand it plain Boolean/String
      // values instead ("macro has not been expanded").
      val reconstructedMatchesLogged = reconstructed == logged
      val mismatchClue =
        s"forRead did not reproduce the logged stats.\n  logged=$logged\n" +
          s"  reconstructed=$reconstructed"
      assert(reconstructedMatchesLogged, mismatchClue)
    }
  }

  /**
   * A stand-in table root; synthesized entries carry no DV, so it is only a
   * path placeholder.
   */
  private val tableRoot = new Path("file:/tmp/amt-test-table")

  /**
   * A [[Protocol]] representative of a real AMT table: it carries every
   * feature `AdaptiveMetadataTableFeature` requires. In particular it is
   * DV-capable, so Delta's derived stats schema always carries `tightBounds`.
   */
  private def amtProtocol: Protocol =
    Protocol(3, 7)
      .withFeature(CatalogOwnedTableFeature)
      .withFeature(RowTrackingFeature)
      .withFeature(DomainMetadataTableFeature)
      .withFeature(DeletionVectorsTableFeature)
      .withFeature(ColumnMappingTableFeature)

  /** A [[Metadata]] for `schema` in `id` column-mapping mode, with column ids
   * and physical names assigned the way a real AMT table has them (AMT
   * requires that mode, and the content-stats field ids are derived from those
   * column ids). Stats are keyed by whatever physical name Delta assigns, so
   * it does not matter that the assignment is a fresh UUID per column.
   */
  private def metadataWithColumnIds(
      schema: StructType,
      partitionColumns: Seq[String] = Seq.empty,
      configuration: Map[String, String] = Map.empty
  ): Metadata = {
    val base = Metadata(
      id = "amt-content-stats-test",
      schemaString = schema.json,
      partitionColumns = partitionColumns,
      configuration = configuration +
        (DeltaConfigs.COLUMN_MAPPING_MODE.key -> "id")
    )
    DeltaColumnMapping.assignColumnIdAndPhysicalName(
      newMetadata = base,
      oldMetadata = Metadata(),
      isChangingModeOnExistingTable = false,
      isOverwritingSchema = false
    )
  }

  /**
   * A Delta stats JSON string covering every data column of `metadata` (keyed by physical name, as
   * Delta writes it): a placeholder min/max, a zero null count, and the given file-level
   * `tightBounds`. Boundless types (variant, geometry/geography) get a null count but no min/max.
   */
  private def getSampleStatsJson(metadata: Metadata, tightBounds: Boolean): String = {
    def bounds(dataType: DataType): Option[(String, String)] = dataType match {
      case _: IntegerType | _: LongType => Some(("1", "9"))
      case _: StringType => Some(("\"apple\"", "\"pear\""))
      case _: TimestampType =>
        Some(("\"2020-01-01T00:00:00.000Z\"", "\"2020-12-31T00:00:00.000Z\""))
      case _: TimestampNTZType =>
        Some(("\"2020-01-01T00:00:00.000\"", "\"2020-12-31T00:00:00.000\""))
      case _: VariantType => None
      case other => fail(s"getSampleStatsJson has no placeholder for $other")
    }
    val columns = metadata.dataSchema
    def obj(value: StructField => Option[String]): String =
      columns.flatMap(f =>
        value(f).map(v => s""""${DeltaColumnMapping.getPhysicalName(f)}":$v""")).mkString(",")
    s"""{"numRecords":10,
       |"minValues":{${obj(f => bounds(f.dataType).map(_._1))}},
       |"maxValues":{${obj(f => bounds(f.dataType).map(_._2))}},
       |"nullCount":{${obj(_ => Some("0"))}},
       |"tightBounds":$tightBounds}""".stripMargin
  }

  /**
   * Builds the [[AMTSingleAction]] input for one data file carrying `statsJson`, then runs
   * [[AMTContentStats.forWrite]] to get the persisted, typed `content_stats`.
   */
  private def writeStats(statsJson: String, metadata: Metadata, protocol: Protocol): DataFrame = {
    val addFile = AddFile(
      path = "part-00000.parquet",
      partitionValues = Map("p" -> "1"),
      size = 1024L,
      modificationTime = 100L,
      dataChange = true,
      stats = statsJson)
    val entry = DataEntry.fromAddFile(
      addFile, AMTWriteHelper.addedTrackingForDataEntry(), tableRoot)
    val input = Seq(entry.wrap).toDS().toDF()
    AMTContentStats.forWrite(input, metadata, protocol)
  }

  /**
   * Runs [[AMTContentStats.forRead]] on a persisted DataFrame and returns the single entry's
   * reconstructed Delta stats JSON.
   */
  private def readStats(persisted: DataFrame, metadata: Metadata, protocol: Protocol): String =
    AMTContentStats.forRead(persisted, metadata, protocol)
      .as[AMTSingleAction].head()
      .unwrap.asInstanceOf[DataEntry].toAddFile(tableRoot).stats

  test("variant columns persist a null count but no bounds") {
    // Delta collects min/max for VARIANT, but AMTContentStats has no Iceberg V4 bound
    // representation for it yet, so `isSupportedBoundType` rejects it. It is still counted for
    // nulls, so it surfaces with a `null_value_count` only -- like a plain array column.
    val metadata = metadataWithColumnIds(
      new StructType()
        .add("id", LongType)    // supported bound type: full typed bounds
        .add("v", VariantType)) // unsupported bound type
    val protocol = amtProtocol
    val persisted =
      writeStats(getSampleStatsJson(metadata, tightBounds = true), metadata, protocol)
    assertStatFields(persisted, "id", Map(
      "lower_bound" -> (LongType, "1"),
      "upper_bound" -> (LongType, "9"),
      "null_value_count" -> (LongType, "0")))
    assertStatFields(persisted, "v", Map("null_value_count" -> (LongType, "0")))
  }

  test("timestamp columns persist a null count but no bounds") {
    // Delta truncates timestamp stats to milliseconds, so its max can sit below the file's true
    // max; persisted as-is it would be an unsafe Iceberg upper_bound. `isSupportedBoundType`
    // therefore rejects both timestamp types, which keep only a `null_value_count`, and reading
    // back reconstructs no min/max for them (Delta just does not skip on those columns).
    val metadata = metadataWithColumnIds(
      new StructType()
        .add("id", LongType)
        .add("ts", TimestampType)
        .add("ts_ntz", TimestampNTZType))
    val persisted =
      writeStats(getSampleStatsJson(metadata, tightBounds = true), metadata, amtProtocol)
    assertStatFields(persisted, "ts", Map("null_value_count" -> (LongType, "0")))
    assertStatFields(persisted, "ts_ntz", Map("null_value_count" -> (LongType, "0")))

    val reconstructed = JsonUtils.mapper.readTree(readStats(persisted, metadata, amtProtocol))
    Seq("ts", "ts_ntz").foreach { name =>
      val physical = DeltaColumnMapping.getPhysicalName(metadata.dataSchema(name))
      assert(!reconstructed.get("minValues").has(physical), s"$name must have no min")
      assert(!reconstructed.get("maxValues").has(physical), s"$name must have no max")
      assert(reconstructed.get("nullCount").get(physical).asLong == 0,
        s"$name keeps its null count")
    }
  }

  test("content stats adapter round-trips a table that collects no statistics") {
    // numIndexedCols = 0 means Delta collects only numRecords, so there are no per-column stats and
    // `content_stats` is dropped from the schema (an empty struct is not Parquet-writable).
    val metadata = metadataWithColumnIds(
      schema = new StructType().add("id", LongType),
      configuration = Map(DeltaConfigs.DATA_SKIPPING_NUM_INDEXED_COLS.key -> "0"))
    val protocol = amtProtocol

    assert(
      !AMTSingleAction.persistedSchema(metadata, protocol).fieldNames.contains(
        "content_stats"),
      "a table with no per-column statistics omits content_stats from the persisted schema")

    val persisted = writeStats("""{"numRecords":7,"tightBounds":true}""", metadata, protocol)
    assert(persistedTightStats(persisted).contains(false),
      "tight_stats is set even when content_stats is dropped")
    // record_count and tight_stats alone reconstruct the stats. tightBounds must be spelled out
    // here too: an absent one would read as tight.
    Seq(Some(true) -> true, Some(false) -> false, None -> false).foreach {
      case (tightStats, expected) =>
        val flag = tightStats.map(lit).getOrElse(lit(null).cast(BooleanType))
        val restoredJson =
          readStats(persisted.withColumn("tight_stats", flag), metadata, protocol)
        assert(JsonUtils.mapper.readTree(restoredJson) ==
          JsonUtils.mapper.readTree(s"""{"numRecords":7,"tightBounds":$expected}"""),
          s"tight_stats=$tightStats must reconstruct tightBounds=$expected")
    }
  }

  test("content stats cover only the indexed columns, not every column") {
    // Delta collects stats only for the first `dataSkippingNumIndexedCols` columns, so
    // `statCollectionPhysicalSchema` -- and therefore `content_stats` -- covers only that prefix.
    // Three columns with a limit of 2: only `a` and `b` get a sub-struct; `c` is left out.
    val metadata = metadataWithColumnIds(
      schema = new StructType().add("a", LongType).add("b", LongType).add("c", LongType),
      configuration = Map(DeltaConfigs.DATA_SKIPPING_NUM_INDEXED_COLS.key -> "2"))
    val protocol = amtProtocol
    val persisted =
      writeStats(getSampleStatsJson(metadata, tightBounds = true), metadata, protocol)

    assert(contentStatsLogicalNames(persisted) == Seq("a", "b"),
      s"only the first 2 indexed columns get content_stats; got " +
        s"${contentStatsLogicalNames(persisted)}")
    assertStatFields(persisted, "a", Map(
      "lower_bound" -> (LongType, "1"),
      "upper_bound" -> (LongType, "9"),
      "null_value_count" -> (LongType, "0")))
  }

  test("reading back with fewer indexed columns than were written keeps only the read subset") {
    // Written with 3 indexed columns, then read back after the stats-columns config was lowered to
    // 2 (same table, physical names/ids unchanged). `forRead` derives its leaves from the read
    // metadata, so it projects only a and b; c's typed sub-struct is still on disk but drops out of
    // the reconstructed Delta stats. A superset of persisted stats is never an error, just unused.
    val schema = new StructType().add("a", LongType).add("b", LongType).add("c", LongType)
    val writeMetadata = metadataWithColumnIds(
      schema, configuration = Map(DeltaConfigs.DATA_SKIPPING_NUM_INDEXED_COLS.key -> "3"))
    val readMetadata = writeMetadata.copy(
      configuration = writeMetadata.configuration +
        (DeltaConfigs.DATA_SKIPPING_NUM_INDEXED_COLS.key -> "2"))
    val protocol = amtProtocol

    val persisted =
      writeStats(getSampleStatsJson(writeMetadata, tightBounds = true), writeMetadata, protocol)
    assert(contentStatsLogicalNames(persisted) == Seq("a", "b", "c"),
      s"written with 3 indexed columns, content_stats should carry a, b, c; got " +
        s"${contentStatsLogicalNames(persisted)}")

    val physical = readMetadata.dataSchema
      .map(f => f.name -> DeltaColumnMapping.getPhysicalName(f)).toMap
    val minValues = JsonUtils.mapper.readTree(readStats(persisted, readMetadata, protocol))
      .get("minValues")
    assert(
      minValues.has(physical("a")) && minValues.has(physical("b")) && !minValues.has(physical("c")),
      s"reading back with 2 indexed columns keeps only a and b, dropping c; got $minValues")
  }

  test("the dataSkippingStatsColumns allowlist selects which columns get content_stats") {
    // The `delta.dataSkippingStatsColumns` table property is an explicit allowlist that overrides
    // the first-N-columns default (`dataSkippingNumIndexedCols`). Allowlisting `b,c` (skipping the
    // leading `a`) proves the selection follows the property, not a column prefix.
    val metadata = metadataWithColumnIds(
      schema = new StructType().add("a", LongType).add("b", LongType).add("c", LongType),
      configuration = Map(DeltaConfigs.DATA_SKIPPING_STATS_COLUMNS.key -> "b,c"))
    val protocol = amtProtocol
    val persisted =
      writeStats(getSampleStatsJson(metadata, tightBounds = true), metadata, protocol)

    assert(contentStatsLogicalNames(persisted) == Seq("b", "c"),
      s"only the allowlisted columns get content_stats; got " +
        s"${contentStatsLogicalNames(persisted)}")
    assertStatFields(persisted, "b", Map(
      "lower_bound" -> (LongType, "1"),
      "upper_bound" -> (LongType, "9"),
      "null_value_count" -> (LongType, "0")))
    assertStatFields(persisted, "c", Map(
      "lower_bound" -> (LongType, "1"),
      "upper_bound" -> (LongType, "9"),
      "null_value_count" -> (LongType, "0")))
  }

  test("content stats round-trip a schema whose flattened leaf names collide") {
    // A top-level column `a_b` and a nested leaf `a`.`b` both flatten to the logical name "a_b".
    // Naming each content_stats sub-struct by that flattened name makes the two collide: the
    // persisted struct gets two "a_b" fields and `forRead`'s by-name lookup is ambiguous. Keying
    // the sub-structs by the (unique) column id keeps them distinct. This asserts each column's
    // stats reconstruct into its own physical-name slot -- it fails while the sub-structs are named
    // by the flattened logical name and passes once they are keyed by column id.
    val schema = new StructType()
      .add("a_b", LongType)
      .add("a", new StructType().add("b", LongType))
    val metadata = metadataWithColumnIds(schema)
    val protocol = amtProtocol

    val physTop = DeltaColumnMapping.getPhysicalName(metadata.dataSchema("a_b"))
    val structA = metadata.dataSchema("a")
    val physA = DeltaColumnMapping.getPhysicalName(structA)
    val physB = DeltaColumnMapping.getPhysicalName(structA.dataType.asInstanceOf[StructType]("b"))

    // Distinct bounds per column so a mixed-up mapping is detectable: a_b -> [1,9], a.b -> [2,8].
    val statsJson =
      s"""{"numRecords":10,
         |"minValues":{"$physTop":1,"$physA":{"$physB":2}},
         |"maxValues":{"$physTop":9,"$physA":{"$physB":8}},
         |"nullCount":{"$physTop":0,"$physA":{"$physB":0}},
         |"tightBounds":true}""".stripMargin

    val persisted = writeStats(statsJson, metadata, protocol)
    val reconstructed = JsonUtils.mapper.readTree(readStats(persisted, metadata, protocol))
    val minValues = reconstructed.get("minValues")
    val maxValues = reconstructed.get("maxValues")

    assert(minValues.get(physTop).asLong == 1 && maxValues.get(physTop).asLong == 9,
      s"top-level a_b stats must reconstruct into its own slot; got $reconstructed")
    assert(minValues.get(physA).get(physB).asLong == 2 &&
      maxValues.get(physA).get(physB).asLong == 8,
      s"nested a.b stats must reconstruct into its own slot; got $reconstructed")
  }

  test("nested null counts: null parents are counted and required fields keep a zero count") {
    val tableName = "amt_content_stats_null_parent"
    withTable(tableName) {
      createAMTTable(
        tableName,
        checkpointInterval = Int.MaxValue,
        tableSchema = "s STRUCT<i: INT, f: FLOAT>, s2 STRUCT<i: INT, f: FLOAT>, " +
          "req STRUCT<i: INT NOT NULL, f: FLOAT NOT NULL> NOT NULL")
      // One file, three rows. In `s` the children are never independently null, so its count comes
      // only from the null parent in row 1. In `s2` the inner fields are null in row 0 and the
      // parent is null in row 1, so its count combines both. `req` is NOT NULL at the field and
      // across its whole ancestor chain, so it can never be null, yet still keeps a null count:
      //
      // | row | s               | s2                 | req             |
      // |-----|-----------------|--------------------|-----------------|
      // | 0   | {i: 0,  f: 0.0} | {i: null, f: null} | {i: 0,  f: 0.0} |
      // | 1   | null            | null               | {i: 10, f: 1.0} |
      // | 2   | {i: 20, f: 2.0} | {i: 20, f: 2.0}    | {i: 20, f: 2.0} |
      appendRowsAsSeparateFiles(
        tableName,
        numFiles = 1,
        rowsPerFile = 3,
        columnExprs = Seq(
          "IF(id = 1, NULL, named_struct('i', CAST(id * 10 AS INT), 'f', CAST(id AS FLOAT)))",
          "IF(id = 1, NULL, named_struct(" +
            "'i', IF(id = 0, NULL, CAST(id * 10 AS INT)), " +
            "'f', IF(id = 0, NULL, CAST(id AS FLOAT))))",
          "named_struct('i', CAST(id * 10 AS INT), 'f', CAST(id AS FLOAT))"))
      val deltaLog = deltaLogForName(tableName)
      val snapshot = deltaLog.update()
      val add = liveAddFiles(snapshot).head
      val entry = DataEntry.fromAddFile(
        add, AMTWriteHelper.addedTrackingForDataEntry(), deltaLog.dataPath)
      val input = Seq(entry.wrap).toDS().toDF()
      val persisted = AMTContentStats.forWrite(input, snapshot.metadata, snapshot.protocol)

      def nullCount(name: String): Long = {
        val sub = col("content_stats").getField(contentStatsField(persisted, name))
        persisted.select(sub.getField("null_value_count")).as[Long].head()
      }
      // `s`: only the null parent (row 1) is counted, int and float alike.
      assert(nullCount("s_i") == 1, "int child: the null parent row must be counted")
      assert(nullCount("s_f") == 1, "float child: the null parent row must be counted, same as int")
      // `s2`: null inner (row 0) plus null parent (row 1) both counted, int and float alike.
      assert(nullCount("s2_i") == 2, "int child: null inner and null parent are both counted")
      assert(nullCount("s2_f") == 2, "float child: null inner and null parent are both counted")
      // `req`: a required field (NOT NULL up the whole chain) still keeps null_value_count, as 0.
      assert(nullCount("req_i") == 0, "required int field: null_value_count is kept, as 0")
      assert(nullCount("req_f") == 0, "required float field: null_value_count is kept, as 0")
    }
  }

  /** The persisted Iceberg `tight_stats` of the single entry in `persisted`, or None if absent. */
  private def persistedTightStats(persisted: DataFrame): Option[Boolean] =
    persisted.select(col("tight_stats")).as[Option[Boolean]].head()

  /** The file-level `tightBounds` recorded in a Delta stats JSON, or None if absent. */
  private def tightBoundsOf(statsJson: String): Option[Boolean] =
    Option(JsonUtils.mapper.readTree(statsJson).get("tightBounds")).map(_.asBoolean)

  test("an entry built from an AddFile has tight_stats = false, whatever Delta's tightBounds") {
    // Iceberg tracks tightness with the file-level `tight_stats`, which vouches for more than
    // Delta's tightBounds does (e.g. exact counts and untruncated timestamp bounds), so tightBounds
    // is not carried over: every entry AMT builds is explicitly not tight.
    val metadata = metadataWithColumnIds(
      new StructType().add("id", LongType).add("s", StringType))
    Seq(true, false).foreach { tight =>
      val persisted =
        writeStats(getSampleStatsJson(metadata, tightBounds = tight), metadata, amtProtocol)
      assert(persistedTightStats(persisted).contains(false),
        s"tightBounds=$tight must persist tight_stats=false")
      assert(tightBoundsOf(readStats(persisted, metadata, amtProtocol)).contains(false),
        s"tightBounds=$tight must reconstruct tightBounds=false")
    }
  }

  test("forRead maps tight_stats to Delta's tightBounds as-is, and an absent flag to false") {
    // forWrite always writes false, but an external Iceberg writer may set the flag or omit it.
    // Iceberg treats an absent tight_stats as not tight, whereas Delta treats an absent tightBounds
    // as tight, so the reconstruction must spell an absent flag out as false.
    val metadata = metadataWithColumnIds(
      new StructType().add("id", LongType).add("s", StringType))
    val persisted =
      writeStats(getSampleStatsJson(metadata, tightBounds = true), metadata, amtProtocol)
    Seq(Some(true) -> true, Some(false) -> false, None -> false).foreach {
      case (tightStats, expected) =>
        val flag = tightStats.map(lit).getOrElse(lit(null).cast(BooleanType))
        val tampered = persisted.withColumn("tight_stats", flag)
        assert(persistedTightStats(tampered) == tightStats)
        assert(tightBoundsOf(readStats(tampered, metadata, amtProtocol)).contains(expected),
          s"tight_stats=$tightStats must reconstruct tightBounds=$expected")
    }
  }
}
