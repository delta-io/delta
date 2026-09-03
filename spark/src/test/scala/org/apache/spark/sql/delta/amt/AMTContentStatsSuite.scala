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
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.datasources.parquet.ParquetUtils
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.{BooleanType, ByteType, DataType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, StructField, StructType, TimestampNTZType, TimestampType, VariantType}

/**
 * Tests for [[AMTContentStats]] -- the conversion between Delta's stats JSON string and the typed
 * Iceberg V4 `content_stats` struct. `forWrite` and `forRead` are exercised end to end against a
 * real all-types table (see [[withAllTypesTable]]); the edge cases -- unsupported bound types, a
 * table that collects no statistics, and the `tightBounds` reconstruction -- run on synthesized
 * `(Metadata, Protocol)` fixtures (see [[metadataWithColumnIds]]).
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
    withAllTypesTable("amt_content_stats_write", numFiles = 1) { deltaLog =>
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

      def bounded(name: String, dataType: DataType, value: String, tight: Boolean): Unit =
        assertStatFields(persisted, name, Map(
          "lower_bound" -> (dataType, value),
          "upper_bound" -> (dataType, value),
          "tight_bounds" -> (BooleanType, tight.toString),
          "null_value_count" -> (LongType, "0")))
      def boundless(name: String): Unit =
        assertStatFields(persisted, name, Map("null_value_count" -> (LongType, "0")))

      bounded("d_int", IntegerType, "0", tight = true)
      bounded("d_long", LongType, "0", tight = true)
      bounded("d_short", ShortType, "0", tight = true)
      bounded("d_byte", ByteType, "0", tight = true)
      bounded("d_str", StringType, "row0", tight = false)
      bounded("d_date", DateType, "2026-07-25", tight = true)
      bounded("d_ts", TimestampType, "2026-07-25 01:02:03.456", tight = false)
      bounded("d_ts_ntz", TimestampNTZType, "2026-07-25T01:02:03.456", tight = false)
      bounded("d_dec", DecimalType(9, 3), "0.000", tight = true)
      bounded("d_float", FloatType, "0.0", tight = true)
      bounded("d_double", DoubleType, "0.0", tight = true)
      bounded("d_nested_inner", IntegerType, "0", tight = true)
      boundless("d_bool")
      boundless("d_binary")
      boundless("d_arr")
    }
  }

  test("forRead reconstructs the stats JSON the delta log holds, for every type") {
    withAllTypesTable("amt_content_stats_roundtrip", numFiles = leafPackedFiles) { deltaLog =>
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
      val logged = spark.read.schema(loggedSchema)
        .json(new Path(deltaLog.logPath, "*.json").toString)
        .where(col("add").isNotNull)
        .select(col("add.stats"))
        .as[String].collect()
        .map(JsonUtils.mapper.readTree).toSet
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
    // representation for it yet, so `isBoundTypeSupported` rejects it. It is still counted for
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
      "tight_bounds" -> (BooleanType, "true"),
      "null_value_count" -> (LongType, "0")))
    assertStatFields(persisted, "v", Map("null_value_count" -> (LongType, "0")))
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

    val persisted = writeStats("""{"numRecords":7}""", metadata, protocol)
    // record_count alone reconstructs the stats Delta had.
    val restoredJson = readStats(persisted, metadata, protocol)
    assert(JsonUtils.mapper.readTree(restoredJson) ==
      JsonUtils.mapper.readTree("""{"numRecords":7}"""))
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
      "tight_bounds" -> (BooleanType, "true"),
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

  /** The persisted Iceberg `tight_bounds` of the sub-struct for logical column `column`. */
  private def persistedTightBounds(persisted: DataFrame, column: String): Boolean = {
    val sub = col("content_stats").getField(contentStatsField(persisted, column))
    persisted.select(sub.getField("tight_bounds")).as[Boolean].head()
  }

  /** The file-level `tightBounds` recorded in a Delta stats JSON, or None if absent. */
  private def tightBoundsOf(statsJson: String): Option[Boolean] =
    Option(JsonUtils.mapper.readTree(statsJson).get("tightBounds")).map(_.asBoolean)

  test("all columns have truncated type: Delta tightBounds is reconstructed conservatively false") {
    // The only bounded columns are string/timestamp. Each is forced to tight_bounds=false on write,
    // and on read there is no untruncated column to recover the file flag from, so it falls back
    // to false -- a safe precision loss even though the source file was tight.
    val metadata = metadataWithColumnIds(
      new StructType().add("s", StringType).add("ts", TimestampType))
    Seq(true, false).foreach { tight =>
      val persisted =
        writeStats(getSampleStatsJson(metadata, tightBounds = tight), metadata, amtProtocol)
      assert(!persistedTightBounds(persisted, "s"), "a string column is never tight")
      assert(!persistedTightBounds(persisted, "ts"), "a timestamp column is never tight")
      assert(tightBoundsOf(readStats(persisted, metadata, amtProtocol)).contains(false),
        "with no untruncated column to vouch for it, the file flag reconstructs to false")
    }
  }

  test("no truncated column types: the Delta tightBounds round-trips exactly") {
    // Every column is untruncated, so each carries the file flag verbatim and the
    // reconstructed flag matches -- tight and wide both round-trip unchanged.
    val metadata = metadataWithColumnIds(
      new StructType().add("id", LongType).add("n", IntegerType))
    Seq(true, false).foreach { tight =>
      val persisted =
        writeStats(getSampleStatsJson(metadata, tightBounds = tight), metadata, amtProtocol)
      assert(persistedTightBounds(persisted, "id") == tight)
      assert(persistedTightBounds(persisted, "n") == tight)
      assert(tightBoundsOf(readStats(persisted, metadata, amtProtocol)).contains(tight),
        s"an all-untruncated table must round-trip tightBounds=$tight unchanged")
    }
  }

  test("mixed column types: the Delta tightBounds round-trips via the untruncated column") {
    // A mix of an untruncated (long) and a truncated (string) column. The untruncated column
    // carries the file flag, the string is never tight, and the reconstructed flag comes back from
    // the untruncated column -- so both tight and wide round-trip unchanged.
    val metadata = metadataWithColumnIds(
      new StructType().add("id", LongType).add("name", StringType))
    Seq(true, false).foreach { tight =>
      val persisted =
        writeStats(getSampleStatsJson(metadata, tightBounds = tight), metadata, amtProtocol)
      assert(persistedTightBounds(persisted, "id") == tight,
        "the untruncated column carries the file flag")
      assert(!persistedTightBounds(persisted, "name"), "a string column is never tight")
      assert(tightBoundsOf(readStats(persisted, metadata, amtProtocol)).contains(tight),
        s"a mixed table must round-trip tightBounds=$tight via the untruncated column")
    }
  }

  test("reading back to Delta ignores a string column's Iceberg tight_bounds") {
    val metadata = metadataWithColumnIds(new StructType().add("name", StringType))
    // `forWrite` never marks a string tight, so this happens only when an external writer sets a
    // string column with tight_bounds=true.
    val persisted =
      writeStats(getSampleStatsJson(metadata, tightBounds = true), metadata, amtProtocol)
    val tampered = persisted.withColumn("content_stats",
      col("content_stats").withField(s"${contentStatsField(persisted, "name")}.tight_bounds",
        lit(true)))
    assert(persistedTightBounds(tampered, "name"), "the tampered Iceberg flag reads back as true")
    assert(tightBoundsOf(readStats(tampered, metadata, amtProtocol)).contains(false),
      "Delta ignores a truncated column's tightness, so the reconstructed flag stays false")
  }

  test("reading back to Delta ANDs untruncated columns, so a mix reconstructs tightBounds false") {
    // Two untruncated columns. A tight file makes both tight_bounds=true, but if a manifest carries
    // a mix (one column not tight), `forRead` ANDs the untruncated columns, so the reconstructed
    // Delta tightBounds is false even though the other column is still tight.
    val metadata = metadataWithColumnIds(
      new StructType().add("id", LongType).add("id2", LongType))
    val persisted =
      writeStats(getSampleStatsJson(metadata, tightBounds = true), metadata, amtProtocol)
    val tampered = persisted.withColumn("content_stats",
      col("content_stats").withField(s"${contentStatsField(persisted, "id2")}.tight_bounds",
        lit(false)))
    assert(persistedTightBounds(tampered, "id"), "the untouched column stays tight")
    assert(!persistedTightBounds(tampered, "id2"), "the tampered column is not tight")
    assert(tightBoundsOf(readStats(tampered, metadata, amtProtocol)).contains(false),
      "a mix of tight and non-tight untruncated columns reconstructs to false")
  }
}
