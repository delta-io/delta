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

import java.io.File

import scala.collection.JavaConverters._

import org.apache.spark.sql.delta.DeltaColumnMapping
import org.apache.spark.sql.delta.util.JsonUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.format.converter.ParquetMetadataConverter
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.schema.GroupType

import org.apache.spark.sql.execution.datasources.parquet.ParquetUtils
import org.apache.spark.sql.types.{StructField, StructType}

/**
 * Pins the Iceberg V4 field ids an AMT checkpoint puts on disk:
 * `AMTSingleAction.persistedSchema` must reproduce the authoritative name-to-id map, and every leaf
 * and root footer written by the incremental and full write paths must carry those ids on its
 * parquet schema.
 */
class AMTFieldSpecSuite extends AMTCheckpointTestBase {

  /**
   * Collects field-id assignments from a Parquet footer as dotted paths. Intermediate LIST/MAP
   * groups (`list`, `key_value`) are omitted so paths match Spark/Iceberg names
   * (e.g. `split_offsets.element`).
   */
  private def actualFieldIdByName(group: GroupType): Map[String, Int] = {
    def collect(current: GroupType, prefix: Seq[String]): Map[String, Int] =
      current.getFields.asScala.foldLeft(Map.empty[String, Int]) { (acc, field) =>
        val skipInPath = field.getName == "list" || field.getName == "key_value"
        val path = if (skipInPath) prefix else prefix :+ field.getName
        val withOwn = Option(field.getId).map(_.intValue()) match {
          case Some(id) if !skipInPath => acc + (path.mkString(".") -> id)
          case _ => acc
        }
        if (field.isPrimitive) withOwn
        else withOwn ++ collect(field.asGroupType(), path)
      }

    collect(group, Seq.empty)
  }

  private def assertFieldIds(
      hadoopConf: Configuration,
      file: File,
      label: String,
      expected: Map[String, Int]): Unit = {
    val schema = ParquetFileReader.readFooter(
      hadoopConf, new Path(file.getAbsolutePath), ParquetMetadataConverter.NO_FILTER)
      .getFileMetaData.getSchema
    val actual = actualFieldIdByName(schema)
    assert(
      actual.values.toSet.size == actual.size,
      s"$label contains duplicate field ids: $actual")
    assert(
      actual == expected,
      s"$label field ids did not match the schema the writer derived\n" +
        s"  missing=${expected.toSet.diff(actual.toSet)}\n" +
        s"  unexpected=${actual.toSet.diff(expected.toSet)}")
  }

  /**
   * Collects the field-id assignments actually stamped on `AMTSingleAction.persistedSchema` as
   * dotted paths, mirroring [[actualFieldIdByName]]'s convention: a scalar/struct id comes from the
   * field's own `parquet.field.id`; list-element and map key/value ids come from the parent's
   * `parquet.field.nested.ids` (keyed `<field>.element` / `values.key` / `values.value`).
   */
  private def stampedFieldIdByName(schema: StructType): Map[String, Int] = {
    // Read the nested-id sub-metadata (an opaque `Metadata` whose entries are keyed by the
    // relative element/key/value path) via its public JSON form, since the key set is not known
    // to the reader and `Metadata`'s underlying map is not publicly accessible.
    def nestedIds(field: StructField): Map[String, Int] = {
      val key = DeltaColumnMapping.PARQUET_FIELD_NESTED_IDS_METADATA_KEY
      if (!field.metadata.contains(key)) Map.empty
      else {
        val json = field.metadata.getMetadata(key).json
        JsonUtils.fromJson[Map[String, Long]](json).map { case (k, id) => k -> id.toInt }
      }
    }
    def collect(fields: Seq[StructField], prefix: Seq[String]): Map[String, Int] =
      fields.foldLeft(Map.empty[String, Int]) { (acc, field) =>
        val path = prefix :+ field.name
        val ownId =
          if (field.metadata.contains(ParquetUtils.FIELD_ID_METADATA_KEY)) {
            Map(path.mkString(".") ->
              field.metadata.getLong(ParquetUtils.FIELD_ID_METADATA_KEY).toInt)
          } else Map.empty[String, Int]
        // Nested (list-element / map key-value) ids are stamped as `<field>.<suffix>` relative to
        // the field name, so join them under the current prefix.
        val nested = nestedIds(field).map { case (rel, id) =>
          (prefix :+ rel).mkString(".") -> id
        }
        val children = field.dataType match {
          case s: StructType => collect(s.fields.toSeq, path)
          case _ => Map.empty[String, Int]
        }
        acc ++ ownId ++ nested ++ children
      }
    collect(schema.fields.toSeq, Seq.empty)
  }

  test("persistedSchema stamps an id on every mapped field of AMTSingleAction") {
    withAllTypesTable("amt_fieldid_schema", numFiles = 0) { deltaLog =>
      val snapshot = deltaLog.update()
      val metadata = snapshot.metadata
      val protocol = snapshot.protocol
      val stamped = stampedFieldIdByName(AMTSingleAction.persistedSchema(metadata, protocol))

      // Content-stats sub-structs are based at 10_000 + 200 * columnId, with the per-statistic
      // offsets from the Iceberg V4 column-stats proposal. The sub-struct carries the base id
      // (stats are resolved by id) and is named by the column's logical name with its field id
      // attached (see StatsLeaf.fieldName), so the name stays unique. An array is not
      // skipping-eligible, so it gets a null count and no bounds; a struct contributes its leaf.
      def statsIds(columnId: Int, name: String, withBounds: Boolean): Seq[(String, Int)] = {
        val base = 10000 + 200 * columnId
        val field = s"${name}_$columnId" // sub-struct field name: logical name + field id
        // Iceberg V4 offsets: lower_bound=1, upper_bound=2, tight_bounds=3, null_value_count=5.
        Seq(
          s"content_stats.$field" -> base,
          s"content_stats.$field.null_value_count" -> (base + 5)) ++
          (if (!withBounds) Nil
           else Seq(
             s"content_stats.$field.lower_bound" -> (base + 1),
             s"content_stats.$field.upper_bound" -> (base + 2),
             s"content_stats.$field.tight_bounds" -> (base + 3)))
      }
      // Partition fields take Iceberg ids from 1000 in partition-spec order, whatever their type.
      val expected = AMTSingleAction.allFieldIdByName ++ Map("partition" -> 102) ++
        partitionableTestColumns.map(_.structField("p_")).zipWithIndex.map {
          case (field, ordinal) => s"partition.${field.name}" -> (1000 + ordinal)
        } ++
        // Data columns are one per type in `allTypeColumns` order, so column ids run d_int=1 ..
        // d_double=12, d_binary=13, d_arr=14, d_nested=15 (inner=16).
        Seq(
          (1, "d_int"), (2, "d_long"), (3, "d_short"), (4, "d_byte"), (5, "d_str"),
          (6, "d_date"), (7, "d_ts"), (8, "d_ts_ntz"), (9, "d_dec"),
          (11, "d_float"), (12, "d_double"), (16, "d_nested_inner"))
          .flatMap { case (id, name) => statsIds(id, name, withBounds = true) } ++
        // Boolean, array, and binary are not skipping-eligible (no min/max): null count only.
        Seq((10, "d_bool"), (13, "d_binary"), (14, "d_arr"))
          .flatMap { case (id, name) => statsIds(id, name, withBounds = false) }
      assert(
        stamped == expected,
        "persistedSchema field ids did not match the expected map\n" +
          s"  missing=${expected.toSet.diff(stamped.toSet)}\n" +
          s"  unexpected=${stamped.toSet.diff(expected.toSet)}")

      // Guard against a field added to a nested id-bearing struct (tracking / deletion_vector /
      // manifest_info) without a corresponding id: every scalar of those structs must be stamped.
      val schema = AMTSingleAction.persistedSchema(metadata, protocol)
      Seq("tracking", "deletion_vector", "manifest_info").foreach { parent =>
        val struct = schema(parent).dataType.asInstanceOf[StructType]
        struct.fields.foreach { f =>
          assert(f.metadata.contains(ParquetUtils.FIELD_ID_METADATA_KEY),
            s"nested field '$parent.${f.name}' has no stamped Iceberg field id.")
        }
      }
    }
  }

  testAcrossAMTCheckpointScenarios(
      "writes stamp field ids on every leaf and root field",
      "amt_fieldid_write",
      sqlConfs = leafPackingConfs)(
      setup = name => appendRowsAsSeparateFiles(name, numFiles = leafPackedFiles - 1),
      inlineCheckpointTriggerActionsOrSQL = Some(name => Right(
        s"INSERT INTO $name VALUES (${leafPackedFiles - 1})"))) { context =>
    val hadoopConf = context.postCheckpointSnapshot.deltaLog.newDeltaHadoopConf()
    // `content_stats` is shaped per table, so the footer's ids can't match a static map. Compare
    // against the schema the writer actually derived from the metadata/protocol the checkpoint
    // recorded -- which is exactly what a reader projects with.
    val checkpointAction = context.provider.checkpointAction
    val expected = stampedFieldIdByName(AMTSingleAction.persistedSchema(
      checkpointAction.metaData, checkpointAction.protocol))
    // `leaf.location` and `contentRoot.path` are stored table-root-relative, so go through the
    // provider, which resolves them against the table root.
    val leaves = context.provider.liveLeafManifestAbsolutePaths.map(path => new File(path.toUri))
    val root = new File(context.provider.topLevelFiles.head.getPath.toUri)
    assert(leaves.nonEmpty, "Expected at least one leaf manifest.")
    leaves.foreach(f => assertFieldIds(hadoopConf, f, s"leaf ${f.getName}", expected))
    assertFieldIds(hadoopConf, root, s"root ${root.getName}", expected)
  }

  test("a full checkpoint rewrite refreshes content_stats names after a column rename") {
    withTable("amt_rename_rewrite") {
      // A huge checkpoint interval keeps the rename commit from auto-checkpointing, so the only
      // rewrites are the two explicit ones below.
      createAMTTable(
        "amt_rename_rewrite", tableSchema = "c LONG", checkpointInterval = Int.MaxValue)
      appendRowsAsSeparateFiles(
        "amt_rename_rewrite", numFiles = 2, columnExprs = Seq("CAST(id AS LONG)"))
      val deltaLog = deltaLogForName("amt_rename_rewrite")

      // The logical name of the single `content_stats` sub-struct on the latest checkpoint's
      // manifests. Sub-structs are named `<logical name>_<field id>` (the id keeps the name unique;
      // see StatsLeaf.fieldName), so strip the id suffix to compare the logical part that a rename
      // refreshes.
      def contentStatsNames(): Seq[String] = {
        val snapshot = deltaLog.update()
        val provider = amtProvider(snapshot).getOrElse(fail("expected AMTCheckpointProvider"))
        val manifestPaths =
          (provider.topLevelFiles.map(_.getPath.toString) ++
            provider.liveLeafManifestAbsolutePaths.map(_.toString)).distinct
        allowReadWithinDeltaLog {
          val manifestDf = spark.read.parquet(manifestPaths: _*)
          manifestDf.schema("content_stats").dataType.asInstanceOf[StructType]
            .fieldNames.toSeq.map(_.replaceAll("_\\d+$", ""))
        }
      }

      // The pre-rename checkpoint names the sub-struct by the original logical name.
      commitCheckpoint(deltaLog, incremental = false)
      assert(contentStatsNames() == Seq("c"))

      // A metadata-only rename does not rewrite the existing manifests, so the checkpoint on disk
      // still names the sub-struct by the old logical name until it is rewritten.
      sql("ALTER TABLE amt_rename_rewrite RENAME COLUMN c TO c_renamed")
      assert(contentStatsNames() == Seq("c"))

      // Forcing a full rewrite re-derives content_stats from the current metadata, so the
      // sub-struct picks up the new logical name (same field id).
      commitCheckpoint(deltaLog, incremental = false)
      assert(contentStatsNames() == Seq("c_renamed"))
    }
  }
}
