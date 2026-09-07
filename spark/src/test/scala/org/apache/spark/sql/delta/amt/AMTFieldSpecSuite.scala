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
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.actions.Metadata
import org.apache.spark.sql.delta.util.JsonUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.format.converter.ParquetMetadataConverter
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.schema.{GroupType, LogicalTypeAnnotation, MessageType, PrimitiveType, Type}

import org.apache.spark.SparkRuntimeException
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.datasources.parquet.ParquetUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{StructField, StructType}

/**
 * Pins the Iceberg V4 field ids an AMT checkpoint puts on disk:
 * `AMTSingleAction.persistedSchema` must reproduce the authoritative name-to-id map, and every leaf
 * and root footer written by the incremental and full write paths must carry those ids on its
 * parquet schema.
 */
class AMTFieldSpecSuite extends AMTCheckpointTestBase {

  private val tsExpr = allTypeColumns("ts").valueExpr
  private val tsNtzExpr = allTypeColumns("ts_ntz").valueExpr

  /**
   * Every field of a Parquet footer keyed by its dotted path. Intermediate LIST/MAP groups
   * (`list`, `key_value`) are omitted from the path so it matches Spark/Iceberg names
   * (e.g. `split_offsets.element`).
   */
  private def typesByPath(group: GroupType): Map[String, Type] = {
    def collect(current: GroupType, prefix: Seq[String]): Map[String, Type] =
      current.getFields.asScala.foldLeft(Map.empty[String, Type]) { (acc, field) =>
        val skipInPath = field.getName == "list" || field.getName == "key_value"
        val path = if (skipInPath) prefix else prefix :+ field.getName
        val withOwn = if (skipInPath) acc else acc + (path.mkString(".") -> field)
        if (field.isPrimitive) withOwn else withOwn ++ collect(field.asGroupType(), path)
      }

    collect(group, Seq.empty)
  }

  /** The Parquet footer schema of `file`. */
  private def footerSchema(hadoopConf: Configuration, file: File): MessageType =
    ParquetFileReader.readFooter(
      hadoopConf, new Path(file.getAbsolutePath), ParquetMetadataConverter.NO_FILTER)
      .getFileMetaData.getSchema

  /**
   * The [[StructType]] of manifest column `column`, read bare off the root manifest (the schema is
   * uniform across the tree, so the always-present root suffices).
   */
  private def onDiskColumnSchema(provider: AMTCheckpointProvider, column: String): StructType =
    allowReadWithinDeltaLog {
      spark.read.parquet(provider.topLevelFiles.map(_.getPath.toString): _*)
        .schema(column).dataType.asInstanceOf[StructType]
    }

  /**
   * Reconstructs the live AddFiles through the production read path, but under a checkpoint whose
   * metadata is `metadata` (so the read derives its requested, field-id-stamped schema from
   * `metadata` rather than the checkpoint's own). Returns the rows with a non-null `add`.
   */
  private def reconstructAddFiles(
      provider: AMTCheckpointProvider, deltaLog: DeltaLog, metadata: Metadata): DataFrame =
    new AMTCheckpointProvider(
        manifestCommitVersion = provider.manifestCommitVersion,
        checkpointAction = provider.checkpointAction.copy(metaData = metadata),
        leaves = provider.leaves,
        tableRoot = provider.tableRoot)
      .loadActionsForStateReconstruction(spark, deltaLog)
      .getOrElse(fail("expected reconstructed actions"))
      .where("add is not null")

  private def assertFieldIds(
      hadoopConf: Configuration,
      file: File,
      label: String,
      expectedIdByPath: Map[String, Int]): Unit = {
    val actualIdByPath = typesByPath(footerSchema(hadoopConf, file)).flatMap { case (path, field) =>
      Option(field.getId).map(id => path -> id.intValue())
    }
    assert(
      actualIdByPath.values.toSet.size == actualIdByPath.size,
      s"$label contains duplicate field ids: $actualIdByPath")
    assert(
      actualIdByPath == expectedIdByPath,
      s"$label field ids did not match the schema the writer derived\n" +
        s"  missing=${expectedIdByPath.toSet.diff(actualIdByPath.toSet)}\n" +
        s"  unexpected=${actualIdByPath.toSet.diff(expectedIdByPath.toSet)}")
  }

  /**
   * Collects the field-id assignments actually stamped on `AMTSingleAction.persistedSchema` as
   * dotted paths, mirroring [[typesByPath]]'s convention: a scalar/struct id comes from the
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
        onDiskColumnSchema(provider, "content_stats").fieldNames.toSeq
          .map(_.replaceAll("_\\d+$", ""))
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

  testAcrossAMTCheckpointScenarios(
      "content stats survive a column rename (field-id-based read)",
      "amt_rename",
      tableSchema = "col_to_be_renamed STRING, col_no_rename STRING")(
      setup = name => appendRowsAsSeparateFiles(
        name,
        numFiles = 2,
        columnExprs = Seq(
          "CONCAT('renamedVal', CAST(id AS STRING))",
          "CONCAT('keptVal', CAST(id AS STRING))")),
      inlineCheckpointTriggerActionsOrSQL = Some(name => Right(
        s"""INSERT INTO $name SELECT CONCAT('renamedVal', CAST(id AS STRING)),
           |CONCAT('keptVal', CAST(id AS STRING)) FROM range(2, 3)""".stripMargin))) { context =>
    val deltaLog = context.postCheckpointSnapshot.deltaLog
    val provider = context.provider

    // Reconstruct the live AddFiles under a checkpoint whose metadata is `meta`, projecting the
    // stats each file carries.
    def reconstructStats(meta: Metadata): Seq[String] =
      reconstructAddFiles(provider, deltaLog, meta)
        .select("add.stats").collect()
        .map(r => if (r.isNullAt(0)) "null" else r.getString(0)).toSeq

    // The sub-structs on disk are named `<pre-rename logical name>_<field id>` (a bare read
    // reflects the physical layout verbatim; content_stats keys each sub-struct by column id).
    val preRenameMeta = deltaLog.update().metadata
    val onDisk = onDiskColumnSchema(provider, "content_stats")
    val expectedSubStructs = preRenameMeta.dataSchema
      .map(f => s"${f.name}_${DeltaColumnMapping.getColumnId(f)}").toSet
    assert(onDisk.fieldNames.toSet == expectedSubStructs,
      s"on-disk sub-structs ${onDisk.fieldNames.toSet} != expected $expectedSubStructs")

    // Before the rename, reconstruction carries every file's stats for both columns.
    val statsBefore = reconstructStats(deltaLog.update().metadata)
    assert(statsBefore.nonEmpty && statsBefore.forall(_.contains("minValues")))
    assert(
      statsBefore.exists(_.contains("renamedVal")) && statsBefore.exists(_.contains("keptVal")),
      s"expected both columns' stats before rename: $statsBefore")

    // After the rename (same id and physical name, new logical name), the field-id read remaps
    // the on-disk `col_to_be_renamed` sub-struct onto the current `col_renamed`, so it still
    // succeeds -- identical for both columns (the stats JSON is keyed by physical name,
    // unchanged).
    sql(s"ALTER TABLE ${context.tableName} RENAME COLUMN col_to_be_renamed TO col_renamed")
    val statsAfter = reconstructStats(deltaLog.update().metadata)
    assert(statsAfter.sorted == statsBefore.sorted,
      s"stats changed across rename: $statsBefore -> $statsAfter")

    // Field-id read is what makes this work. With it off, the read falls back to
    // name-matching: the on-disk `content_stats` struct still matches by name, but its renamed
    // `col_to_be_renamed` sub-struct no longer matches the new `col_renamed`, so only that
    // column's stats are lost -- `col_no_rename` still resolves by name.
    val degraded = withSQLConf(SQLConf.PARQUET_FIELD_ID_READ_ENABLED.key -> "false") {
      reconstructStats(deltaLog.update().metadata)
    }
    assert(degraded.exists(_.contains("keptVal")),
      s"the un-renamed `col_no_rename` column's stats should survive name-matching: $degraded")
    assert(degraded.forall(!_.contains("renamedVal")),
      s"the renamed column's stats should be lost without field-id read: $degraded")
  }

  testAcrossAMTCheckpointScenarios(
      "partition values survive a column rename (field-id-based read)",
      "amt_part_rename",
      tableSchema = "id LONG, p STRING, keep_p STRING",
      partitionColumns = Seq("p", "keep_p"))(
      setup = name => appendRowsAsSeparateFiles(
        name,
        numFiles = 2,
        columnExprs = Seq(
          "CAST(id AS LONG)",
          "CONCAT('pRenamedVal', CAST(id AS STRING))",
          "CONCAT('pKeptVal', CAST(id AS STRING))")),
      inlineCheckpointTriggerActionsOrSQL = Some(name => Right(
        s"""INSERT INTO $name SELECT CAST(id AS LONG),
           |CONCAT('pRenamedVal', CAST(id AS STRING)),
           |CONCAT('pKeptVal', CAST(id AS STRING)) FROM range(2, 3)""".stripMargin))) { context =>
    val deltaLog = context.postCheckpointSnapshot.deltaLog
    val provider = context.provider

    // Reconstruct the live AddFiles under a checkpoint whose metadata is `meta`, projecting the
    // partition values each file carries.
    def reconstructPartitions(meta: Metadata): Seq[Map[String, String]] =
      reconstructAddFiles(provider, deltaLog, meta)
        .select("add.partitionValues").collect().toSeq
        .map(r =>
          if (r.isNullAt(0)) Map.empty[String, String]
          else r.getMap[String, String](0).toMap)

    // The on-disk partition fields are still named by the pre-rename logical column names.
    val onDisk = onDiskColumnSchema(provider, "partition")
    assert(onDisk.fieldNames.toSet == Set("p", "keep_p"))

    // Before the rename, reconstruction carries every file's partition values for both columns.
    val before = reconstructPartitions(deltaLog.update().metadata)
    assert(before.nonEmpty && before.forall(m => m.size == 2 && m.values.forall(_ != null)))
    val beforeValues = before.flatMap(_.values).toSet
    assert(beforeValues.exists(_.startsWith("pRenamedVal")) &&
      beforeValues.exists(_.startsWith("pKeptVal")),
      s"expected both partition columns' values before rename: $before")

    // After the rename (same id and physical name, new logical name), the field-id read remaps
    // the on-disk `p` field onto the current `p_renamed`, so reconstruction still succeeds --
    // identical for both columns (the value map is keyed by physical name, unchanged).
    sql(s"ALTER TABLE ${context.tableName} RENAME COLUMN p TO p_renamed")
    val after = reconstructPartitions(deltaLog.update().metadata)
    assert(
      after.sortBy(_.toString) == before.sortBy(_.toString),
      s"partition values changed across rename: $before -> $after")

    // Field-id read is what makes this work (see the content-stats test): with it off, the
    // read falls back to name-matching, so only the renamed `p` field is lost -- it reads null
    // under the new `p_renamed` name -- while `keep_p` still resolves by name.
    val degraded = withSQLConf(SQLConf.PARQUET_FIELD_ID_READ_ENABLED.key -> "false") {
      reconstructPartitions(deltaLog.update().metadata)
    }
    val degradedNonNull = degraded.flatMap(_.values).filter(_ != null).toSet
    assert(degradedNonNull.exists(_.startsWith("pKeptVal")),
      s"the un-renamed `keep_p` value should survive name-matching: $degraded")
    assert(!degradedNonNull.exists(_.startsWith("pRenamedVal")),
      s"the renamed partition value should be lost without field-id read: $degraded")
    assert(degraded.exists(_.values.exists(_ == null)),
      s"the renamed partition value should read null without field-id read: $degraded")
  }

  testAcrossAMTCheckpointScenarios(
      "manifest writes timestamps as int64 TIMESTAMP_MICROS, not INT96",
      "amt_ts",
      tableSchema =
        "d_ts TIMESTAMP, d_ts_ntz TIMESTAMP_NTZ, p_ts TIMESTAMP, p_ts_ntz TIMESTAMP_NTZ",
      partitionColumns = Seq("p_ts", "p_ts_ntz"))(
      setup = name => appendRowsAsSeparateFiles(
        name,
        numFiles = 2,
        columnExprs = Seq(tsExpr, tsNtzExpr, tsExpr, tsNtzExpr)),
      inlineCheckpointTriggerActionsOrSQL = Some(name => Right(
        s"""INSERT INTO $name SELECT $tsExpr, $tsNtzExpr, $tsExpr, $tsNtzExpr
           |FROM range(2, 3)""".stripMargin))) {
    context =>
    val deltaLog = context.postCheckpointSnapshot.deltaLog
    val provider = context.provider
    val hadoopConf = deltaLog.newDeltaHadoopConf()
    // This small table is not leaf-packed, so it is a single root-resident manifest; reading the
    // root covers every persisted timestamp value.
    val files = provider.topLevelFiles.map(f => new File(f.getPath.toUri))
    assert(files.nonEmpty, "Expected at least one manifest file.")

    // content_stats names each sub-struct `<column>_<field id>`; derive that suffix here.
    val tsMeta = deltaLog.update().metadata
    def cs(column: String): String =
      s"content_stats.${column}_${DeltaColumnMapping.getColumnId(tsMeta.schema(column))}"

    files.foreach { file =>
      val primitives = typesByPath(footerSchema(hadoopConf, file)).collect {
        case (path, field) if field.isPrimitive => path -> field.asPrimitiveType()
      }
      def assertTimestampMicros(name: String, adjustToUtc: Boolean): Unit = {
        val prim =
          primitives.getOrElse(name, fail(s"no primitive at '$name' in ${file.getName}"))
        assert(
          prim.getPrimitiveTypeName == PrimitiveType.PrimitiveTypeName.INT64,
          s"'$name' should be int64 micros, got ${prim.getPrimitiveTypeName} in ${file.getName}")
        prim.getLogicalTypeAnnotation match {
          case ts: LogicalTypeAnnotation.TimestampLogicalTypeAnnotation =>
            assert(
              ts.getUnit == LogicalTypeAnnotation.TimeUnit.MICROS,
              s"'$name' should be MICROS, got ${ts.getUnit} in ${file.getName}")
            assert(
              ts.isAdjustedToUTC == adjustToUtc,
              s"'$name' adjustToUtc should be $adjustToUtc in ${file.getName}")
          case other =>
            fail(s"'$name' should carry a timestamp annotation, got $other in ${file.getName}")
        }
      }
      // TimestampType -> adjust-to-utc; TimestampNTZType -> not. content_stats keys each
      // sub-struct by `<column>_<field id>`; partition keys by logical name.
      assertTimestampMicros(s"${cs("d_ts")}.lower_bound", adjustToUtc = true)
      assertTimestampMicros(s"${cs("d_ts")}.upper_bound", adjustToUtc = true)
      assertTimestampMicros(s"${cs("d_ts_ntz")}.lower_bound", adjustToUtc = false)
      assertTimestampMicros("partition.p_ts", adjustToUtc = true)
      assertTimestampMicros("partition.p_ts_ntz", adjustToUtc = false)
    }
  }

  test("field-id read recovers a renamed static manifest_info column on the root") {
    withTable("amt_static_rename") {
      withSQLConf(leafPackingConfs: _*) {
        createAMTTable("amt_static_rename", checkpointInterval = Int.MaxValue)
        appendRowsAsSeparateFiles("amt_static_rename", numFiles = leafPackedFiles)
        val deltaLog = deltaLogForName("amt_static_rename")
        commitCheckpoint(deltaLog, incremental = false)
        val provider = amtProvider(deltaLog.update())
          .getOrElse(fail("expected AMTCheckpointProvider"))
        assert(provider.leaves.nonEmpty, "Expected leaf pointers in the root.")
        val checkpoint = provider.checkpointAction
        val persistedSchema =
          AMTSingleAction.persistedSchema(checkpoint.metaData, checkpoint.protocol)

        // Read the root's physical columns verbatim (the persisted schema carries the field ids),
        // then rewrite the root with its static `manifest_info` struct renamed to
        // `manifest_info_new`, keeping the same Iceberg field id. A reader that matched columns by
        // name would now miss it entirely.
        val rootPath = provider.topLevelFiles.head.getPath.toString
        val rootRows = allowReadWithinDeltaLog {
          spark.read.schema(persistedSchema).parquet(rootPath).collect().toSeq
        }
        val renamedSchema = StructType(persistedSchema.map { field =>
          if (field.name == "manifest_info") field.copy(name = "manifest_info_new") else field
        })
        withTempDir { dir =>
          val rewrittenDir = new File(dir, "root-1-dash")
          withSQLConf(SQLConf.PARQUET_FIELD_ID_WRITE_ENABLED.key -> "true") {
            spark.createDataFrame(rootRows.asJava, renamedSchema)
              .coalesce(1).write.parquet(rewrittenDir.getAbsolutePath)
          }
          val rewrittenFile = Option(rewrittenDir.listFiles()).toSeq.flatten
            .find(_.getName.endsWith(".parquet"))
            .getOrElse(fail("no rewritten root parquet produced"))

          // The field really is renamed on disk (so a name-based read could not recover it).
          val rewrittenSchema = footerSchema(deltaLog.newDeltaHadoopConf(), rewrittenFile)
          assert(
            rewrittenSchema.containsField("manifest_info_new") &&
              !rewrittenSchema.containsField("manifest_info"),
            s"rewritten root should rename manifest_info: $rewrittenSchema")

          // Reading the rewritten root through the production path recovers every leaf pointer
          // unchanged: `manifest_info` is resolved by its Iceberg field id, not its name. (If it
          // were not, the DATA_MANIFEST rows would unwrap with a missing `manifest_info`.)
          val rewrittenCheckpoint = checkpoint.copy(
            contentRoot = checkpoint.contentRoot.copy(
              path = rewrittenFile.getAbsolutePath,
              sizeInBytes = rewrittenFile.length()))
          val rewrittenProvider = AMTCheckpointProvider.fromCheckpoint(
            deltaLog, rewrittenCheckpoint, provider.manifestCommitVersion)
          assertLeavesEqual(rewrittenProvider.leaves, provider.leaves)

          // Field-id read is what makes this work: with it off, the read falls back to
          // name-matching, so the requested `manifest_info` no longer matches the on-disk
          // `manifest_info_new` and reads null. Decoding the manifest pointer then fails when
          // `AMTSingleAction.validate` rejects a DATA_MANIFEST entry with no `manifest_info`, and
          // the row decode surfaces it as EXPRESSION_DECODING_FAILED wrapping that requirement.
          withSQLConf(SQLConf.PARQUET_FIELD_ID_READ_ENABLED.key -> "false") {
            val error = intercept[SparkRuntimeException] {
              AMTCheckpointProvider.fromCheckpoint(
                deltaLog, rewrittenCheckpoint, provider.manifestCommitVersion)
            }
            assert(error.getCondition == "EXPRESSION_DECODING_FAILED",
              s"Expected an EXPRESSION_DECODING_FAILED error, got ${error.getCondition}.")
            assert(error.getCause.isInstanceOf[IllegalArgumentException],
              s"Expected the decode to fail on the validate requirement, got ${error.getCause}.")
            assert(
              error.getCause.getMessage.contains(
                "manifest_info must be set iff content_type is a manifest pointer"),
              s"Expected a missing manifest_info requirement failure, " +
                s"got ${error.getCause.getMessage}.")
          }
        }
      }
    }
  }
}
