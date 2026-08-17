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

  private def assertFieldIds(hadoopConf: Configuration, file: File, label: String): Unit = {
    val schema = ParquetFileReader.readFooter(
      hadoopConf, new Path(file.getAbsolutePath), ParquetMetadataConverter.NO_FILTER)
      .getFileMetaData.getSchema
    val actual = actualFieldIdByName(schema)
    assert(
      actual.values.toSet.size == actual.size,
      s"$label contains duplicate field ids: $actual")
    assert(
      actual == AMTSingleAction.allFieldIdByName,
      s"$label field ids did not match AMTSingleAction.allFieldIdByName\n" +
        s"  missing=${AMTSingleAction.allFieldIdByName.toSet.diff(actual.toSet)}\n" +
        s"  unexpected=${actual.toSet.diff(AMTSingleAction.allFieldIdByName.toSet)}")
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
    val stamped = stampedFieldIdByName(
      AMTSingleAction.persistedSchema(allTypesPartitionSchema))
    // Partition fields take Iceberg ids from 1000 in partition-spec order, whatever their type.
    val expected = AMTSingleAction.allFieldIdByName ++ Map("partition" -> 102) ++
      allTypesPartitionSchema.fields.zipWithIndex.map { case (field, ordinal) =>
        s"partition.${field.name}" -> (1000 + ordinal)
      }
    assert(
      stamped == expected,
      "persistedSchema field ids did not match AMTSingleAction.allFieldIdByName\n" +
        s"  missing=${expected.toSet.diff(stamped.toSet)}\n" +
        s"  unexpected=${stamped.toSet.diff(expected.toSet)}")

    // Guard against a field added to a nested id-bearing struct (tracking / deletion_vector /
    // manifest_info) without a corresponding id: every scalar of those structs must be stamped.
    val schema = AMTSingleAction.persistedSchema(allTypesPartitionSchema)
    Seq("tracking", "deletion_vector", "manifest_info").foreach { parent =>
      val struct = schema(parent).dataType.asInstanceOf[StructType]
      struct.fields.foreach { f =>
        assert(f.metadata.contains(ParquetUtils.FIELD_ID_METADATA_KEY),
          s"nested field '$parent.${f.name}' has no stamped Iceberg field id.")
      }
    }
  }

  testAcrossAMTCheckpointScenarios(
      "writes stamp field ids on every leaf and root field",
      "amt_fieldid_write",
      sqlConfs = leafPackingConfs)(
      setup = name => appendRowsAsSeparateFiles(name, numRows = leafPackedFiles - 1),
      inlineCheckpointTriggerActionsOrSQL = Some(name => Right(
        s"INSERT INTO $name VALUES (${leafPackedFiles - 1})"))) { context =>
    val hadoopConf = context.postCheckpointSnapshot.deltaLog.newDeltaHadoopConf()
    // `leaf.location` and `contentRoot.path` are stored table-root-relative, so go through the
    // provider, which resolves them against the table root.
    val leaves = context.provider.liveLeafManifestAbsolutePaths.map(path => new File(path.toUri))
    val root = new File(context.provider.topLevelFiles.head.getPath.toUri)
    assert(leaves.nonEmpty, "Expected at least one leaf manifest.")
    leaves.foreach(f => assertFieldIds(hadoopConf, f, s"leaf ${f.getName}"))
    assertFieldIds(hadoopConf, root, s"root ${root.getName}")
  }
}
