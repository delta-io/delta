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
package io.delta.spark.internal.v2.read.metadata;

import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.rowtracking.MaterializedRowTrackingColumn;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Optional;
import org.apache.spark.sql.catalyst.expressions.MetadataStructFieldWithLogicalName$;
import org.apache.spark.sql.delta.DeltaParquetFileFormatBase;
import org.apache.spark.sql.delta.RowCommitVersion$;
import org.apache.spark.sql.delta.RowId$;
import org.apache.spark.sql.execution.datasources.FileFormat$;
import org.apache.spark.sql.execution.datasources.PartitionedFile;
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Function1;
import scala.collection.immutable.Map;
import scala.collection.immutable.Seq;

/**
 * Schema context for the DSv2 {@code _metadata} struct column.
 *
 * <p>Single owner of everything needed to materialise the pruned {@code _metadata} struct at read
 * time:
 *
 * <ul>
 *   <li>the pruned struct itself ({@link #getPrunedMetadataStruct()}), parsed out of the requested
 *       read schema;
 *   <li>the physical Parquet read schema ({@link #getParquetReadSchema()}), augmented with
 *       row-tracking helper columns when {@code _metadata.row_id} or {@code
 *       _metadata.row_commit_version} are requested;
 *   <li>data / partition projection ordinals that account for the helper columns inserted between
 *       data and partition columns;
 *   <li>an ordered array of {@link MetadataValueSetterBuilder}s, one per pruned-struct field, that
 *       {@link MetadataStructReadFunction} runs per row to materialise each metadata value.
 * </ul>
 *
 * <p>Constructed via the static {@link #forSchema} factory which returns {@code Optional.empty()}
 * when the requested read schema does not contain a top-level {@code _metadata} struct - the caller
 * therefore avoids paying any context-construction cost when the scan does not request {@code
 * _metadata} at all.
 *
 * <p>Row tracking is modelled as two specialised setters ({@link RowIdValueSetterBuilder}, {@link
 * RowCommitVersionValueSetterBuilder}) registered alongside generic {@link
 * FileConstantValueSetterBuilder}s for Spark's file-source base fields and any Delta-specific
 * extractors exposed by {@link DeltaParquetFileFormatBase#fileConstantMetadataExtractors}. There is
 * no separate row-tracking schema/resolver class - the row-tracking concern lives entirely inside
 * its setter implementations.
 */
public class MetadataStructSchemaContext implements Serializable {

  /**
   * The canonical *logical* name of the file-source metadata struct ({@code _metadata}).
   *
   * <p>Do NOT use this to find the metadata struct in a read schema by physical field name (e.g.
   * {@code METADATA_COLUMN_NAME.equals(field.name())}). When a user column named {@code _metadata}
   * collides with the struct, the struct is physically renamed by prepending underscores until
   * unique (e.g. {@code __metadata}, or further) while keeping this logical name in its field
   * metadata, so a physical-name match silently misses it.
   * Use {@link #isFileSourceMetadataStruct(StructField)} for identification instead.
   *
   * <p>It is safe to use directly only as the logical-name constant itself: comparing against a
   * logical name recovered from field metadata (as {@code isFileSourceMetadataStruct} does), or
   * when producing / requesting the struct by its canonical name (the struct's physical name equals
   * this whenever there is no collision).
   */
  private static final String METADATA_COLUMN_NAME = FileFormat$.MODULE$.METADATA_NAME();
  private static final String ROW_ID_FIELD_NAME = RowId$.MODULE$.ROW_ID();
  private static final String ROW_COMMIT_VERSION_FIELD_NAME =
      RowCommitVersion$.MODULE$.METADATA_STRUCT_FIELD_NAME();

  private final StructType prunedMetadataStruct;
  private final StructType parquetReadSchema;
  private final StructType dataSchema;
  private final StructType partitionSchema;
  private final Seq<Object> dataColumnsOrdinals;
  private final Seq<Object> partitionColumnsOrdinals;
  private final MetadataValueSetterBuilder[] valueSetterBuilders;

  /**
   * Builds a context iff {@code readDataSchema} contains a top-level {@code _metadata} struct.
   *
   * @param readDataSchema the requested read schema, including {@code _metadata} when requested
   * @param partitionSchema the partition columns appended by Spark after the inner read row
   * @param deltaFormat the Delta Parquet format whose {@link
   *     DeltaParquetFileFormatBase#fileConstantMetadataExtractors} drives the file-constant setters
   * @param tableMetadata Kernel table metadata used to resolve the materialised row-tracking
   *     helper-column physical names when row tracking fields are requested
   * @return a present {@code MetadataStructSchemaContext} when {@code _metadata} is requested,
   *     otherwise {@code Optional.empty()}
   */
  public static Optional<MetadataStructSchemaContext> forSchema(
      StructType readDataSchema,
      StructType partitionSchema,
      DeltaParquetFileFormatBase deltaFormat,
      Metadata tableMetadata) {
    return forSchemaWithExtractors(
        readDataSchema,
        partitionSchema,
        deltaFormat.fileConstantMetadataExtractors(),
        tableMetadata);
  }

  /**
   * Internal factory variant that takes the file-constant extractors map directly. Visible for
   * testing - production code should always use {@link #forSchema} which sources the map from
   * {@link DeltaParquetFileFormatBase#fileConstantMetadataExtractors}.
   */
  static Optional<MetadataStructSchemaContext> forSchemaWithExtractors(
      StructType readDataSchema,
      StructType partitionSchema,
      Map<String, Function1<PartitionedFile, Object>> extractors,
      Metadata tableMetadata) {
    StructField metadataColumn =
        Arrays.stream(readDataSchema.fields())
            .filter(MetadataStructSchemaContext::isFileSourceMetadataStruct)
            .findFirst()
            .orElse(null);
    if (metadataColumn == null || !(metadataColumn.dataType() instanceof StructType)) {
      return Optional.empty();
    }
    StructType prunedMetadata = (StructType) metadataColumn.dataType();
    // Exclude the metadata struct from the base schema by its actual physical name, not the logical
    // `_metadata`: on a collision the struct is renamed (e.g. `__metadata`) and a user column keeps
    // the physical `_metadata`, so filtering by the logical name would drop the user's column.
    StructType baseSchema =
        new StructType(
            Arrays.stream(readDataSchema.fields())
                .filter(f -> !metadataColumn.name().equals(f.name()))
                .toArray(StructField[]::new));
    return Optional.of(
        new MetadataStructSchemaContext(
            prunedMetadata, baseSchema, partitionSchema, extractors, tableMetadata));
  }

  private MetadataStructSchemaContext(
      StructType prunedMetadata,
      StructType baseSchema,
      StructType partitionSchema,
      Map<String, Function1<PartitionedFile, Object>> extractors,
      Metadata tableMetadata) {
    this.prunedMetadataStruct = prunedMetadata;
    this.dataSchema = baseSchema;
    this.partitionSchema = partitionSchema;

    boolean rowIdRequested = containsField(prunedMetadata, ROW_ID_FIELD_NAME);
    boolean rowCommitVersionRequested =
        containsField(prunedMetadata, ROW_COMMIT_VERSION_FIELD_NAME);

    StructType augmented = baseSchema;
    int materializedRowIdIdx = -1;
    int rowIndexIdx = -1;
    int materializedRowCommitVersionIdx = -1;

    if (rowIdRequested) {
      String rowIdColumnName =
          MaterializedRowTrackingColumn.MATERIALIZED_ROW_ID.getPhysicalColumnName(
              tableMetadata.getConfiguration());
      augmented = augmented.add(rowIdColumnName, DataTypes.LongType, true);
      materializedRowIdIdx = augmented.fields().length - 1;

      augmented =
          augmented.add(
              ParquetFileFormat.ROW_INDEX_TEMPORARY_COLUMN_NAME(), DataTypes.LongType, true);
      rowIndexIdx = augmented.fields().length - 1;
    }
    if (rowCommitVersionRequested) {
      String rowCommitVersionColumnName =
          MaterializedRowTrackingColumn.MATERIALIZED_ROW_COMMIT_VERSION.getPhysicalColumnName(
              tableMetadata.getConfiguration());
      augmented = augmented.add(rowCommitVersionColumnName, DataTypes.LongType, true);
      materializedRowCommitVersionIdx = augmented.fields().length - 1;
    }
    this.parquetReadSchema = augmented;

    int dataLen = baseSchema.fields().length;
    int partitionStart = augmented.fields().length;
    this.dataColumnsOrdinals = buildRangeOrdinals(0, dataLen);
    this.partitionColumnsOrdinals =
        buildRangeOrdinals(partitionStart, partitionStart + partitionSchema.fields().length);

    this.valueSetterBuilders =
        buildValueSetters(
            prunedMetadata,
            extractors,
            materializedRowIdIdx,
            rowIndexIdx,
            materializedRowCommitVersionIdx);
  }

  /**
   * Builds the per-field setter array. Each pruned-struct field maps to either a {@link
   * FileConstantValueSetterBuilder} (driven by {@code
   * DeltaParquetFileFormatBase#fileConstantMetadataExtractors}) or one of the row-tracking setters.
   * Throws {@link IllegalStateException} for any unsupported subfield name - fail-fast at context
   * construction rather than per-row.
   */
  private static MetadataValueSetterBuilder[] buildValueSetters(
      StructType prunedMetadata,
      Map<String, Function1<PartitionedFile, Object>> extractors,
      int materializedRowIdIdx,
      int rowIndexIdx,
      int materializedRowCommitVersionIdx) {
    StructField[] fields = prunedMetadata.fields();
    MetadataValueSetterBuilder[] setters = new MetadataValueSetterBuilder[fields.length];
    for (int i = 0; i < fields.length; i++) {
      String name = fields[i].name();
      if (ROW_ID_FIELD_NAME.equals(name)) {
        setters[i] = new RowIdValueSetterBuilder(materializedRowIdIdx, rowIndexIdx);
      } else if (ROW_COMMIT_VERSION_FIELD_NAME.equals(name)) {
        setters[i] = new RowCommitVersionValueSetterBuilder(materializedRowCommitVersionIdx);
      } else if (extractors.contains(name)) {
        setters[i] = new FileConstantValueSetterBuilder(extractors.apply(name));
      } else {
        throw new IllegalStateException(
            "Unsupported _metadata subfield '"
                + name
                + "': expected a file-source base metadata field exposed by "
                + "DeltaParquetFileFormatBase#fileConstantMetadataExtractors or a row-tracking field "
                + "(row_id, row_commit_version).");
      }
    }
    return setters;
  }

  /** Pruned {@code _metadata} struct type. Field order matches setter / output order. */
  public StructType getPrunedMetadataStruct() {
    return prunedMetadataStruct;
  }

  /**
   * Schema fed to the inner Parquet reader: data columns plus row-tracking helper columns when
   * {@code row_id} / {@code row_commit_version} are requested. The DSv2 read pipeline may further
   * augment this with a deletion-vector column outside the metadata context.
   */
  public StructType getParquetReadSchema() {
    return parquetReadSchema;
  }

  /**
   * Output type of the data-columns projection used to compose {@code data + _metadata +
   * partition}.
   */
  public StructType getDataSchema() {
    return dataSchema;
  }

  public Seq<Object> getDataColumnsOrdinals() {
    return dataColumnsOrdinals;
  }

  public StructType getPartitionSchema() {
    return partitionSchema;
  }

  public Seq<Object> getPartitionColumnsOrdinals() {
    return partitionColumnsOrdinals;
  }

  public boolean hasPartitionColumns() {
    return partitionSchema.fields().length > 0;
  }

  /**
   * Ordered setter array, one per pruned-struct field, used by {@link MetadataStructReadFunction}.
   */
  public MetadataValueSetterBuilder[] getValueSetterBuilders() {
    return valueSetterBuilders;
  }

  private static boolean containsField(StructType struct, String name) {
    return Arrays.stream(struct.fields()).anyMatch(field -> name.equals(field.name()));
  }

  /**
   * True if {@code field} is the file-source metadata struct (logical name {@code _metadata}),
   * matched by its Spark metadata-column marker so it is recognized even when renamed to avoid a
   * collision with a user column of the same name. On collision the physical name gains a leading
   * underscore repeatedly until it is unique (e.g. {@code __metadata}, or {@code ___metadata} if a
   * user column {@code __metadata} also exists); the logical name stays {@code _metadata}.
   *
   * <p>This is the single identification point for the metadata struct in the DSv2 read path. New
   * read-path sites that need to locate the struct must call this rather than matching on the
   * physical field name (see {@link #METADATA_COLUMN_NAME}).
   */
  public static boolean isFileSourceMetadataStruct(StructField field) {
    scala.Option<scala.Tuple2<StructField, String>> matched =
        MetadataStructFieldWithLogicalName$.MODULE$.unapply(field);
    return matched.isDefined() && METADATA_COLUMN_NAME.equals(matched.get()._2());
  }

  private static Seq<Object> buildRangeOrdinals(int startInclusive, int endExclusive) {
    int len = Math.max(0, endExclusive - startInclusive);
    int[] ordinals = new int[len];
    for (int i = 0; i < len; i++) {
      ordinals[i] = startInclusive + i;
    }
    return scala.Predef.wrapIntArray(ordinals).toList();
  }
}
