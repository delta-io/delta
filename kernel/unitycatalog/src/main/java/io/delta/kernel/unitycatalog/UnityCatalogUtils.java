/*
 * Copyright (2025) The Delta Lake Project Authors.
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

package io.delta.kernel.unitycatalog;

import static io.delta.kernel.commit.CatalogCommitterUtils.*;
import static io.delta.kernel.internal.DeltaErrors.wrapEngineExceptionThrowsIO;
import static io.delta.kernel.internal.util.Preconditions.checkState;
import static io.delta.kernel.internal.util.Utils.singletonCloseableIterator;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.delta.kernel.commit.CommitMetadata;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.actions.CommitInfo;
import io.delta.kernel.internal.actions.DomainMetadata;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.clustering.ClusteringMetadataDomain;
import io.delta.kernel.internal.types.DataTypeJsonSerDe;
import io.delta.kernel.internal.util.ColumnMapping;
import io.delta.kernel.internal.util.Tuple2;
import io.delta.kernel.types.*;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import io.delta.storage.commit.uccommitcoordinator.UCClient;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.*;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

public class UnityCatalogUtils {
  private UnityCatalogUtils() {}

  private static final String UC_PROP_CLUSTERING_COLUMNS = "clusteringColumns";
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  /** Read schema for parsing delta files. */
  private static final StructType DELTA_FILE_READ_SCHEMA =
      new StructType()
          .add("protocol", Protocol.FULL_SCHEMA)
          .add("metaData", Metadata.FULL_SCHEMA)
          .add("commitInfo", CommitInfo.FULL_SCHEMA)
          .add("domainMetadata", DomainMetadata.FULL_SCHEMA);

  /**
   * Parse a single delta file and extract its actions.
   *
   * <p>Reads the delta file using the provided Engine's JsonHandler and extracts:
   *
   * <ul>
   *   <li>Protocol action (if present)
   *   <li>Metadata action (if present)
   *   <li>CommitInfo action (if present)
   *   <li>DomainMetadata actions (zero or more)
   * </ul>
   *
   * @param engine the engine to use for reading the JSON delta file
   * @param deltaFile the delta file to parse
   * @return a {@link ParsedDeltaFileContents} containing the extracted actions
   */
  public static ParsedDeltaFileContents parseDeltaFileContents(
      Engine engine, FileStatus deltaFile) {
    Protocol protocol = null;
    Metadata metadata = null;
    CommitInfo commitInfo = null;
    List<DomainMetadata> domainMetadatas = new ArrayList<>();

    try (CloseableIterator<ColumnarBatch> batches =
        wrapEngineExceptionThrowsIO(
            () ->
                engine
                    .getJsonHandler()
                    .readJsonFiles(
                        singletonCloseableIterator(deltaFile),
                        DELTA_FILE_READ_SCHEMA,
                        Optional.empty()),
            "Parsing delta file %s with readSchema=%s",
            deltaFile.getPath(),
            DELTA_FILE_READ_SCHEMA)) {

      while (batches.hasNext()) {
        final ColumnarBatch batch = batches.next();

        if (protocol == null) {
          protocol = findFirstAction(batch.getColumnVector(0), Protocol::fromColumnVector);
        }

        if (metadata == null) {
          metadata = findFirstAction(batch.getColumnVector(1), Metadata::fromColumnVector);
        }

        if (commitInfo == null) {
          commitInfo = findFirstAction(batch.getColumnVector(2), CommitInfo::fromColumnVector);
        }

        final ColumnVector domainMetadataVector = batch.getColumnVector(3);
        for (int i = 0; i < domainMetadataVector.getSize(); i++) {
          DomainMetadata dm = DomainMetadata.fromColumnVector(domainMetadataVector, i);
          if (dm != null) {
            domainMetadatas.add(dm);
          }
        }
      }
    } catch (IOException ex) {
      throw new UncheckedIOException("Failed to parse delta file: " + deltaFile.getPath(), ex);
    }

    return new ParsedDeltaFileContents(
        Optional.ofNullable(protocol),
        Optional.ofNullable(metadata),
        Optional.ofNullable(commitInfo),
        domainMetadatas);
  }

  /**
   * Scans a column vector in the batch and returns the first non-null action found, or null if none
   * exists.
   */
  private static <T> T findFirstAction(
      ColumnVector vector, BiFunction<ColumnVector, Integer, T> extractor) {
    for (int i = 0; i < vector.getSize(); i++) {
      T value = extractor.apply(vector, i);
      if (value != null) {
        return value;
      }
    }
    return null;
  }

  /**
   * Extract all properties that should be sent to Unity Catalog when creating a table (version 0).
   *
   * <p>This method extracts:
   *
   * <ul>
   *   <li>All table properties from the metadata configuration
   *   <li>Protocol-derived properties (e.g., delta.minReaderVersion=3, delta.feature.XXX=supported)
   *   <li>UC-specific properties (delta.lastUpdateVersion, delta.lastCommitTimestamp)
   *   <li>Clustering properties if a clustering domain metadata is present
   * </ul>
   *
   * @param engine the engine to use for I/O operations (to retrieve the commit timestamp)
   * @param postCreateSnapshot the snapshot after version 0 has been written
   * @return a map of properties to send to Unity Catalog
   * @throws IllegalArgumentException if the snapshot is not version 0
   */
  public static Map<String, String> getPropertiesForCreate(
      Engine engine, SnapshotImpl postCreateSnapshot) {
    if (postCreateSnapshot.getVersion() != 0) {
      throw new IllegalArgumentException(
          String.format(
              "Expected a snapshot at version 0, but got a snapshot at version %d",
              postCreateSnapshot.getVersion()));
    }
    return assembleCreateProperties(
        postCreateSnapshot.getTableProperties(),
        postCreateSnapshot.getProtocol(),
        postCreateSnapshot.getVersion(),
        postCreateSnapshot.getTimestamp(engine),
        extractClusteringProperties(postCreateSnapshot));
  }

  /**
   * Extract clustering properties from the snapshot.
   *
   * <p>Converts physical clustering columns to logical column names and serializes them as a JSON
   * array of arrays for the "clusteringColumns" property.
   *
   * <p>Examples:
   *
   * <ul>
   *   <li>Not clustered: returns empty map (no "clusteringColumns" property)
   *   <li>Clustered with empty list: returns {"clusteringColumns": "[]"}
   *   <li>Clustered with columns: physical column "col-abcd-1234" maps to nested logical column
   *       "address.city" and is serialized as {"clusteringColumns": "[["address", "city"]]"}
   * </ul>
   *
   * @return clustering properties if present, otherwise empty map
   */
  private static Map<String, String> extractClusteringProperties(SnapshotImpl snapshot) {
    return snapshot
        .getPhysicalClusteringColumns()
        .map(cols -> serializeClusteringColumns(cols, snapshot.getSchema()))
        .orElse(Collections.emptyMap());
  }

  /** Assembles the UC property map from already-extracted primitives. */
  private static Map<String, String> assembleCreateProperties(
      Map<String, String> tableConfig,
      Protocol protocol,
      long version,
      long commitTimestamp,
      Map<String, String> clusteringProperties) {
    final Map<String, String> properties = new HashMap<>();

    // Case 1: All table properties from metadata.configuration
    properties.putAll(tableConfig);

    // Case 2: Protocol-derived properties
    properties.putAll(extractProtocolProperties(protocol));

    // Case 3: UC-specific properties
    properties.put(METASTORE_LAST_UPDATE_VERSION, String.valueOf(version));
    properties.put(METASTORE_LAST_COMMIT_TIMESTAMP, String.valueOf(commitTimestamp));

    // Case 4: Clustering properties if present
    properties.putAll(clusteringProperties);

    return properties;
  }

  /**
   * Converts physical clustering columns to logical column names and serializes them as a JSON
   * property map. Shared by both the SnapshotImpl and DomainMetadata extraction paths.
   */
  private static Map<String, String> serializeClusteringColumns(
      List<Column> physicalClusteringCols, StructType schema) {
    final List<List<String>> logicalClusteringCols =
        physicalClusteringCols.stream()
            .map(
                physicalCol -> {
                  final Tuple2<Column, DataType> logicalColumnAndType =
                      ColumnMapping.getLogicalColumnNameAndDataType(schema, physicalCol);
                  return Arrays.asList(logicalColumnAndType._1.getNames());
                })
            .collect(Collectors.toList());
    try {
      return Map.of(
          UC_PROP_CLUSTERING_COLUMNS, OBJECT_MAPPER.writeValueAsString(logicalClusteringCols));
    } catch (JsonProcessingException ex) {
      throw new RuntimeException("Failed to serialize clustering columns to JSON", ex);
    }
  }

  // ---------------------------------------------------------------------------
  // CommitMetadata overload (used by the committer's createImpl path,
  // which has no post-commit snapshot)
  // ---------------------------------------------------------------------------

  /**
   * Same as {@link #getPropertiesForCreate(Engine, SnapshotImpl)} but reads from CommitMetadata.
   */
  public static Map<String, String> getPropertiesForCreate(CommitMetadata commitMetadata) {
    if (commitMetadata.getVersion() != 0) {
      throw new IllegalArgumentException(
          String.format(
              "Expected commit metadata at version 0, but got version %d",
              commitMetadata.getVersion()));
    }
    // Defensive: CommitMetadata constructor already validates ICT is present for
    // catalog-managed tables, but we guard here since this is a public method.
    checkState(
        commitMetadata.getCommitInfo().getInCommitTimestamp().isPresent(),
        "InCommitTimestamp must be present for version 0 catalog-managed table creation");
    return assembleCreateProperties(
        commitMetadata.getEffectiveMetadata().getConfiguration(),
        commitMetadata.getEffectiveProtocol(),
        commitMetadata.getVersion(),
        commitMetadata.getCommitInfo().getInCommitTimestamp().get(),
        extractClusteringProperties(
            commitMetadata.getCommitDomainMetadatas(),
            commitMetadata.getEffectiveMetadata().getSchema()));
  }

  /** Extracts clustering columns from domain metadata (used when no snapshot is available). */
  private static Map<String, String> extractClusteringProperties(
      List<DomainMetadata> domainMetadatas, StructType schema) {
    return domainMetadatas.stream()
        .filter(dm -> ClusteringMetadataDomain.DOMAIN_NAME.equals(dm.getDomain()))
        .filter(dm -> !dm.isRemoved())
        .findFirst()
        .map(dm -> ClusteringMetadataDomain.fromJsonConfiguration(dm.getConfiguration()))
        .map(domain -> serializeClusteringColumns(domain.getClusteringColumns(), schema))
        .orElse(Collections.emptyMap());
  }

  // ---------------------------------------------------------------------------
  // UC column definition conversion from Kernel schema
  // ---------------------------------------------------------------------------

  /**
   * Converts a Kernel {@link StructType} schema to a list of UC {@link UCClient.ColumnDef}
   * definitions suitable for the Unity Catalog createTable API.
   */
  public static List<UCClient.ColumnDef> toColumnDefs(StructType schema) {
    List<StructField> fields = schema.fields();
    List<UCClient.ColumnDef> defs = new ArrayList<>(fields.size());
    for (int i = 0; i < fields.size(); i++) {
      StructField f = fields.get(i);
      defs.add(
          new UCClient.ColumnDef(
              f.getName(),
              toUCTypeName(f.getDataType()),
              f.getDataType().toString(),
              DataTypeJsonSerDe.serializeDataType(f.getDataType()),
              f.isNullable(),
              i));
    }
    return defs;
  }

  /**
   * Maps a Kernel {@link DataType} to the corresponding Unity Catalog {@code ColumnTypeName} string
   * value.
   */
  static String toUCTypeName(DataType type) {
    if (type instanceof BooleanType) return "BOOLEAN";
    if (type instanceof ByteType) return "BYTE";
    if (type instanceof ShortType) return "SHORT";
    if (type instanceof IntegerType) return "INT";
    if (type instanceof LongType) return "LONG";
    if (type instanceof FloatType) return "FLOAT";
    if (type instanceof DoubleType) return "DOUBLE";
    if (type instanceof DateType) return "DATE";
    if (type instanceof TimestampType) return "TIMESTAMP";
    if (type instanceof TimestampNTZType) return "TIMESTAMP_NTZ";
    if (type instanceof StringType) return "STRING";
    if (type instanceof BinaryType) return "BINARY";
    if (type instanceof VariantType) return "VARIANT";
    if (type instanceof DecimalType) return "DECIMAL";
    if (type instanceof ArrayType) return "ARRAY";
    if (type instanceof MapType) return "MAP";
    if (type instanceof StructType) return "STRUCT";
    throw new UnsupportedOperationException(
        "No UC ColumnTypeName mapping for Kernel type: " + type);
  }
}
