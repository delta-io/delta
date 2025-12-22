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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.util.ColumnMapping;
import io.delta.kernel.internal.util.Tuple2;
import io.delta.kernel.types.DataType;
import java.util.*;
import java.util.stream.Collectors;

public class UnityCatalogUtils {
  private UnityCatalogUtils() {}

  private static final String UC_PROP_CLUSTERING_COLUMNS = "clusteringColumns";
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

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

    final Map<String, String> properties = new HashMap<>();

    // Case 1: All table properties from metadata.configuration
    properties.putAll(postCreateSnapshot.getTableProperties());

    // Case 2: Protocol-derived properties
    properties.putAll(extractProtocolProperties(postCreateSnapshot.getProtocol()));

    // Case 3: UC-specific properties
    properties.put(METASTORE_LAST_UPDATE_VERSION, String.valueOf(postCreateSnapshot.getVersion()));
    properties.put(
        METASTORE_LAST_COMMIT_TIMESTAMP, String.valueOf(postCreateSnapshot.getTimestamp(engine)));

    // Case 4: Clustering properties if present
    properties.putAll(extractClusteringProperties(postCreateSnapshot));

    return properties;
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
        .map(
            physicalClusteringCols -> {
              // Convert physical to logical column names
              final List<List<String>> logicalClusteringCols =
                  physicalClusteringCols.stream()
                      .map(
                          physicalCol -> {
                            final Tuple2<Column, DataType> logicalColumnAndType =
                                ColumnMapping.getLogicalColumnNameAndDataType(
                                    snapshot.getSchema(), physicalCol);
                            final Column logicalColumn = logicalColumnAndType._1;
                            return Arrays.asList(logicalColumn.getNames());
                          })
                      .collect(Collectors.toList());

              // Serialize to JSON
              try {
                final String clusteringColumnsJson =
                    OBJECT_MAPPER.writeValueAsString(logicalClusteringCols);
                return Map.of(UC_PROP_CLUSTERING_COLUMNS, clusteringColumnsJson);
              } catch (JsonProcessingException ex) {
                throw new RuntimeException("Failed to serialize clustering columns to JSON", ex);
              }
            })
        .orElse(Collections.emptyMap());
  }
}
