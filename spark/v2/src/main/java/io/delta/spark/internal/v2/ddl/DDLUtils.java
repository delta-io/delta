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
package io.delta.spark.internal.v2.ddl;

import io.delta.kernel.Meta;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.transaction.DataLayoutSpec;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.StructType;
import io.delta.storage.commit.uccommitcoordinator.UCCommitCoordinatorClient;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.expressions.IdentityTransform;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.delta.skipping.clustering.temp.ClusterByTransform;

/**
 * Shared utilities for DDL operations (CREATE TABLE, REPLACE TABLE, ALTER TABLE). Stateless — all
 * inputs are passed as parameters.
 */
public final class DDLUtils {

  private DDLUtils() {}

  public static final String ENGINE_INFO = "Delta-Spark-DSv2/" + Meta.KERNEL_VERSION;

  private static final Set<String> DSV2_INTERNAL_KEYS =
      Set.of(
          TableCatalog.PROP_LOCATION,
          TableCatalog.PROP_PROVIDER,
          TableCatalog.PROP_OWNER,
          TableCatalog.PROP_EXTERNAL,
          // V1 (AbstractDeltaCatalog) does not strip this key, which is a latent bug — it gets
          // persisted as a spurious table property in the Delta log. We fix this in V2.
          TableCatalog.PROP_IS_MANAGED_LOCATION,
          "path",
          "option.path");

  private static final Set<String> KERNEL_UNSUPPORTED_PROTOCOL_KEYS =
      Set.of("delta.minReaderVersion", "delta.minWriterVersion", "delta.ignoreProtocolDefaults");

  /**
   * Extracts the table description from DSv2 properties. Kernel's transaction builders do not yet
   * expose a {@code withDescription()} API, so the comment is preserved in {@link
   * DDLRequestContext#comment()} for when that becomes available. TODO(#6473): Write comment to
   * Delta log once Kernel exposes withDescription().
   */
  public static Optional<String> extractComment(Map<String, String> properties) {
    String comment = properties.get(TableCatalog.PROP_COMMENT);
    return Optional.ofNullable(comment).filter(c -> !c.isEmpty());
  }

  /**
   * Strips properties that should not be passed to Kernel's transaction builders:
   *
   * <ul>
   *   <li>DSv2 plumbing keys (location, provider, owner, etc.) — catalog metadata, not Delta table
   *       properties. The V1 path strips these identically in {@code AbstractDeltaCatalog}.
   *   <li>Protocol version overrides (minReaderVersion, minWriterVersion, ignoreProtocolDefaults) —
   *       throws {@link UnsupportedOperationException} if present. The V1 path passes these through
   *       to its own protocol resolution, but Kernel resolves protocol internally and does not
   *       accept them. We fail loudly rather than silently ignoring the user's intent.
   *   <li>UC table ID — internal routing marker consumed by {@link UCCommitCoordinatorClient}, not
   *       a user-visible table property.
   * </ul>
   *
   * User-facing table properties and Delta feature flags (e.g. {@code delta.feature.*}) pass
   * through.
   */
  public static Map<String, String> filterProperties(Map<String, String> properties) {
    // Fail loudly if the user specified protocol version overrides. Kernel resolves protocol
    // internally and rejects these via TableConfig.validateAndNormalizeDeltaProperties. Rather
    // than letting Kernel throw a confusing error, we catch them here with a clear message.
    // The V1 path passes these through to its own protocol resolution, so this is a deliberate
    // divergence until Kernel supports protocol overrides natively.
    for (String key : KERNEL_UNSUPPORTED_PROTOCOL_KEYS) {
      if (properties.containsKey(key)) {
        throw new UnsupportedOperationException(
            "Protocol version overrides are not yet supported in the V2 connector: "
                + key
                + ". Remove this property or use the V1 connector.");
      }
    }
    Map<String, String> result = new HashMap<>(properties);
    DSV2_INTERNAL_KEYS.forEach(result::remove);
    result.remove(TableCatalog.PROP_COMMENT);
    result.remove(UCCommitCoordinatorClient.UC_TABLE_ID_KEY);
    return result;
  }

  /**
   * Converts Spark DSv2 {@link Transform} array into a Kernel {@link DataLayoutSpec}. Spark
   * represents both partitioning (identity transforms) and clustering ({@link ClusterByTransform})
   * through the same {@code Transform[]} parameter; this method disambiguates and converts to the
   * corresponding Kernel representation.
   */
  public static DataLayoutSpec toDataLayoutSpec(Transform[] partitions) {
    if (partitions == null || partitions.length == 0) {
      return DataLayoutSpec.noDataLayout();
    }

    if (partitions[0] instanceof ClusterByTransform) {
      return processClustering(partitions);
    }

    return processPartitioning(partitions);
  }

  private static DataLayoutSpec processClustering(Transform[] partitions) {
    if (partitions.length > 1) {
      throw new IllegalArgumentException(
          "CLUSTER BY cannot be combined with other transforms; got " + partitions.length);
    }
    ClusterByTransform clusterBy = (ClusterByTransform) partitions[0];
    List<Column> columns = new ArrayList<>(clusterBy.columnNames().size());
    for (NamedReference ref :
        scala.jdk.javaapi.CollectionConverters.asJava(clusterBy.columnNames())) {
      columns.add(new Column(ref.fieldNames()));
    }
    return DataLayoutSpec.clustered(columns);
  }

  private static DataLayoutSpec processPartitioning(Transform[] partitions) {
    List<Column> columns = new ArrayList<>(partitions.length);
    for (Transform t : partitions) {
      if (!(t instanceof IdentityTransform)) {
        throw new UnsupportedOperationException(
            "Unsupported transform (expected identity partition or cluster-by): " + t);
      }
      NamedReference ref = ((IdentityTransform) t).reference();
      if (ref.fieldNames().length > 1) {
        throw new UnsupportedOperationException(
            "Nested partition columns are not supported: " + String.join(".", ref.fieldNames()));
      }
      columns.add(new Column(ref.fieldNames()[0]));
    }
    return DataLayoutSpec.partitioned(columns);
  }

  /**
   * Resolve table path from DSv2 properties: {@code location} → {@code path} → {@code option.path}.
   *
   * @throws IllegalArgumentException if no valid path can be resolved
   */
  public static String resolveTablePath(Map<String, String> properties, Identifier ident) {
    for (String key : new String[] {TableCatalog.PROP_LOCATION, "path", "option.path"}) {
      String value = properties.get(key);
      if (value != null && !value.trim().isEmpty()) {
        return value;
      }
    }
    throw new IllegalArgumentException(
        "No table path resolved for "
            + ident
            + ". Specify a LOCATION or path property for non-UC tables.");
  }

  // ---------------------------------------------------------------------------
  // Validation
  // ---------------------------------------------------------------------------

  /**
   * Validates that every column in a CLUSTER BY spec exists in the schema and that there are no
   * duplicates (case-insensitive). Delegates to {@link #resolveNestedColumn} for each column path.
   */
  public static void validateClusteringColumns(DataLayoutSpec spec, StructType kernelSchema) {
    if (!spec.hasClustering()) {
      return;
    }
    Set<String> seen = new HashSet<>();
    for (Column col : spec.getClusteringColumns()) {
      String[] names = col.getNames();
      String fullPath = String.join(".", names);
      resolveNestedColumn(names, kernelSchema, fullPath);
      if (!seen.add(fullPath.toLowerCase(Locale.ROOT))) {
        throw new IllegalArgumentException("Duplicate column in CLUSTER BY: " + fullPath);
      }
    }
  }

  /**
   * Resolves a CLUSTER BY column path through the Kernel schema by walking each segment, verifying
   * it exists and that intermediate segments are struct types. This catches invalid references at
   * DDL time rather than letting them slip through to Kernel's write path.
   *
   * <p>For example, given:
   *
   * <pre>{@code
   * CREATE TABLE events (
   *   id INT,
   *   address STRUCT<city: STRING, zip: INT>
   * ) CLUSTER BY (address.city)
   * }</pre>
   *
   * Spark passes {@code names = ["address", "city"]}. Resolution walks the schema:
   *
   * <ol>
   *   <li>"address" — found in top-level schema, is a StructType → descend
   *   <li>"city" — found in address struct, last segment → valid
   * </ol>
   *
   * <p>Failure cases:
   *
   * <ul>
   *   <li>{@code CLUSTER BY (address.state)} — throws ("not found in table schema")
   *   <li>{@code CLUSTER BY (id.nested)} — throws ("path segment is not a struct")
   * </ul>
   */
  static void resolveNestedColumn(String[] names, StructType schema, String fullPath) {
    StructType current = schema;
    for (int i = 0; i < names.length; i++) {
      if (current.indexOf(names[i]) < 0) {
        throw new IllegalArgumentException(
            "CLUSTER BY column not found in table schema: " + fullPath);
      }
      if (i < names.length - 1) {
        DataType fieldType = current.get(names[i]).getDataType();
        if (!(fieldType instanceof StructType)) {
          throw new IllegalArgumentException(
              "CLUSTER BY path segment is not a struct in table schema: " + fullPath);
        }
        current = (StructType) fieldType;
      }
    }
  }
}
