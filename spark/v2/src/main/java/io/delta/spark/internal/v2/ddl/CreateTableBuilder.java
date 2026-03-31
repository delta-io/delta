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
import io.delta.kernel.TableManager;
import io.delta.kernel.Transaction;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.transaction.CreateTableTransactionBuilder;
import io.delta.kernel.transaction.DataLayoutSpec;
import io.delta.kernel.unitycatalog.UCCatalogManagedClient;
import io.delta.spark.internal.v2.snapshot.unitycatalog.UCTableInfo;
import io.delta.spark.internal.v2.utils.SchemaUtils;
import io.delta.storage.commit.uccommitcoordinator.UCClient;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.expressions.IdentityTransform;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.delta.coordinatedcommits.UCTokenBasedRestClientFactory$;
import org.apache.spark.sql.delta.skipping.clustering.temp.ClusterByTransform;
import org.apache.spark.sql.types.StructType;

/** Prepares and builds Kernel transactions for CREATE TABLE. */
public final class CreateTableBuilder {

  private static final String ENGINE_INFO = "Delta-Spark-DSv2/" + Meta.KERNEL_VERSION;

  private static final Set<String> DSV2_INTERNAL_KEYS =
      Set.of(
          TableCatalog.PROP_LOCATION,
          TableCatalog.PROP_PROVIDER,
          TableCatalog.PROP_OWNER,
          TableCatalog.PROP_EXTERNAL,
          TableCatalog.PROP_IS_MANAGED_LOCATION,
          "path",
          "option.path");

  /**
   * Protocol properties injected by Spark based on table features / TBLPROPERTIES syntax. Kernel
   * determines the reader/writer protocol versions internally from the requested table features, so
   * these must be stripped before forwarding properties to {@link CreateTableTransactionBuilder}.
   * Passing them through causes Kernel to reject them as unknown configuration keys.
   */
  private static final Set<String> PROTOCOL_KEYS =
      Set.of("delta.minReaderVersion", "delta.minWriterVersion", "delta.ignoreProtocolDefaults");

  /**
   * Properties managed by the UC commit coordinator. For UC managed tables, {@code UCSingleCatalog}
   * injects {@code io.unitycatalog.tableId} into properties during staging. Kernel independently
   * sets this via {@code UCCatalogManagedClient.getRequiredTablePropertiesForCreate}, so it must be
   * stripped from user properties to avoid duplicate-key conflicts.
   */
  private static final String UC_TABLE_ID_KEY = "io.unitycatalog.tableId";

  private CreateTableBuilder() {}

  /** Convert Spark-level inputs into a Kernel-ready {@link DDLRequestContext}. */
  public static DDLRequestContext prepare(
      Identifier ident,
      StructType sparkSchema,
      Transform[] partitions,
      Map<String, String> properties,
      Configuration hadoopConf,
      Optional<UCTableInfo> ucTableInfo) {

    String tablePath =
        ucTableInfo
            .map(UCTableInfo::getTablePath)
            .orElseGet(() -> resolveTablePath(properties, ident));

    Optional<String> comment = extractComment(properties);
    io.delta.kernel.types.StructType kernelSchema =
        SchemaUtils.convertSparkSchemaToKernelSchema(sparkSchema);
    DataLayoutSpec dataLayoutSpec = toDataLayoutSpec(partitions);
    validateClusteringColumns(dataLayoutSpec, kernelSchema);

    return new DDLRequestContext(
        ident,
        tablePath,
        kernelSchema,
        filterProperties(properties),
        comment,
        dataLayoutSpec,
        DefaultEngine.create(hadoopConf),
        ucTableInfo,
        ENGINE_INFO);
  }

  /** Build a Kernel {@link Transaction} from the prepared request. */
  public static Transaction buildTransaction(DDLRequestContext request) {
    CreateTableTransactionBuilder builder =
        request.ucTableInfo().isPresent()
            ? buildUCTransaction(request)
            : TableManager.buildCreateTableTransaction(
                request.tablePath(), request.kernelSchema(), request.engineInfo());

    if (!request.properties().isEmpty()) {
      builder.withTableProperties(request.properties());
    }
    if (!request.dataLayoutSpec().hasNoDataLayoutSpec()) {
      builder.withDataLayoutSpec(request.dataLayoutSpec());
    }
    return builder.build(request.engine());
  }

  /**
   * Extract the table comment (from {@code CREATE TABLE ... COMMENT 'x'}) before it is filtered out
   * of the properties map. The Kernel {@link CreateTableTransactionBuilder} does not yet support a
   * {@code withDescription()} method, so the comment is preserved in {@link
   * DDLRequestContext#comment()} but not currently written to the Delta log.
   */
  static Optional<String> extractComment(Map<String, String> properties) {
    String comment = properties.get(TableCatalog.PROP_COMMENT);
    return Optional.ofNullable(comment).filter(c -> !c.isEmpty());
  }

  /**
   * Strip DSv2-internal keys, comment, and protocol keys — keeping only user/delta properties for
   * Kernel. Protocol versions ({@code delta.minReaderVersion}, {@code delta.minWriterVersion}) are
   * managed internally by Kernel and must not be forwarded as table properties.
   */
  static Map<String, String> filterProperties(Map<String, String> properties) {
    Map<String, String> result = new HashMap<>(properties);
    DSV2_INTERNAL_KEYS.forEach(result::remove);
    result.remove(TableCatalog.PROP_COMMENT);
    result.remove(UC_TABLE_ID_KEY);
    PROTOCOL_KEYS.forEach(result::remove);
    return result;
  }

  /**
   * Convert Spark partition/clustering transforms to a Kernel {@link DataLayoutSpec}.
   *
   * <p>The transforms array contains either identity partition transforms ({@code PARTITIONED BY})
   * or a single {@link ClusterByTransform} ({@code CLUSTER BY}) — never both. The Spark parser
   * enforces this mutual exclusivity.
   */
  static DataLayoutSpec toDataLayoutSpec(Transform[] partitions) {
    if (partitions == null || partitions.length == 0) {
      return DataLayoutSpec.noDataLayout();
    }

    if (partitions[0] instanceof ClusterByTransform) {
      ClusterByTransform clusterBy = (ClusterByTransform) partitions[0];
      List<Column> columns = new ArrayList<>(clusterBy.columnNames().size());
      for (NamedReference ref :
          scala.jdk.javaapi.CollectionConverters.asJava(clusterBy.columnNames())) {
        columns.add(new Column(ref.fieldNames()));
      }
      return DataLayoutSpec.clustered(columns);
    }

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
   * Validates that clustering columns exist in the schema and are not duplicated.
   *
   * <p>Supports nested columns (e.g. {@code CLUSTER BY (a.b, a.c)} where {@code a} is a struct).
   * Each column's full dotted path is resolved against the schema by walking nested struct types.
   *
   * <p>This mirrors the V1 validation in {@code AbstractDeltaCatalog.validateClusterBySpec} which
   * the Kernel path bypasses. Partition column validation is handled by Kernel itself during {@code
   * CreateTableTransactionBuilder.build()}.
   */
  static void validateClusteringColumns(
      DataLayoutSpec spec, io.delta.kernel.types.StructType kernelSchema) {
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
   * Walk the schema to verify that a possibly-nested column path exists. Throws if any segment is
   * missing or a non-terminal segment is not a struct.
   */
  private static void resolveNestedColumn(
      String[] names, io.delta.kernel.types.StructType schema, String fullPath) {
    io.delta.kernel.types.StructType current = schema;
    for (int i = 0; i < names.length; i++) {
      if (current.indexOf(names[i]) < 0) {
        throw new IllegalArgumentException(
            "CLUSTER BY column not found in table schema: " + fullPath);
      }
      if (i < names.length - 1) {
        io.delta.kernel.types.DataType fieldType = current.get(names[i]).getDataType();
        if (!(fieldType instanceof io.delta.kernel.types.StructType)) {
          throw new IllegalArgumentException(
              "CLUSTER BY column not found in table schema: " + fullPath);
        }
        current = (io.delta.kernel.types.StructType) fieldType;
      }
    }
  }

  /**
   * Resolve table path from DSv2 properties: {@code location} → {@code path} → {@code option.path}.
   *
   * <p>V1 {@code AbstractDeltaCatalog.createDeltaTable} uses only {@code location} (plus path
   * identifiers and default managed paths). The {@code path} and {@code option.path} fallbacks are
   * DSv2-specific since Spark may encode DataSource options under these keys. For UC-managed tables
   * the path comes from {@link UCTableInfo#getTablePath()} instead of this method.
   *
   * @throws IllegalArgumentException if no path can be resolved
   */
  static String resolveTablePath(Map<String, String> properties, Identifier ident) {
    for (String key : new String[] {TableCatalog.PROP_LOCATION, "path", "option.path"}) {
      if (properties.containsKey(key)) {
        return properties.get(key);
      }
    }
    throw new IllegalArgumentException(
        "No table path resolved for "
            + ident
            + ". "
            + "Specify a LOCATION or path property for non-UC tables.");
  }

  private static CreateTableTransactionBuilder buildUCTransaction(DDLRequestContext request) {
    UCTableInfo info = request.ucTableInfo().get();
    Map<String, String> appVersions =
        new HashMap<>(UCTokenBasedRestClientFactory$.MODULE$.defaultAppVersionsAsJava());
    appVersions.put("Kernel", Meta.KERNEL_VERSION);
    appVersions.put("Delta V2 connector", "true");
    UCClient ucClient =
        UCTokenBasedRestClientFactory$.MODULE$.createUCClientWithVersions(
            info.getUcUri(), info.getAuthConfig(), appVersions);
    return new UCCatalogManagedClient(ucClient)
        .buildCreateTableTransaction(
            info.getTableId(), info.getTablePath(), request.kernelSchema(), request.engineInfo());
  }
}
