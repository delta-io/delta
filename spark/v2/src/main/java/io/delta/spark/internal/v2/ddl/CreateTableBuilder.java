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
import io.delta.kernel.engine.Engine;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.transaction.CreateTableTransactionBuilder;
import io.delta.kernel.transaction.DataLayoutSpec;
import io.delta.kernel.unitycatalog.UCCatalogManagedClient;
import io.delta.spark.internal.v2.snapshot.unitycatalog.UCTableInfo;
import io.delta.spark.internal.v2.utils.SchemaUtils;
import io.delta.storage.commit.uccommitcoordinator.UCClient;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.expressions.IdentityTransform;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.delta.coordinatedcommits.UCTokenBasedRestClientFactory$;
import org.apache.spark.sql.types.StructType;

/**
 * Prepares and builds Kernel transactions for CREATE TABLE.
 *
 * <p>Two-step usage: {@link #prepare} converts Spark inputs into a {@link DDLRequest}, then {@link
 * #buildTransaction} creates the Kernel {@link Transaction}.
 */
public final class CreateTableBuilder {

  static final String ENGINE_INFO = "Delta-Spark-DSv2/" + Meta.KERNEL_VERSION;

  /** Keys injected by Spark's DSv2 layer that Kernel should not see. */
  static final Set<String> DSV2_INTERNAL_KEYS =
      Set.of(
          TableCatalog.PROP_LOCATION,
          TableCatalog.PROP_PROVIDER,
          TableCatalog.PROP_COMMENT,
          TableCatalog.PROP_OWNER,
          TableCatalog.PROP_EXTERNAL,
          TableCatalog.PROP_IS_MANAGED_LOCATION,
          "path",
          "option.path");

  private CreateTableBuilder() {}

  // ── prepare ────────────────────────────────────────────────────────

  /** Convert Spark-level inputs into a Kernel-ready {@link DDLRequest}. */
  public static DDLRequest prepare(
      Identifier ident,
      StructType sparkSchema,
      Transform[] partitions,
      Map<String, String> properties,
      Configuration hadoopConf,
      Optional<UCTableInfo> ucTableInfo) {

    Engine engine = DefaultEngine.create(hadoopConf);
    io.delta.kernel.types.StructType kernelSchema =
        SchemaUtils.convertSparkSchemaToKernelSchema(sparkSchema);
    Map<String, String> filteredProps = filterProperties(properties);
    DataLayoutSpec layout = buildDataLayoutSpec(partitions);

    String tablePath =
        ucTableInfo
            .map(UCTableInfo::getTablePath)
            .orElseGet(() -> resolveTablePath(properties, ident));

    return new DDLRequest(
        ident, tablePath, kernelSchema, filteredProps, layout, engine, ucTableInfo, ENGINE_INFO);
  }

  // ── build transaction ──────────────────────────────────────────────

  /** Build a Kernel {@link Transaction} from the prepared request. */
  public static Transaction buildTransaction(DDLRequest request) {
    CreateTableTransactionBuilder builder;

    if (request.ucTableInfo().isPresent()) {
      UCTableInfo info = request.ucTableInfo().get();
      UCCatalogManagedClient ucClient = createUCClient(info);
      builder =
          ucClient.buildCreateTableTransaction(
              info.getTableId(), info.getTablePath(), request.kernelSchema(), request.engineInfo());
    } else {
      builder =
          TableManager.buildCreateTableTransaction(
              request.tablePath(), request.kernelSchema(), request.engineInfo());
    }

    if (!request.properties().isEmpty()) {
      builder.withTableProperties(request.properties());
    }
    if (!request.dataLayoutSpec().hasNoDataLayoutSpec()) {
      builder.withDataLayoutSpec(request.dataLayoutSpec());
    }

    return builder.build(request.engine());
  }

  // ── helpers (package-visible for testing) ──────────────────────────

  /** Strip DSv2-internal keys, keeping only user / delta properties for Kernel. */
  static Map<String, String> filterProperties(Map<String, String> properties) {
    Map<String, String> filtered = new LinkedHashMap<>();
    properties.forEach(
        (k, v) -> {
          if (!DSV2_INTERNAL_KEYS.contains(k)) {
            filtered.put(k, v);
          }
        });
    return filtered;
  }

  /** Convert Spark {@link Transform} array to a Kernel {@link DataLayoutSpec}. */
  static DataLayoutSpec buildDataLayoutSpec(Transform[] partitions) {
    if (partitions == null || partitions.length == 0) {
      return DataLayoutSpec.noDataLayout();
    }
    List<Column> columns =
        Stream.of(partitions)
            .map(
                t -> {
                  if (!(t instanceof IdentityTransform)) {
                    throw new UnsupportedOperationException(
                        "Only identity partitioning is supported, got: " + t);
                  }
                  String colName = ((IdentityTransform) t).reference().fieldNames()[0];
                  return new Column(colName);
                })
            .collect(Collectors.toList());
    return DataLayoutSpec.partitioned(columns);
  }

  /** Resolve table path from properties, falling back to ident name. */
  static String resolveTablePath(Map<String, String> properties, Identifier ident) {
    String loc = properties.get(TableCatalog.PROP_LOCATION);
    if (loc != null) return loc;
    loc = properties.get("path");
    if (loc != null) return loc;
    return ident.name();
  }

  private static UCCatalogManagedClient createUCClient(UCTableInfo info) {
    Map<String, String> appVersions =
        UCTokenBasedRestClientFactory$.MODULE$.defaultAppVersionsAsJava();
    appVersions.put("Kernel", Meta.KERNEL_VERSION);
    appVersions.put("Delta V2 connector", "true");
    UCClient ucClient =
        UCTokenBasedRestClientFactory$.MODULE$.createUCClientWithVersions(
            info.getUcUri(), info.getAuthConfig(), appVersions);
    return new UCCatalogManagedClient(ucClient);
  }
}
