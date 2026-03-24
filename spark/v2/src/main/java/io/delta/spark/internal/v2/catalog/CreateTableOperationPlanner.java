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
package io.delta.spark.internal.v2.catalog;

import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.UnsupportedTableFeatureException;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.transaction.DataLayoutSpec;
import io.delta.spark.internal.v2.snapshot.DeltaSnapshotManager;
import io.delta.spark.internal.v2.snapshot.SnapshotManagerFactory;
import io.delta.spark.internal.v2.snapshot.unitycatalog.UCUtils;
import io.delta.spark.internal.v2.utils.ScalaUtils;
import io.delta.spark.internal.v2.utils.SchemaUtils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.util.CharVarcharUtils;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;

/** Plans the immutable Delta/Kernel state needed for CREATE TABLE before commit. */
public final class CreateTableOperationPlanner {

  public PreparedCreateTableOperation planCreateTable(
      Identifier ident,
      StructType schema,
      Transform[] partitions,
      Map<String, String> properties,
      SparkSession spark,
      String catalogName,
      boolean isPathIdentifier) {
    String location = resolveLocation(ident, properties, isPathIdentifier);
    Map<String, String> tableProperties =
        CreateTablePropertySupport.filterCreateTableProperties(properties);
    CreateTablePropertySupport.addCatalogManagedQoLDefaults(spark, tableProperties);
    StructType normalizedSchema = CharVarcharUtils.replaceCharVarcharWithStringInSchema(schema);
    io.delta.kernel.types.StructType kernelSchema =
        SchemaUtils.convertSparkSchemaToKernelSchema(normalizedSchema);
    Engine engine = DefaultEngine.create(createHadoopConf(spark, properties));
    DeltaSnapshotManager snapshotManager =
        SnapshotManagerFactory.forCreateTable(
            location,
            engine,
            UCUtils.extractTableInfoForCreate(location, tableProperties, catalogName, spark));
    return new PreparedCreateTableOperation(
        location,
        tableProperties,
        normalizedSchema,
        kernelSchema,
        toDataLayoutSpec(partitions),
        engine,
        snapshotManager);
  }

  public static boolean canRepresentWithKernel(Transform[] partitions) {
    for (Transform partition : partitions) {
      if (!"identity".equals(partition.name())) {
        return false;
      }
    }
    return true;
  }

  public static boolean shouldFallbackToV1Create(Throwable error) {
    for (Throwable current = error; current != null; current = current.getCause()) {
      if (current instanceof UnsupportedTableFeatureException) {
        return true;
      }
    }
    return false;
  }

  private static String resolveLocation(
      Identifier ident, Map<String, String> properties, boolean isPathIdentifier) {
    String location = properties.get(TableCatalog.PROP_LOCATION);
    if (location == null) {
      location = properties.get("location");
    }
    if (location == null && isPathIdentifier) {
      location = ident.name();
    }
    if (location == null) {
      throw new IllegalArgumentException("Unable to resolve location for CREATE TABLE " + ident);
    }
    return location;
  }

  private static Configuration createHadoopConf(
      SparkSession spark, Map<String, String> properties) {
    Map<String, String> fsOptions = new HashMap<>();
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      String key = entry.getKey();
      if (CreateTablePropertySupport.isHadoopOption(key)) {
        String normalizedKey = key.startsWith("option.") ? key.substring("option.".length()) : key;
        fsOptions.put(normalizedKey, entry.getValue());
      }
    }
    return spark.sessionState().newHadoopConfWithOptions(ScalaUtils.toScalaMap(fsOptions));
  }

  private static Optional<DataLayoutSpec> toDataLayoutSpec(Transform[] partitions) {
    List<Column> partitionColumns = new ArrayList<>();
    for (Transform partition : partitions) {
      if (partition.references().length > 0) {
        partitionColumns.add(new Column(partition.references()[0].describe()));
      }
    }
    if (partitionColumns.isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(DataLayoutSpec.partitioned(partitionColumns));
  }
}
