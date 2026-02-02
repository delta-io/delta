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

import static java.util.Objects.requireNonNull;

import io.delta.kernel.TableManager;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.transaction.CreateTableTransactionBuilder;
import io.delta.kernel.transaction.DataLayoutSpec;
import io.delta.kernel.utils.CloseableIterable;
import io.delta.spark.internal.v2.utils.SchemaUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.expressions.IdentityTransform;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;

/**
 * Kernel-backed CREATE TABLE helper for the Spark V2 connector.
 *
 * <p>Phase 1 supports path-based tables and partitioning by identity columns only.
 */
public final class KernelTableCreator {

  private static final String ENGINE_INFO = "delta-spark-v2-connector";

  private KernelTableCreator() {}

  public static SparkTable createTable(
      Identifier ident, StructType schema, Transform[] partitions, Map<String, String> properties) {
    requireNonNull(ident, "ident is null");
    requireNonNull(schema, "schema is null");

    if (!isPathIdentifier(ident)) {
      throw new UnsupportedOperationException(
          "V2 CREATE TABLE supports only path-based tables in phase 1");
    }

    String tablePath = ident.name();
    Engine engine = createEngine();

    io.delta.kernel.types.StructType kernelSchema =
        SchemaUtils.convertSparkSchemaToKernelSchema(schema);

    CreateTableTransactionBuilder builder =
        TableManager.buildCreateTableTransaction(tablePath, kernelSchema, ENGINE_INFO);

    if (properties != null && !properties.isEmpty()) {
      builder = builder.withTableProperties(properties);
    }

    Optional<DataLayoutSpec> dataLayoutSpec = buildDataLayoutSpec(partitions);
    if (dataLayoutSpec.isPresent()) {
      builder = builder.withDataLayoutSpec(dataLayoutSpec.get());
    }

    builder.build(engine).commit(engine, CloseableIterable.emptyIterable());
    return new SparkTable(ident, tablePath);
  }

  private static Engine createEngine() {
    SparkSession spark = SparkSession.active();
    Configuration hadoopConf = spark.sessionState().newHadoopConf();
    return DefaultEngine.create(hadoopConf);
  }

  private static Optional<DataLayoutSpec> buildDataLayoutSpec(Transform[] partitions) {
    if (partitions == null || partitions.length == 0) {
      return Optional.empty();
    }

    List<Column> partitionColumns = new ArrayList<>();
    for (Transform transform : partitions) {
      if (!(transform instanceof IdentityTransform)) {
        throw new UnsupportedOperationException(
            "V2 CREATE TABLE supports only identity partition columns in phase 1");
      }

      NamedReference ref = transform.references()[0];
      String[] fieldNames = ref.fieldNames();
      if (fieldNames.length != 1) {
        throw new IllegalArgumentException(
            "Partition columns must be top-level columns in phase 1");
      }
      partitionColumns.add(new Column(fieldNames[0]));
    }

    return Optional.of(DataLayoutSpec.partitioned(partitionColumns));
  }

  private static boolean isPathIdentifier(Identifier ident) {
    return ident.namespace().length == 1 && "delta".equalsIgnoreCase(ident.namespace()[0]);
  }
}
