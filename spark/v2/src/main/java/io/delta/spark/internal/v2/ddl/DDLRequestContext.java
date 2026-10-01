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

import static java.util.Objects.requireNonNull;

import io.delta.kernel.engine.Engine;
import io.delta.kernel.transaction.CreateTableTransactionBuilder;
import io.delta.kernel.transaction.DataLayoutSpec;
import io.delta.kernel.types.StructType;
import io.delta.spark.internal.v2.snapshot.unitycatalog.UCTableInfo;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.spark.sql.connector.catalog.Identifier;

/**
 * Immutable POJO capturing all inputs for a DDL operation (CREATE TABLE, CTAS, RTAS).
 *
 * <p>Spark-level types (schema, partitions) are pre-converted to Kernel types by {@link
 * CreateTableBuilder#prepare} so downstream code deals only with Kernel abstractions. The Spark
 * {@link Identifier} is retained for logging and catalog-level identification only — it is never
 * passed to Kernel APIs.
 *
 * <p>All collection fields are defensively copied; accessors return unmodifiable views.
 */
public final class DDLRequestContext {

  private final Identifier ident;
  private final String tablePath;
  private final StructType kernelSchema;
  private final Map<String, String> properties;
  private final Optional<String> comment;
  private final DataLayoutSpec dataLayoutSpec;
  private final Engine engine;
  private final Optional<UCTableInfo> ucTableInfo;
  private final CreateTableTransactionBuilder transactionBuilder;

  /**
   * @param ident Spark catalog identifier (namespace + name); used for logging, not Kernel APIs
   * @param tablePath resolved filesystem path for the Delta table
   * @param kernelSchema Kernel-typed schema converted from Spark StructType
   * @param properties user and Delta table properties with DSv2-internal keys already stripped
   * @param comment table description from {@code CREATE TABLE ... COMMENT 'x'}, if provided; not
   *     yet written to the Delta log (see {@link #comment()})
   * @param dataLayoutSpec partitioning / clustering specification
   * @param engine Kernel engine instance (typically {@code DefaultEngine})
   * @param ucTableInfo Unity Catalog metadata when the table is UC-managed, empty otherwise
   * @param transactionBuilder pre-resolved Kernel transaction builder (UC or path-based)
   */
  DDLRequestContext(
      Identifier ident,
      String tablePath,
      StructType kernelSchema,
      Map<String, String> properties,
      Optional<String> comment,
      DataLayoutSpec dataLayoutSpec,
      Engine engine,
      Optional<UCTableInfo> ucTableInfo,
      CreateTableTransactionBuilder transactionBuilder) {
    this.ident = requireNonNull(ident);
    this.tablePath = requireNonNull(tablePath);
    this.kernelSchema = requireNonNull(kernelSchema);
    this.properties = Collections.unmodifiableMap(new HashMap<>(requireNonNull(properties)));
    this.comment = requireNonNull(comment);
    this.dataLayoutSpec = requireNonNull(dataLayoutSpec);
    this.engine = requireNonNull(engine);
    this.ucTableInfo = requireNonNull(ucTableInfo);
    this.transactionBuilder = requireNonNull(transactionBuilder);
  }

  public Identifier ident() {
    return ident;
  }

  public String tablePath() {
    return tablePath;
  }

  public StructType kernelSchema() {
    return kernelSchema;
  }

  /** Returns an unmodifiable view of the table properties. */
  public Map<String, String> properties() {
    return properties;
  }

  /**
   * Table description from {@code COMMENT 'x'}, if provided. Not yet written to the Delta log
   * because Kernel's {@code CreateTableTransactionBuilder} does not yet expose a {@code
   * withDescription()} method. Preserved here for when that API becomes available. TODO(#6473):
   * Write comment to Delta log once Kernel exposes withDescription().
   */
  public Optional<String> comment() {
    return comment;
  }

  public DataLayoutSpec dataLayoutSpec() {
    return dataLayoutSpec;
  }

  /**
   * Kernel engine instance created via {@code DefaultEngine.create(hadoopConf)}. A new engine is
   * created per DDL operation. {@code DefaultEngine} does not hold closeable resources so explicit
   * shutdown is not required. See <a href="https://github.com/delta-io/delta/issues/5675">#5675</a>
   * for centralizing engine creation.
   */
  public Engine engine() {
    return engine;
  }

  public Optional<UCTableInfo> ucTableInfo() {
    return ucTableInfo;
  }

  public CreateTableTransactionBuilder transactionBuilder() {
    return transactionBuilder;
  }
}
