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
import io.delta.kernel.transaction.DataLayoutSpec;
import io.delta.kernel.types.StructType;
import io.delta.spark.internal.v2.snapshot.unitycatalog.UCTableInfo;
import java.util.Map;
import java.util.Optional;
import org.apache.spark.sql.connector.catalog.Identifier;

/**
 * Kernel-ready POJO for DDL operations (CREATE TABLE, CTAS, RTAS).
 *
 * <p>All Spark types are pre-converted to Kernel types by {@link CreateTableBuilder#prepare} so
 * downstream code deals only with Kernel abstractions.
 */
public final class DDLRequest {

  private final Identifier ident;
  private final String tablePath;
  private final StructType kernelSchema;
  private final Map<String, String> properties;
  private final DataLayoutSpec dataLayoutSpec;
  private final Engine engine;
  private final Optional<UCTableInfo> ucTableInfo;
  private final String engineInfo;

  DDLRequest(
      Identifier ident,
      String tablePath,
      StructType kernelSchema,
      Map<String, String> properties,
      DataLayoutSpec dataLayoutSpec,
      Engine engine,
      Optional<UCTableInfo> ucTableInfo,
      String engineInfo) {
    this.ident = requireNonNull(ident);
    this.tablePath = requireNonNull(tablePath);
    this.kernelSchema = requireNonNull(kernelSchema);
    this.properties = requireNonNull(properties);
    this.dataLayoutSpec = requireNonNull(dataLayoutSpec);
    this.engine = requireNonNull(engine);
    this.ucTableInfo = requireNonNull(ucTableInfo);
    this.engineInfo = requireNonNull(engineInfo);
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

  public Map<String, String> properties() {
    return properties;
  }

  public DataLayoutSpec dataLayoutSpec() {
    return dataLayoutSpec;
  }

  public Engine engine() {
    return engine;
  }

  public Optional<UCTableInfo> ucTableInfo() {
    return ucTableInfo;
  }

  public String engineInfo() {
    return engineInfo;
  }
}
