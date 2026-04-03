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

import io.delta.kernel.engine.Engine;
import io.delta.kernel.transaction.DataLayoutSpec;
import io.delta.spark.internal.v2.snapshot.DeltaSnapshotManager;
import java.util.Map;
import java.util.Optional;
import org.apache.spark.sql.types.StructType;

/** Immutable planned state for a CREATE TABLE operation before version 0 is committed. */
public final class PreparedCreateTableOperation {
  private final String location;
  private final Map<String, String> tableProperties;
  private final StructType normalizedSchema;
  private final io.delta.kernel.types.StructType kernelSchema;
  private final Optional<DataLayoutSpec> dataLayoutSpec;
  private final Engine engine;
  private final DeltaSnapshotManager snapshotManager;

  public PreparedCreateTableOperation(
      String location,
      Map<String, String> tableProperties,
      StructType normalizedSchema,
      io.delta.kernel.types.StructType kernelSchema,
      Optional<DataLayoutSpec> dataLayoutSpec,
      Engine engine,
      DeltaSnapshotManager snapshotManager) {
    this.location = location;
    this.tableProperties = tableProperties;
    this.normalizedSchema = normalizedSchema;
    this.kernelSchema = kernelSchema;
    this.dataLayoutSpec = dataLayoutSpec;
    this.engine = engine;
    this.snapshotManager = snapshotManager;
  }

  public String getLocation() {
    return location;
  }

  public Map<String, String> getTableProperties() {
    return tableProperties;
  }

  public StructType getNormalizedSchema() {
    return normalizedSchema;
  }

  public io.delta.kernel.types.StructType getKernelSchema() {
    return kernelSchema;
  }

  public Optional<DataLayoutSpec> getDataLayoutSpec() {
    return dataLayoutSpec;
  }

  public Engine getEngine() {
    return engine;
  }

  public DeltaSnapshotManager getSnapshotManager() {
    return snapshotManager;
  }
}
