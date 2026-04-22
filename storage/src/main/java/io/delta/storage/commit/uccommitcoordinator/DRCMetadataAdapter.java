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

package io.delta.storage.commit.uccommitcoordinator;

import io.delta.storage.commit.actions.AbstractMetadata;
import io.unitycatalog.client.delta.model.StructField;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link AbstractMetadata} implementation that carries a Delta REST Catalog (DRC) table's
 * schema as the raw UC SDK POJO column list, avoiding the JSON round-trip imposed by the
 * legacy read path.
 *
 * <p><b>Fast path (DRC read).</b> {@code UCDeltaClientImpl.loadTable} constructs this
 * adapter from the UC SDK {@code TableMetadata}. Delta-Spark pattern-matches on the
 * concrete type, calls {@link #getDRCColumns()} to retrieve the POJO list, and feeds it
 * to {@code DeltaRestSchemaConverter} for direct conversion to {@code StructType}. No
 * JSON in the middle.
 *
 * <p><b>{@link #getSchemaString()} is a deterministic empty-struct stub in v1.</b> Spark
 * read-path consumers MUST use {@link #getDRCColumns()} via a type cast from
 * {@link AbstractMetadata}. The stub exists so that framework defensive callers (logging,
 * {@code toString}, error formatters, metric tags) do not crash a read when they reach
 * past the adapter's preferred interface. A once-per-instance WARN surfaces unintended
 * use without stopping the session. A real JSON fallback is tracked in design-doc
 * clarifications §6 pending the Kernel/Flink consumer audit.
 *
 * <p><b>Internal.</b> Consumers outside the DRC read path should not depend on this
 * class directly; cast through {@link AbstractMetadata} and {@code getDRCColumns()}.
 *
 * <p>Immutable -- columns, partition list, and configuration are defensively copied at
 * construction and returned via unmodifiable views so that mutations on the UC SDK side
 * cannot leak into downstream consumers.
 */
public final class DRCMetadataAdapter implements AbstractMetadata {

  private static final Logger LOG = LoggerFactory.getLogger(DRCMetadataAdapter.class);
  private static final String DELTA_PROVIDER = "delta";
  private static final String EMPTY_STRUCT_JSON = "{\"type\":\"struct\",\"fields\":[]}";

  // Per-instance so that one stray caller per table surfaces one WARN -- correlatable
  // in bug reports. JVM-wide would be too quiet with multiple loaded tables.
  private final AtomicBoolean loggedSchemaStringStub = new AtomicBoolean(false);

  private final String id;
  private final String name;
  private final String description;
  private final List<StructField> drcColumns;
  private final List<String> partitionColumns;
  private final Map<String, String> configuration;
  private final Long createdTime;

  public DRCMetadataAdapter(
      String id,
      String name,
      String description,
      List<StructField> drcColumns,
      List<String> partitionColumns,
      Map<String, String> configuration,
      Long createdTime) {
    this.id = Objects.requireNonNull(id, "id");
    this.name = name;
    this.description = description;
    this.drcColumns = Collections.unmodifiableList(
        new ArrayList<>(Objects.requireNonNull(drcColumns, "drcColumns")));
    this.partitionColumns = Collections.unmodifiableList(
        new ArrayList<>(Objects.requireNonNull(partitionColumns, "partitionColumns")));
    this.configuration = Collections.unmodifiableMap(
        new HashMap<>(Objects.requireNonNull(configuration, "configuration")));
    this.createdTime = createdTime;
  }

  /**
   * Returns the DRC-native structured column list straight from the UC SDK. Consumers on
   * the Delta-Spark read path MUST use this method (after a type cast from
   * {@link AbstractMetadata}) rather than {@link #getSchemaString()}.
   */
  public List<StructField> getDRCColumns() {
    return drcColumns;
  }

  @Override public String getId() { return id; }
  @Override public String getName() { return name; }
  @Override public String getDescription() { return description; }
  @Override public String getProvider() { return DELTA_PROVIDER; }
  @Override public Map<String, String> getFormatOptions() { return Collections.emptyMap(); }
  @Override public List<String> getPartitionColumns() { return partitionColumns; }
  @Override public Map<String, String> getConfiguration() { return configuration; }
  @Override public Long getCreatedTime() { return createdTime; }

  /**
   * Returns an empty-struct JSON stub. This is NOT the DRC read-path schema accessor --
   * see {@link #getDRCColumns()} and the class-level Javadoc. A one-time WARN is emitted
   * on first invocation per instance so that unintended callers surface during testing;
   * a proper JSON fallback is tracked in design-doc clarifications §6.
   */
  @Override
  public String getSchemaString() {
    if (loggedSchemaStringStub.compareAndSet(false, true)) {
      LOG.warn("DRCMetadataAdapter.getSchemaString() invoked on table id={}. Returning an "
          + "empty-struct stub; Delta-Spark read-path consumers must use getDRCColumns() "
          + "instead. If this WARN originates from non-Spark code (Kernel, Flink, logging "
          + "framework), file an issue under clarifications §6.", id);
    }
    return EMPTY_STRUCT_JSON;
  }
}
