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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link AbstractMetadata} implementation that carries a Delta REST Catalog table's schema as
 * the raw UC SDK POJO column list, without the JSON round-trip that the legacy read path
 * imposes.
 *
 * <p>The DRC read path never serializes schema to JSON on the way from UC to Spark. Instead:
 * <ol>
 *   <li>{@code UCDeltaClientImpl.loadTable} constructs a {@code DRCMetadataAdapter} from the UC
 *       SDK {@code LoadTableResponse}.</li>
 *   <li>Delta-Spark pattern-matches on the concrete type, calls {@link #getDRCColumns()} to
 *       retrieve the POJO list, and feeds it to {@code DeltaRestSchemaConverter} for direct
 *       conversion to {@code StructType}.</li>
 * </ol>
 *
 * <p><b>{@link #getSchemaString()} returns a deterministic empty-struct stub in v1</b> rather
 * than throwing. Delta-Spark consumers on the DRC read path MUST use {@link #getDRCColumns()}
 * via a type cast -- the Javadoc on that method is the binding contract. The stub exists only
 * so that framework defensive calls (logging, {@code equals}/{@code toString} in IDEs, metric
 * tags, error-path formatters) do not bring down a read. A one-time WARN is emitted the first
 * time the stub is returned so that a bug report from Kernel/Flink after the clarifications §6
 * audit lands has a signal to correlate against; the proper JSON serializer is tracked there.
 *
 * <p>Immutable; the columns, partition list, and configuration map are all defensively copied
 * and returned unmodifiable so that mutations from the DRC SDK layer cannot leak into consumers.
 */
public final class DRCMetadataAdapter implements AbstractMetadata {

  private static final Logger LOG = LoggerFactory.getLogger(DRCMetadataAdapter.class);
  private static final String DELTA_PROVIDER = "delta";
  private static final String EMPTY_STRUCT_JSON = "{\"type\":\"struct\",\"fields\":[]}";

  /**
   * Per-instance gate for the one-time WARN. Static across the JVM would be too noisy for unit
   * tests; per-instance gives one warning per table-load which is plenty to surface a misuse.
   */
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
        Objects.requireNonNull(drcColumns, "drcColumns"));
    this.partitionColumns = Collections.unmodifiableList(
        Objects.requireNonNull(partitionColumns, "partitionColumns"));
    this.configuration = Collections.unmodifiableMap(
        new HashMap<>(Objects.requireNonNull(configuration, "configuration")));
    this.createdTime = createdTime;
  }

  /**
   * Returns the DRC-native structured column list straight from the UC SDK, bypassing any JSON
   * serialization. Callers on the Delta-Spark read path MUST use this method (after a type cast
   * from {@link AbstractMetadata}) rather than {@link #getSchemaString()}.
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
   * Returns a deterministic empty-struct JSON stub. This method is NOT the DRC read-path's
   * schema accessor -- see the class-level Javadoc and {@link #getDRCColumns()}. A one-time
   * WARN is emitted on first invocation per instance so that an unintended caller (typically
   * a framework logging or formatting path) surfaces during testing without crashing
   * production. A real JSON fallback is tracked under design-doc clarifications §6.
   */
  @Override
  public String getSchemaString() {
    if (loggedSchemaStringStub.compareAndSet(false, true)) {
      LOG.warn("DRCMetadataAdapter.getSchemaString() invoked on table id={}. Returning an "
          + "empty-struct stub; Delta-Spark read-path consumers must use getDRCColumns() "
          + "instead. If this WARN originates from non-Spark code (Kernel, Flink, logging "
          + "framework), file an issue under clarifications §6 (owner TBD).", id);
    }
    return EMPTY_STRUCT_JSON;
  }
}
