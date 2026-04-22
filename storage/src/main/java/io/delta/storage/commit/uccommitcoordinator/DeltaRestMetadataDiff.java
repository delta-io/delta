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

import io.delta.storage.annotation.Unstable;
import io.unitycatalog.client.delta.model.DeltaProtocol;
import io.unitycatalog.client.delta.model.RemovePropertiesUpdate;
import io.unitycatalog.client.delta.model.SetPartitionColumnsUpdate;
import io.unitycatalog.client.delta.model.SetPropertiesUpdate;
import io.unitycatalog.client.delta.model.SetProtocolUpdate;
import io.unitycatalog.client.delta.model.SetSchemaUpdate;
import io.unitycatalog.client.delta.model.StructType;
import io.unitycatalog.client.delta.model.TableUpdate;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Computes {@link TableUpdate}s from old/new metadata snapshots for the DRC commit path.
 *
 * <p>The DRC wire format distinguishes <b>intent-based</b> updates (properties -- changed keys
 * only) from <b>full-replacement</b> updates (protocol, schema, partition columns). Intent-based
 * means the wire carries only what changed; full-replacement carries the entire new value and
 * relies on {@code AssertEtag} (when attached by the caller) to guard against concurrent writes.
 *
 * <p>Pure function; no I/O. Inputs are UC SDK POJOs -- callers in the spark module use the
 * reverse {@code DeltaRestSchemaConverter} to turn a Spark {@code StructType} into the UC
 * {@code StructType} expected by {@link #columnsUpdate}.
 */
@Unstable
public final class DeltaRestMetadataDiff {

  private DeltaRestMetadataDiff() {}

  /**
   * Intent-based diff of two property maps. Returns 0, 1, or 2 updates:
   * <ul>
   *   <li>A {@link SetPropertiesUpdate} for keys whose value is present in {@code newProps} and
   *       either absent from {@code oldProps} or differs in value;</li>
   *   <li>A {@link RemovePropertiesUpdate} for keys present in {@code oldProps} but missing
   *       from {@code newProps};</li>
   *   <li>Unchanged keys contribute nothing.</li>
   * </ul>
   *
   * <p>Uses {@code containsKey} so "key added with explicit null value" is distinguished from
   * "unchanged key". A null input map is treated as an empty map.
   */
  public static List<TableUpdate> propertyUpdates(
      Map<String, String> oldProps,
      Map<String, String> newProps) {
    Map<String, String> oldSafe = oldProps == null ? Collections.emptyMap() : oldProps;
    Map<String, String> newSafe = newProps == null ? Collections.emptyMap() : newProps;

    Map<String, String> upserts = new HashMap<>();
    for (Map.Entry<String, String> e : newSafe.entrySet()) {
      boolean oldHasKey = oldSafe.containsKey(e.getKey());
      String oldVal = oldSafe.get(e.getKey());
      if (!oldHasKey || !Objects.equals(oldVal, e.getValue())) {
        upserts.put(e.getKey(), e.getValue());
      }
    }
    Set<String> removals = new LinkedHashSet<>();
    for (String k : oldSafe.keySet()) {
      if (!newSafe.containsKey(k)) {
        removals.add(k);
      }
    }

    List<TableUpdate> out = new ArrayList<>();
    if (!upserts.isEmpty()) {
      out.add(new SetPropertiesUpdate().updates(upserts));
    }
    if (!removals.isEmpty()) {
      out.add(new RemovePropertiesUpdate().removals(new ArrayList<>(removals)));
    }
    return out;
  }

  /**
   * Full-replacement diff for protocol. Returns a {@link SetProtocolUpdate} with the new
   * protocol if {@code oldProtocol} is absent or differs from {@code newProtocol}, otherwise
   * {@link Optional#empty()}. Equality uses {@link DeltaProtocol#equals}.
   */
  public static Optional<TableUpdate> protocolUpdate(
      Optional<DeltaProtocol> oldProtocol,
      DeltaProtocol newProtocol) {
    Objects.requireNonNull(newProtocol, "newProtocol must not be null");
    Objects.requireNonNull(oldProtocol, "oldProtocol Optional must not be null");
    if (oldProtocol.isPresent() && oldProtocol.get().equals(newProtocol)) {
      return Optional.empty();
    }
    TableUpdate u = new SetProtocolUpdate().protocol(newProtocol);
    return Optional.of(u);
  }

  /**
   * Full-replacement diff for schema. Returns a {@link SetSchemaUpdate} with the new columns
   * when {@code oldColumns} is absent or differs from {@code newColumns}, otherwise
   * {@link Optional#empty()}. Equality uses {@link StructType#equals} which recurses through
   * nested fields (the UC model's generated equals).
   */
  public static Optional<TableUpdate> columnsUpdate(
      Optional<StructType> oldColumns,
      StructType newColumns) {
    Objects.requireNonNull(newColumns, "newColumns must not be null");
    Objects.requireNonNull(oldColumns, "oldColumns Optional must not be null");
    if (oldColumns.isPresent() && oldColumns.get().equals(newColumns)) {
      return Optional.empty();
    }
    TableUpdate u = new SetSchemaUpdate().columns(newColumns);
    return Optional.of(u);
  }

  /**
   * Full-replacement diff for partition columns. Returns a {@link SetPartitionColumnsUpdate}
   * with the new list when it differs from {@code oldPartitionColumns}, otherwise
   * {@link Optional#empty()}. A null input list is treated as an empty list; order of columns
   * is significant (partition order affects on-disk layout).
   */
  public static Optional<TableUpdate> partitionColumnsUpdate(
      Optional<List<String>> oldPartitionColumns,
      List<String> newPartitionColumns) {
    Objects.requireNonNull(oldPartitionColumns,
        "oldPartitionColumns Optional must not be null");
    List<String> newSafe = newPartitionColumns == null
        ? Collections.emptyList()
        : newPartitionColumns;
    List<String> oldSafe = oldPartitionColumns.orElse(Collections.emptyList());
    if (oldPartitionColumns.isPresent() && oldSafe.equals(newSafe)) {
      return Optional.empty();
    }
    TableUpdate u = new SetPartitionColumnsUpdate().partitionColumns(newSafe);
    return Optional.of(u);
  }
}
