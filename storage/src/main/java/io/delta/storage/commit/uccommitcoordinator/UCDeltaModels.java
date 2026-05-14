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

import io.delta.storage.commit.actions.AbstractProtocol;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Delta-owned models for the UC Delta REST Catalog API. These decouple the {@link UCDeltaClient}
 * interface from any generated SDK types.
 */
public final class UCDeltaModels {

  private UCDeltaModels() {}

  public enum TableType {
    MANAGED,
    EXTERNAL
  }

  public static class DeltaProtocol implements AbstractProtocol {

    private int minReaderVersion;
    private int minWriterVersion;
    private final Set<String> readerFeatures = new HashSet<>();
    private final Set<String> writerFeatures = new HashSet<>();

    @Override
    public int getMinReaderVersion() {
      return minReaderVersion;
    }

    @Override
    public int getMinWriterVersion() {
      return minWriterVersion;
    }

    @Override
    public Set<String> getReaderFeatures() {
      return readerFeatures;
    }

    @Override
    public Set<String> getWriterFeatures() {
      return writerFeatures;
    }

    public DeltaProtocol minReaderVersion(int minReaderVersion) {
      this.minReaderVersion = minReaderVersion;
      return this;
    }

    public DeltaProtocol minWriterVersion(int minWriterVersion) {
      this.minWriterVersion = minWriterVersion;
      return this;
    }

    public DeltaProtocol readerFeatures(Collection<String> readerFeatures) {
      this.readerFeatures.addAll(readerFeatures);
      return this;
    }

    public DeltaProtocol writerFeatures(Collection<String> writerFeatures) {
      this.writerFeatures.addAll(writerFeatures);
      return this;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof DeltaProtocol)) return false;
      DeltaProtocol that = (DeltaProtocol) o;
      return minReaderVersion == that.minReaderVersion
          && minWriterVersion == that.minWriterVersion
          && Objects.equals(readerFeatures, that.readerFeatures)
          && Objects.equals(writerFeatures, that.writerFeatures);
    }

    @Override
    public int hashCode() {
      return Objects.hash(minReaderVersion, minWriterVersion, readerFeatures, writerFeatures);
    }
  }

  public static final class StagingTableInfo {

    private final String tableId;
    private final TableType tableType;
    private final String location;
    private final DeltaProtocol requiredProtocol;
    private final DeltaProtocol suggestedProtocol;
    private final Map<String, String> requiredProperties;
    private final Map<String, String> suggestedProperties;

    public StagingTableInfo(
        String tableId,
        TableType tableType,
        String location,
        DeltaProtocol requiredProtocol,
        DeltaProtocol suggestedProtocol,
        Map<String, String> requiredProperties,
        Map<String, String> suggestedProperties) {
      this.tableId = tableId;
      this.tableType = tableType;
      this.location = location;
      this.requiredProtocol = requiredProtocol;
      this.suggestedProtocol = suggestedProtocol;
      this.requiredProperties = requiredProperties;
      this.suggestedProperties = suggestedProperties;
    }

    public String getTableId() {
      return tableId;
    }

    public TableType getTableType() {
      return tableType;
    }

    public String getLocation() {
      return location;
    }

    public DeltaProtocol getRequiredProtocol() {
      return requiredProtocol;
    }

    public DeltaProtocol getSuggestedProtocol() {
      return suggestedProtocol;
    }

    public Map<String, String> getRequiredProperties() {
      return requiredProperties == null ? Collections.emptyMap() : requiredProperties;
    }

    public Map<String, String> getSuggestedProperties() {
      return suggestedProperties == null ? Collections.emptyMap() : suggestedProperties;
    }
  }
}
