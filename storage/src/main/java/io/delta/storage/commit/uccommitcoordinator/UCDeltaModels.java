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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Delta-owned models for the UC Delta REST Catalog API. These decouple the
 * {@link UCDeltaClient} interface from any generated SDK types.
 */
public final class UCDeltaModels {
  private UCDeltaModels() {}

  public enum TableType {
    MANAGED,
    EXTERNAL
  }

  public static class DeltaProtocol {
    private int minReaderVersion;
    private int minWriterVersion;
    private final List<String> readerFeatures = new ArrayList<>();
    private final List<String> writerFeatures = new ArrayList<>();

    public DeltaProtocol minReaderVersion(int minReaderVersion) {
      this.minReaderVersion = minReaderVersion;
      return this;
    }

    public int getMinReaderVersion() {
      return minReaderVersion;
    }

    public DeltaProtocol minWriterVersion(int minWriterVersion) {
      this.minWriterVersion = minWriterVersion;
      return this;
    }

    public int getMinWriterVersion() {
      return minWriterVersion;
    }

    public DeltaProtocol readerFeatures(List<String> newReaderFeatures) {
      readerFeatures.addAll(newReaderFeatures);
      return this;
    }

    public List<String> getReaderFeatures() {
      return readerFeatures;
    }

    public DeltaProtocol writerFeatures(List<String> newWriterFeatures) {
      this.writerFeatures.addAll(newWriterFeatures);
      return this;
    }

    public List<String> getWriterFeatures() {
      return writerFeatures;
    }
  }

  public static final class StagingTableResponse {
    private final UUID tableId;
    private final TableType tableType;
    private final String location;
    private final DeltaProtocol requiredProtocol;
    private final DeltaProtocol suggestedProtocol;
    private final Map<String, String> requiredProperties;
    private final Map<String, String> suggestedProperties;

    public StagingTableResponse(
        UUID tableId,
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

    public UUID getTableId() { return tableId; }

    public TableType getTableType() { return tableType; }

    public String getLocation() { return location; }

    public DeltaProtocol getRequiredProtocol() { return requiredProtocol; }

    public DeltaProtocol getSuggestedProtocol() { return suggestedProtocol; }

    public Map<String, String> getRequiredProperties() {
      return requiredProperties == null ? Collections.emptyMap() : requiredProperties;
    }

    public Map<String, String> getSuggestedProperties() {
      return suggestedProperties == null ? Collections.emptyMap() : suggestedProperties;
    }
  }
}
