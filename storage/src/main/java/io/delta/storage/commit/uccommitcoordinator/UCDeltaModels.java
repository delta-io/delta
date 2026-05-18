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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/** Delta-owned models for UC Delta APIs. */
public final class UCDeltaModels {
  private UCDeltaModels() {}

  public enum TableType {
    MANAGED,
    EXTERNAL
  }

  public enum DataSourceFormat {
    DELTA("DELTA"),
    ICEBERG("ICEBERG");

    private final String value;

    DataSourceFormat(String value) {
      this.value = value;
    }

    public String getValue() {
      return value;
    }
  }

  public static class DeltaProtocol {
    private Integer minReaderVersion;
    private Integer minWriterVersion;
    private List<String> readerFeatures;
    private List<String> writerFeatures;

    public DeltaProtocol minReaderVersion(Integer minReaderVersion) {
      this.minReaderVersion = minReaderVersion;
      return this;
    }

    public Integer getMinReaderVersion() {
      return minReaderVersion;
    }

    public void setMinReaderVersion(Integer minReaderVersion) {
      this.minReaderVersion = minReaderVersion;
    }

    public DeltaProtocol minWriterVersion(Integer minWriterVersion) {
      this.minWriterVersion = minWriterVersion;
      return this;
    }

    public Integer getMinWriterVersion() {
      return minWriterVersion;
    }

    public void setMinWriterVersion(Integer minWriterVersion) {
      this.minWriterVersion = minWriterVersion;
    }

    public DeltaProtocol readerFeatures(List<String> readerFeatures) {
      this.readerFeatures = readerFeatures;
      return this;
    }

    public DeltaProtocol addReaderFeaturesItem(String readerFeaturesItem) {
      if (readerFeatures == null) {
        readerFeatures = new ArrayList<>();
      }
      readerFeatures.add(readerFeaturesItem);
      return this;
    }

    public List<String> getReaderFeatures() {
      return readerFeatures == null ? Collections.emptyList() : readerFeatures;
    }

    public void setReaderFeatures(List<String> readerFeatures) {
      this.readerFeatures = readerFeatures;
    }

    public DeltaProtocol writerFeatures(List<String> writerFeatures) {
      this.writerFeatures = writerFeatures;
      return this;
    }

    public DeltaProtocol addWriterFeaturesItem(String writerFeaturesItem) {
      if (writerFeatures == null) {
        writerFeatures = new ArrayList<>();
      }
      writerFeatures.add(writerFeaturesItem);
      return this;
    }

    public List<String> getWriterFeatures() {
      return writerFeatures == null ? Collections.emptyList() : writerFeatures;
    }

    public void setWriterFeatures(List<String> writerFeatures) {
      this.writerFeatures = writerFeatures;
    }
  }

  public static final class CreateTableRequest {
    private String name;
    private String location;
    private TableType tableType;
    private DataSourceFormat dataSourceFormat;
    private String comment;
    private String schemaString;
    private List<String> partitionColumns;
    private DeltaProtocol protocol;
    private Map<String, String> properties;
    private Long lastCommitTimestampMs;

    public CreateTableRequest name(String name) {
      this.name = name;
      return this;
    }

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public CreateTableRequest location(String location) {
      this.location = location;
      return this;
    }

    public String getLocation() {
      return location;
    }

    public void setLocation(String location) {
      this.location = location;
    }

    public CreateTableRequest tableType(TableType tableType) {
      this.tableType = tableType;
      return this;
    }

    public TableType getTableType() {
      return tableType;
    }

    public void setTableType(TableType tableType) {
      this.tableType = tableType;
    }

    public CreateTableRequest dataSourceFormat(DataSourceFormat dataSourceFormat) {
      this.dataSourceFormat = dataSourceFormat;
      return this;
    }

    public DataSourceFormat getDataSourceFormat() {
      return dataSourceFormat;
    }

    public void setDataSourceFormat(DataSourceFormat dataSourceFormat) {
      this.dataSourceFormat = dataSourceFormat;
    }

    public CreateTableRequest comment(String comment) {
      this.comment = comment;
      return this;
    }

    public String getComment() {
      return comment;
    }

    public void setComment(String comment) {
      this.comment = comment;
    }

    public CreateTableRequest schemaString(String schemaString) {
      this.schemaString = schemaString;
      return this;
    }

    public String getSchemaString() {
      return schemaString;
    }

    public void setSchemaString(String schemaString) {
      this.schemaString = schemaString;
    }

    public CreateTableRequest partitionColumns(List<String> partitionColumns) {
      this.partitionColumns = partitionColumns;
      return this;
    }

    public CreateTableRequest addPartitionColumnsItem(String partitionColumnsItem) {
      if (partitionColumns == null) {
        partitionColumns = new ArrayList<>();
      }
      partitionColumns.add(partitionColumnsItem);
      return this;
    }

    public List<String> getPartitionColumns() {
      return partitionColumns == null ? Collections.emptyList() : partitionColumns;
    }

    public void setPartitionColumns(List<String> partitionColumns) {
      this.partitionColumns = partitionColumns;
    }

    public CreateTableRequest protocol(DeltaProtocol protocol) {
      this.protocol = protocol;
      return this;
    }

    public DeltaProtocol getProtocol() {
      return protocol;
    }

    public void setProtocol(DeltaProtocol protocol) {
      this.protocol = protocol;
    }

    public CreateTableRequest properties(Map<String, String> properties) {
      this.properties = properties;
      return this;
    }

    public CreateTableRequest putPropertiesItem(String key, String propertiesItem) {
      if (properties == null) {
        properties = new LinkedHashMap<>();
      }
      properties.put(key, propertiesItem);
      return this;
    }

    public Map<String, String> getProperties() {
      return properties == null ? Collections.emptyMap() : properties;
    }

    public void setProperties(Map<String, String> properties) {
      this.properties = properties;
    }

    public CreateTableRequest lastCommitTimestampMs(Long lastCommitTimestampMs) {
      this.lastCommitTimestampMs = lastCommitTimestampMs;
      return this;
    }

    public Long getLastCommitTimestampMs() {
      return lastCommitTimestampMs;
    }

    public void setLastCommitTimestampMs(Long lastCommitTimestampMs) {
      this.lastCommitTimestampMs = lastCommitTimestampMs;
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

    public UUID getTableId() {
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
