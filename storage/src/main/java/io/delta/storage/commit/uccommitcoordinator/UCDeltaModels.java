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

import io.delta.storage.commit.uniform.UniformMetadata;

/** Delta-owned models for UC Delta APIs. */
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

  public static final class UpdateTableRequest {
    private final List<TableRequirement> requirements = new ArrayList<>();
    private final List<TableUpdate> updates = new ArrayList<>();

    public UpdateTableRequest requirements(List<TableRequirement> requirements) {
      this.requirements.addAll(requirements);
      return this;
    }

    public UpdateTableRequest addRequirementsItem(TableRequirement item) {
      this.requirements.add(item);
      return this;
    }

    public List<TableRequirement> getRequirements() {
      return requirements;
    }

    public UpdateTableRequest updates(List<TableUpdate> updates) {
      this.updates.addAll(updates);
      return this;
    }

    public UpdateTableRequest addUpdatesItem(TableUpdate item) {
      this.updates.add(item);
      return this;
    }

    public List<TableUpdate> getUpdates() {
      return updates;
    }
  }

  public static final class TableRequirement {
    public enum Type {
      ASSERT_TABLE_UUID,
      ASSERT_ETAG
    }

    private Type type;
    private UUID uuid;
    private String etag;

    public static TableRequirement assertTableUuid(UUID uuid) {
      return new TableRequirement().type(Type.ASSERT_TABLE_UUID).uuid(uuid);
    }

    public static TableRequirement assertEtag(String etag) {
      return new TableRequirement().type(Type.ASSERT_ETAG).etag(etag);
    }

    public TableRequirement type(Type type) {
      this.type = type;
      return this;
    }

    public Type getType() {
      return type;
    }

    public TableRequirement uuid(UUID uuid) {
      this.uuid = uuid;
      return this;
    }

    public UUID getUuid() {
      return uuid;
    }

    public TableRequirement etag(String etag) {
      this.etag = etag;
      return this;
    }

    public String getEtag() {
      return etag;
    }
  }

  public static final class DeltaCommit {
    private Long version;
    private Long timestamp;
    private String fileName;
    private Long fileSize;
    private Long fileModificationTimestamp;

    public DeltaCommit version(Long version) {
      this.version = version;
      return this;
    }

    public Long getVersion() {
      return version;
    }

    public DeltaCommit timestamp(Long timestamp) {
      this.timestamp = timestamp;
      return this;
    }

    public Long getTimestamp() {
      return timestamp;
    }

    public DeltaCommit fileName(String fileName) {
      this.fileName = fileName;
      return this;
    }

    public String getFileName() {
      return fileName;
    }

    public DeltaCommit fileSize(Long fileSize) {
      this.fileSize = fileSize;
      return this;
    }

    public Long getFileSize() {
      return fileSize;
    }

    public DeltaCommit fileModificationTimestamp(Long fileModificationTimestamp) {
      this.fileModificationTimestamp = fileModificationTimestamp;
      return this;
    }

    public Long getFileModificationTimestamp() {
      return fileModificationTimestamp;
    }
  }

  public static final class TableUpdate {
    public enum Action {
      SET_PROPERTIES,
      REMOVE_PROPERTIES,
      SET_PROTOCOL,
      SET_COLUMNS,
      SET_PARTITION_COLUMNS,
      SET_TABLE_COMMENT,
      ADD_COMMIT,
      SET_LATEST_BACKFILLED_VERSION,
      UPDATE_METADATA_SNAPSHOT_VERSION
    }

    private Action action;
    private Map<String, String> propertyUpdates;
    private List<String> propertyRemovals;
    private DeltaProtocol protocol;
    private String schemaString;
    private List<String> partitionColumns;
    private String comment;
    private DeltaCommit commit;
    private UniformMetadata uniform;
    private Long latestPublishedVersion;
    private Long lastCommitVersion;
    private Long lastCommitTimestampMs;

    public static TableUpdate setProperties(Map<String, String> updates) {
      return new TableUpdate().action(Action.SET_PROPERTIES).propertyUpdates(updates);
    }

    public static TableUpdate removeProperties(List<String> removals) {
      return new TableUpdate().action(Action.REMOVE_PROPERTIES).propertyRemovals(removals);
    }

    public static TableUpdate setProtocolUpdate(DeltaProtocol protocol) {
      return new TableUpdate().action(Action.SET_PROTOCOL).protocol(protocol);
    }

    public static TableUpdate setColumns(String schemaString) {
      return new TableUpdate().action(Action.SET_COLUMNS).schemaString(schemaString);
    }

    public static TableUpdate setPartitionColumnsUpdate(List<String> partitionColumns) {
      return new TableUpdate()
          .action(Action.SET_PARTITION_COLUMNS)
          .partitionColumns(partitionColumns);
    }

    public static TableUpdate setTableComment(String comment) {
      return new TableUpdate().action(Action.SET_TABLE_COMMENT).comment(comment);
    }

    public static TableUpdate addCommit(DeltaCommit commit, UniformMetadata uniform) {
      return new TableUpdate().action(Action.ADD_COMMIT).commit(commit).uniform(uniform);
    }

    public static TableUpdate setLatestBackfilledVersion(Long latestPublishedVersion) {
      return new TableUpdate()
          .action(Action.SET_LATEST_BACKFILLED_VERSION)
          .latestPublishedVersion(latestPublishedVersion);
    }

    public static TableUpdate updateMetadataSnapshotVersion(
        Long lastCommitVersion,
        Long lastCommitTimestampMs) {
      return new TableUpdate()
          .action(Action.UPDATE_METADATA_SNAPSHOT_VERSION)
          .lastCommitVersion(lastCommitVersion)
          .lastCommitTimestampMs(lastCommitTimestampMs);
    }

    public TableUpdate action(Action action) {
      this.action = action;
      return this;
    }

    public Action getAction() {
      return action;
    }

    public TableUpdate propertyUpdates(Map<String, String> propertyUpdates) {
      this.propertyUpdates = propertyUpdates;
      return this;
    }

    public Map<String, String> getPropertyUpdates() {
      return propertyUpdates == null ? Collections.emptyMap() : propertyUpdates;
    }

    public TableUpdate propertyRemovals(List<String> propertyRemovals) {
      this.propertyRemovals = propertyRemovals;
      return this;
    }

    public List<String> getPropertyRemovals() {
      return propertyRemovals == null ? Collections.emptyList() : propertyRemovals;
    }

    public TableUpdate protocol(DeltaProtocol protocol) {
      this.protocol = protocol;
      return this;
    }

    public DeltaProtocol getProtocol() {
      return protocol;
    }

    public TableUpdate schemaString(String schemaString) {
      this.schemaString = schemaString;
      return this;
    }

    public String getSchemaString() {
      return schemaString;
    }

    public TableUpdate partitionColumns(List<String> partitionColumns) {
      this.partitionColumns = partitionColumns;
      return this;
    }

    public List<String> getPartitionColumns() {
      return partitionColumns == null ? Collections.emptyList() : partitionColumns;
    }

    public TableUpdate comment(String comment) {
      this.comment = comment;
      return this;
    }

    public String getComment() {
      return comment;
    }

    public TableUpdate commit(DeltaCommit commit) {
      this.commit = commit;
      return this;
    }

    public DeltaCommit getCommit() {
      return commit;
    }

    public TableUpdate uniform(UniformMetadata uniform) {
      this.uniform = uniform;
      return this;
    }

    public UniformMetadata getUniform() {
      return uniform;
    }

    public TableUpdate latestPublishedVersion(Long latestPublishedVersion) {
      this.latestPublishedVersion = latestPublishedVersion;
      return this;
    }

    public Long getLatestPublishedVersion() {
      return latestPublishedVersion;
    }

    public TableUpdate lastCommitVersion(Long lastCommitVersion) {
      this.lastCommitVersion = lastCommitVersion;
      return this;
    }

    public Long getLastCommitVersion() {
      return lastCommitVersion;
    }

    public TableUpdate lastCommitTimestampMs(Long lastCommitTimestampMs) {
      this.lastCommitTimestampMs = lastCommitTimestampMs;
      return this;
    }

    public Long getLastCommitTimestampMs() {
      return lastCommitTimestampMs;
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
