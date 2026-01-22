/*
 * Copyright (2021) The Delta Lake Project Authors.
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

import com.fasterxml.jackson.annotation.JsonInclude;
import io.delta.storage.commit.Commit;
import io.delta.storage.commit.actions.AbstractMetadata;
import io.delta.storage.commit.actions.AbstractProtocol;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Container for internal REST classes used by UCTokenBasedRestClient.
 * Encapsulates all necessary classes for JSON serialization/deserialization.
 */
class UCRestClientPayload {

  // ==============================
  // CommitInfo Class
  // ==============================
  static class CommitInfo {
    Long version;
    Long timestamp;
    String fileName;
    Long fileSize;
    Long fileModificationTimestamp;
    Boolean isDisownCommit;

    static CommitInfo fromCommit(Commit externalCommit, boolean isDisownCommit) {
      if (externalCommit == null) {
        throw new IllegalArgumentException("externalCommit cannot be null");
      }
      if (externalCommit.getFileStatus() == null) {
        throw new IllegalArgumentException("externalCommit.getFileStatus() cannot be null");
      }

      CommitInfo commitInfo = new CommitInfo();
      commitInfo.version = externalCommit.getVersion();
      commitInfo.timestamp = externalCommit.getCommitTimestamp();
      commitInfo.fileName = externalCommit.getFileStatus().getPath().getName();
      commitInfo.fileSize = externalCommit.getFileStatus().getLen();
      commitInfo.fileModificationTimestamp = externalCommit.getFileStatus().getModificationTime();
      commitInfo.isDisownCommit = isDisownCommit;
      return commitInfo;
    }

    static Commit toCommit(CommitInfo commitInfo, Path basePath) {
      FileStatus fileStatus = new FileStatus(
        commitInfo.fileSize,
        false /* isdir */,
        0 /* block_replication */,
        0 /* blocksize */,
        commitInfo.fileModificationTimestamp,
        new Path(basePath, commitInfo.fileName));
      return new Commit(commitInfo.version, fileStatus, commitInfo.timestamp);
    }
  }

  // ==============================
  // Protocol Class
  // ==============================
  static class Protocol {
    Integer minReaderVersion;
    Integer minWriterVersion;
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    List<String> readerFeatures;
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    List<String> writerFeatures;

    static Protocol fromAbstractProtocol(AbstractProtocol externalProtocol) {
      if (externalProtocol == null) {
        throw new IllegalArgumentException("externalProtocol cannot be null");
      }

      Protocol protocol = new Protocol();
      protocol.minReaderVersion = externalProtocol.getMinReaderVersion();
      protocol.minWriterVersion = externalProtocol.getMinWriterVersion();
      protocol.readerFeatures = new ArrayList<>(externalProtocol.getReaderFeatures());
      protocol.writerFeatures = new ArrayList<>(externalProtocol.getWriterFeatures());

      return protocol;
    }
  }

  // ==============================
  // Metadata Class
  // ==============================
  static class Metadata {
    String deltaTableId;
    String name;
    String description;
    String provider;
    OptionsKVPairs formatOptions;
    ColumnInfos schema;
    List<String> partitionColumns;
    PropertiesKVPairs properties;
    String createdTime;

    static Metadata fromAbstractMetadata(AbstractMetadata externalMetadata) {
      if (externalMetadata == null) {
        throw new IllegalArgumentException("externalMetadata cannot be null");
      }

      Metadata metadata = new Metadata();
      metadata.deltaTableId = externalMetadata.getId();
      metadata.name = externalMetadata.getName();
      metadata.description = externalMetadata.getDescription();
      metadata.provider = externalMetadata.getProvider();
      metadata.formatOptions = OptionsKVPairs.fromFormatOptions(
        externalMetadata.getFormatOptions());
      metadata.schema = ColumnInfos.fromSchemaString(externalMetadata.getSchemaString());
      metadata.partitionColumns = externalMetadata.getPartitionColumns();
      metadata.properties = PropertiesKVPairs.fromProperties(externalMetadata.getConfiguration());
      metadata.createdTime = externalMetadata.getCreatedTime().toString(); // Assuming ISO format

      return metadata;
    }
  }

  // ==============================
  // OptionsKVPairs Class
  // ==============================
  static class OptionsKVPairs {
    Map<String, String> options;

    static OptionsKVPairs fromFormatOptions(Map<String, String> externalOptions) {
      if (externalOptions == null) {
        throw new IllegalArgumentException("externalOptions cannot be null");
      }

      OptionsKVPairs kvPairs = new OptionsKVPairs();
      kvPairs.options = externalOptions;
      return kvPairs;
    }
  }

  // ==============================
  // PropertiesKVPairs Class
  // ==============================
  static class PropertiesKVPairs {
    Map<String, String> properties;

    static PropertiesKVPairs fromProperties(Map<String, String> externalProperties) {
      if (externalProperties == null) {
        throw new IllegalArgumentException("externalProperties cannot be null");
      }

      PropertiesKVPairs kvPairs = new PropertiesKVPairs();
      kvPairs.properties = externalProperties;
      return kvPairs;
    }
  }

  // ==============================
  // ColumnInfos Class
  // ==============================
  static class ColumnInfos {
    List<ColumnInfo> columns;

    static ColumnInfos fromSchemaString(String schemaString) {
      // TODO: Implement actual schema parsing logic based on schema format
      return null;
    }

    static class ColumnInfo {
      String name;
      String type;
      Boolean nullable;

      static ColumnInfo fromColumnDetails(String name, String type, Boolean nullable) {
        if (name == null || type == null || nullable == null) {
          throw new IllegalArgumentException("Column details cannot be null");
        }

        ColumnInfo columnInfo = new ColumnInfo();
        columnInfo.name = name;
        columnInfo.type = type;
        columnInfo.nullable = nullable;
        return columnInfo;
      }
    }
  }

  // ==============================
  // CommitRequest Class
  // ==============================
  static class CommitRequest {
    String tableId;
    String tableUri;
    CommitInfo commitInfo;
    Long latestBackfilledVersion;
    Metadata metadata;
    Protocol protocol;
  }

  // ==============================
  // GetCommitsRequest Class
  // ==============================
  static class GetCommitsRequest {
    String tableId;
    String tableUri;
    Long startVersion;
    Long endVersion;
  }

  // ==============================
  // RestGetCommitsResponse Class
  // ==============================
  static class RestGetCommitsResponse {
    public List<CommitInfo> commits;
    public Long latestTableVersion;
  }

  // ==============================
  // GetMetastoreSummaryResponse Class
  // ==============================
  static class GetMetastoreSummaryResponse {
    String metastoreId;
  }
}
