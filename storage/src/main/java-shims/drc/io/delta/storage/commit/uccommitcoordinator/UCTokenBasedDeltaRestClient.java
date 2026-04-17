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

import io.delta.storage.commit.Commit;
import io.delta.storage.commit.CommitFailedException;
import io.delta.storage.commit.CoordinatedCommitsUtils;
import io.delta.storage.commit.GetCommitsResponse;
import io.delta.storage.commit.TableDescriptor;
import io.delta.storage.commit.actions.AbstractMetadata;
import io.delta.storage.commit.actions.AbstractProtocol;
import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.delta.DeltaModelUtils;
import io.unitycatalog.client.delta.api.TablesApi;
import io.unitycatalog.client.delta.model.*;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.util.*;

/**
 * DRC-enabled subclass of UCTokenBasedRestClient. Creates the Delta REST Catalog
 * TablesApi from the shared ApiClient. When DRC is enabled, commit/getCommits/
 * loadTable/createTable use the DRC API; otherwise falls back to the legacy
 * UCClient methods in the superclass.
 * Compiled only when building with -DdeltaRestCatalog=true (UC master).
 */
public class UCTokenBasedDeltaRestClient
    extends UCTokenBasedRestClient implements UCDeltaClient {

  private final TablesApi deltaTablesApi;
  private final boolean drcEnabled;

  public UCTokenBasedDeltaRestClient(ApiClient apiClient, boolean drcEnabled) {
    super(apiClient);
    this.drcEnabled = drcEnabled;
    this.deltaTablesApi = drcEnabled ? new TablesApi(apiClient) : null;
  }

  public UCTokenBasedDeltaRestClient(
      String uri,
      io.unitycatalog.client.auth.TokenProvider tokenProvider,
      java.util.Map<String, String> appVersions,
      boolean drcEnabled) {
    super(uri, tokenProvider, appVersions);
    this.drcEnabled = drcEnabled;
    this.deltaTablesApi = drcEnabled ? new TablesApi(this.apiClient) : null;
  }

  // ---- createTable ----

  @Override
  public void createTable(
      String catalog, String schema, String table, String location,
      AbstractMetadata metadata, AbstractProtocol protocol,
      boolean isManaged, String dataSourceFormat,
      List<? extends io.delta.storage.commit.actions.AbstractDomainMetadata> domainMetadata)
      throws CommitFailedException {
    if (!drcEnabled) {
      createTableLegacy(catalog, schema, table, location, metadata, protocol, isManaged);
      return;
    }
    try {
      CreateTableRequest request = new CreateTableRequest();
      request.setName(table);
      request.setLocation(location);
      request.setTableType(isManaged ? TableType.MANAGED : TableType.EXTERNAL);
      request.setDataSourceFormat(DataSourceFormat.fromValue(dataSourceFormat));
      try {
        request.setColumns(DeltaModelUtils.schemaFromJson(metadata.getSchemaString()));
      } catch (IOException e) {
        throw new CommitFailedException(false, false,
            "Failed to parse schema JSON: " + e.getMessage(), e);
      }
      List<String> rf = protocol.getReaderFeatures() != null
          ? new ArrayList<>(protocol.getReaderFeatures()) : null;
      List<String> wf = protocol.getWriterFeatures() != null
          ? new ArrayList<>(protocol.getWriterFeatures()) : null;
      request.setProtocol(DeltaModelUtils.protocol(
          protocol.getMinReaderVersion(), protocol.getMinWriterVersion(), rf, wf));
      request.setProperties(metadata.getConfiguration());
      if (domainMetadata != null) {
        Map<String, String> active = new LinkedHashMap<>();
        for (io.delta.storage.commit.actions.AbstractDomainMetadata dm : domainMetadata) {
          if (!dm.isRemoved()) {
            active.put(dm.getDomain(), dm.getConfiguration());
          }
        }
        if (!active.isEmpty()) {
          DomainMetadataUpdates domainUpdates = buildDomainMetadataUpdates(active);
          if (domainUpdates != null) {
            request.setDomainMetadata(domainUpdates);
          }
        }
      }
      deltaTablesApi.createTable(catalog, schema, request);
    } catch (ApiException e) {
      throw new CommitFailedException(true, false,
          "Failed to create table via DRC: " + e.getResponseBody(), e);
    } catch (IOException e) {
      throw new CommitFailedException(false, false,
          "Failed to build create table request: " + e.getMessage(), e);
    }
  }

  // ---- commit (Spark V1) ----

  @Override
  public void commit(
      TableDescriptor tableDesc, Path logPath,
      Optional<Commit> commit, Optional<Long> lastKnownBackfilledVersion,
      boolean disown,
      Optional<AbstractMetadata> oldMetadata, Optional<AbstractMetadata> newMetadata,
      Optional<AbstractProtocol> oldProtocol, Optional<AbstractProtocol> newProtocol,
      Optional<io.delta.storage.commit.uniform.UniformMetadata> uniform,
      Optional<String> etag)
      throws IOException, CommitFailedException {
    String[] tp = extractTablePath(tableDesc);
    String ucTableId = extractTableId(tableDesc);
    URI tableUri = CoordinatedCommitsUtils.getTablePath(logPath).toUri();
    commitInternal(
        tp != null ? tp[0] : null, tp != null ? tp[1] : null, tp != null ? tp[2] : null,
        ucTableId, tableUri,
        commit, lastKnownBackfilledVersion, disown,
        oldMetadata, newMetadata, oldProtocol, newProtocol, uniform, etag);
  }

  // ---- commit (Kernel V2) ----

  @Override
  public void commit(
      String catalog, String schema, String table,
      String ucTableId, URI tableUri,
      Optional<Commit> commit, Optional<Long> lastKnownBackfilledVersion,
      boolean disown,
      Optional<AbstractMetadata> oldMetadata, Optional<AbstractMetadata> newMetadata,
      Optional<AbstractProtocol> oldProtocol, Optional<AbstractProtocol> newProtocol,
      Optional<io.delta.storage.commit.uniform.UniformMetadata> uniform,
      Optional<String> etag)
      throws IOException, CommitFailedException {
    commitInternal(catalog, schema, table, ucTableId, tableUri,
        commit, lastKnownBackfilledVersion, disown,
        oldMetadata, newMetadata, oldProtocol, newProtocol, uniform, etag);
  }

  private void commitInternal(
      String catalog, String schema, String table,
      String ucTableId, URI tableUri,
      Optional<Commit> commit, Optional<Long> lastKnownBackfilledVersion,
      boolean disown,
      Optional<AbstractMetadata> oldMetadata, Optional<AbstractMetadata> newMetadata,
      Optional<AbstractProtocol> oldProtocol, Optional<AbstractProtocol> newProtocol,
      Optional<io.delta.storage.commit.uniform.UniformMetadata> uniform,
      Optional<String> etag)
      throws IOException, CommitFailedException {
    if (disown || !drcEnabled || catalog == null) {
      // Legacy API does not propagate metadata to UC. If metadata actually
      // changed (old != new), the kill switch should have blocked it.
      boolean metadataChanged = oldMetadata.isPresent() && newMetadata.isPresent()
          && !oldMetadata.get().equals(newMetadata.get());
      boolean protocolChanged = oldProtocol.isPresent() && newProtocol.isPresent()
          && !oldProtocol.get().equals(newProtocol.get());
      if ((metadataChanged || protocolChanged) && !disown) {
        throw new CommitFailedException(false, false,
            "Metadata/protocol changes require Delta REST Catalog but DRC " +
            "is not available. Enable deltaRestCatalog to propagate to UC.");
      }
      try {
        super.commit(ucTableId, tableUri,
            commit, lastKnownBackfilledVersion, disown,
            Optional.empty(), Optional.empty(), uniform);
      } catch (UCCommitCoordinatorException e) {
        throw new CommitFailedException(true, false, e.getMessage(), e);
      }
      return;
    }
    try {
      UpdateTableRequest updateRequest = new UpdateTableRequest();

      // Optimistic concurrency requirements from table metadata
      if (!etag.isPresent()) {
        throw new CommitFailedException(false, false,
            "DRC commit requires etag but none was provided. " +
            "This indicates the table snapshot was not loaded via DRC.");
      }
      List<TableRequirement> requirements = new ArrayList<>();
      AssertTableUUID uuidReq = new AssertTableUUID();
      uuidReq.setUuid(java.util.UUID.fromString(ucTableId));
      requirements.add(uuidReq);
      AssertEtag etagReq = new AssertEtag();
      etagReq.setEtag(etag.get());
      requirements.add(etagReq);
      updateRequest.setRequirements(requirements);

      // Diff old vs new metadata/protocol, only send changed fields
      List<TableUpdate> updates = new ArrayList<>();
      if (newMetadata.isPresent()) {
        try {
          addMetadataUpdates(updates, oldMetadata.orElse(null), newMetadata.get());
        } catch (IOException e) { throw new RuntimeException(e); }
      }
      if (newProtocol.isPresent()) {
        addProtocolUpdate(updates, oldProtocol.orElse(null), newProtocol.get());
      }
      commit.ifPresent(c -> {
        AddCommitUpdate addCommit = new AddCommitUpdate();
        addCommit.setCommit(DeltaModelUtils.commit(
            c.getVersion(), c.getCommitTimestamp(),
            c.getFileStatus().getPath().getName(),
            c.getFileStatus().getLen(),
            c.getFileStatus().getModificationTime()));
        uniform.flatMap(u -> u.getIcebergMetadata()).ifPresent(iceberg -> {
          UniformMetadata um = new UniformMetadata();
          UniformMetadataIceberg ice = new UniformMetadataIceberg();
          ice.setMetadataLocation(iceberg.getMetadataLocation());
          ice.setConvertedDeltaVersion(iceberg.getConvertedDeltaVersion());
          try {
            ice.setConvertedDeltaTimestamp(
                Long.parseLong(iceberg.getConvertedDeltaTimestamp()));
          } catch (NumberFormatException e) { /* skip */ }
          um.setIceberg(ice);
          addCommit.setUniform(um);
        });
        updates.add(addCommit);
      });
      lastKnownBackfilledVersion.ifPresent(v -> {
        SetLatestBackfilledVersionUpdate bfu = new SetLatestBackfilledVersionUpdate();
        bfu.setLatestPublishedVersion(v);
        updates.add(bfu);
      });

      updateRequest.setUpdates(updates);
      deltaTablesApi.updateTable(catalog, schema, table, updateRequest);
    } catch (ApiException e) {
      if (e.getCode() == 409) {
        throw new CommitFailedException(true, true,
            "Commit conflict: " + e.getResponseBody(), e);
      } else if (e.getCode() >= 400 && e.getCode() < 500) {
        throw new CommitFailedException(false, false,
            "Bad request: " + e.getResponseBody(), e);
      } else {
        throw new CommitFailedException(true, false,
            "Commit failed with HTTP " + e.getCode() + ": " + e.getResponseBody(), e);
      }
    }
  }

  // ---- loadTable (DRC: single RPC for table state + commits + etag) ----

  @Override
  public UCDeltaClient.LoadTableResponse loadTable(
      String catalog, String schema, String table,
      String ucTableId, URI tableUri,
      Optional<Long> startVersion, Optional<Long> endVersion) throws IOException {
    if (!drcEnabled || catalog == null) {
      // Legacy: combine getTable + getCommits
      try {
        GetCommitsResponse commits =
            super.getCommits(ucTableId, tableUri, startVersion, endVersion);
        // No etag in legacy path
        return new UCDeltaClient.LoadTableResponse(commits, null,
            null, ucTableId, null, null);
      } catch (UCCommitCoordinatorException e) {
        throw new IOException(e);
      }
    }
    try {
      io.unitycatalog.client.delta.model.LoadTableResponse response =
          deltaTablesApi.loadTable(catalog, schema, table);
      io.unitycatalog.client.delta.model.TableMetadata meta = response.getMetadata();

      List<io.unitycatalog.client.delta.model.DeltaCommit> deltaCommits =
          response.getCommits();
      if (deltaCommits == null) deltaCommits = new ArrayList<>();

      long start = startVersion.orElse(0L);
      long end = endVersion.orElse(Long.MAX_VALUE);
      String tableLoc = meta.getLocation();
      Path commitDir = CoordinatedCommitsUtils.commitDirPath(
          CoordinatedCommitsUtils.logDirPath(new Path(tableLoc)));

      List<Commit> commits = new ArrayList<>();
      for (io.unitycatalog.client.delta.model.DeltaCommit dc : deltaCommits) {
        if (dc.getVersion() >= start && dc.getVersion() <= end) {
          FileStatus fs = new FileStatus(
              dc.getFileSize(), false, 0, 0,
              dc.getFileModificationTimestamp(),
              new Path(commitDir, dc.getFileName()));
          commits.add(new Commit(dc.getVersion(), fs, dc.getTimestamp()));
        }
      }
      Long latestVersion = response.getLatestTableVersion();
      GetCommitsResponse commitsResponse = new GetCommitsResponse(
          commits, latestVersion != null ? latestVersion : -1L, meta.getEtag());

      String tableType = meta.getTableType() != null
          ? meta.getTableType().getValue() : "MANAGED";
      String tableId = meta.getTableUuid() != null
          ? meta.getTableUuid().toString() : "";

      // Adapt DRC model objects to storage-module interfaces (zero-copy)
      AbstractProtocol protocol = meta.getProtocol() != null
          ? new DRCProtocolAdapter(meta.getProtocol()) : null;

      AbstractMetadata metadata = new DRCMetadataAdapter(meta);

      return new UCDeltaClient.LoadTableResponse(
          commitsResponse, meta.getEtag(),
          tableLoc, tableId, tableType,
          meta.getProperties(),
          protocol, metadata,
          vendCredentials(tableId));
    } catch (ApiException e) {
      throw new IOException("Failed to load table via DRC: " + e.getResponseBody(), e);
    }
  }

  // ---- Helpers ----

  private static String extractTableId(TableDescriptor tableDesc) {
    Map<String, String> tableConf = tableDesc.getTableConf();
    String id = tableConf.get(UCCommitCoordinatorClient.UC_TABLE_ID_KEY);
    if (id == null) {
      throw new IllegalStateException("UC Table ID not found in " + tableConf);
    }
    return id;
  }

  private static String[] extractTablePath(TableDescriptor tableDesc) {
    if (!tableDesc.getTableIdentifier().isPresent()) return null;
    io.delta.storage.commit.TableIdentifier tableId = tableDesc.getTableIdentifier().get();
    String[] namespace = tableId.getNamespace();
    if (namespace.length != 2) return null;
    return new String[]{namespace[0], namespace[1], tableId.getName()};
  }

  private DomainMetadataUpdates buildDomainMetadataUpdates(
      Map<String, String> domainMetadata) throws IOException {
    DomainMetadataUpdates updates = new DomainMetadataUpdates();
    com.fasterxml.jackson.databind.ObjectMapper mapper =
        new com.fasterxml.jackson.databind.ObjectMapper();
    boolean hasUpdates = false;
    String clusteringConfig = domainMetadata.get("delta.clustering");
    if (clusteringConfig != null) {
      updates.setDeltaClustering(
          mapper.readValue(clusteringConfig, ClusteringDomainMetadata.class));
      hasUpdates = true;
    }
    String rowTrackingConfig = domainMetadata.get("delta.rowTracking");
    if (rowTrackingConfig != null) {
      updates.setDeltaRowTracking(
          mapper.readValue(rowTrackingConfig, RowTrackingDomainMetadata.class));
      hasUpdates = true;
    }
    return hasUpdates ? updates : null;
  }

  /** Diff old vs new metadata and generate only the changed update actions. */
  private void addMetadataUpdates(
      List<TableUpdate> updates,
      AbstractMetadata oldMd, AbstractMetadata newMd) throws IOException {
    if (!Objects.equals(
        oldMd != null ? oldMd.getDescription() : null, newMd.getDescription())) {
      SetTableCommentUpdate cu = new SetTableCommentUpdate();
      cu.setComment(newMd.getDescription());
      updates.add(cu);
    }
    if (!Objects.equals(
        oldMd != null ? oldMd.getPartitionColumns() : null,
        newMd.getPartitionColumns())) {
      SetPartitionColumnsUpdate pu = new SetPartitionColumnsUpdate();
      pu.setPartitionColumns(newMd.getPartitionColumns() != null
          ? new ArrayList<>(newMd.getPartitionColumns()) : Collections.emptyList());
      updates.add(pu);
    }
    if (!Objects.equals(
        oldMd != null ? oldMd.getSchemaString() : null, newMd.getSchemaString())) {
      SetSchemaUpdate su = new SetSchemaUpdate();
      su.setColumns(DeltaModelUtils.schemaFromJson(newMd.getSchemaString()));
      updates.add(su);
    }
    Map<String, String> oldProps = oldMd != null && oldMd.getConfiguration() != null
        ? oldMd.getConfiguration() : Collections.emptyMap();
    Map<String, String> newProps = newMd.getConfiguration() != null
        ? newMd.getConfiguration() : Collections.emptyMap();
    Map<String, String> changed = new LinkedHashMap<>();
    for (Map.Entry<String, String> entry : newProps.entrySet()) {
      if (!Objects.equals(oldProps.get(entry.getKey()), entry.getValue())) {
        changed.put(entry.getKey(), entry.getValue());
      }
    }
    if (!changed.isEmpty()) {
      SetPropertiesUpdate sp = new SetPropertiesUpdate();
      sp.setUpdates(changed);
      updates.add(sp);
    }
  }

  private void addProtocolUpdate(
      List<TableUpdate> updates,
      AbstractProtocol oldProto, AbstractProtocol newProto) {
    // Always send protocol when it changed (caller already filtered)
    SetProtocolUpdate pu = new SetProtocolUpdate();
    pu.setProtocol(DeltaModelUtils.protocol(
        newProto.getMinReaderVersion(),
        newProto.getMinWriterVersion(),
        newProto.getReaderFeatures() != null
            ? new ArrayList<>(newProto.getReaderFeatures()) : null,
        newProto.getWriterFeatures() != null
            ? new ArrayList<>(newProto.getWriterFeatures()) : null));
    updates.add(pu);
  }
}
