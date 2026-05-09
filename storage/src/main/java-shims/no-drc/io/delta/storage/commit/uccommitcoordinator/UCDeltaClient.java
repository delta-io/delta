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
import io.delta.storage.commit.GetCommitsResponse;
import io.delta.storage.commit.UpdatedActions;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * No-DRC stub of UCDeltaClient. Compiled when -DdeltaRestCatalog is not set (the default).
 * Delegates commit/getCommits to the legacy UCClient. Catalog operations (loadTable, createTable)
 * return null/false, signaling the caller to use the legacy path.
 */
public class UCDeltaClient {

    private final UCClient legacyClient;

    public UCDeltaClient(UCClient legacyClient) {
        this.legacyClient = legacyClient;
    }

    /**
     * Factory method. In the no-DRC stub, the delegate is ignored — only the legacy
     * client is used.
     *
     * @param delegate the catalog delegate (UCSingleCatalog or UCProxy); unused in no-DRC
     * @param legacyClient the legacy UCClient for commit/getCommits
     * @return a new UCDeltaClient backed by the legacy client
     */
    public static UCDeltaClient createFromCatalogDelegate(
            Object delegate, UCClient legacyClient) {
        return new UCDeltaClient(legacyClient);
    }

    /** Result of a loadTable call. */
    public static class LoadResult {
        private final String location;
        private final String tableId;
        private final String tableType;
        private final String etag;

        public LoadResult(String location, String tableId, String tableType, String etag) {
            this.location = location;
            this.tableId = tableId;
            this.tableType = tableType;
            this.etag = etag;
        }

        public String getLocation() { return location; }
        public String getTableId() { return tableId; }
        public String getTableType() { return tableType; }
        public String getEtag() { return etag; }
    }

    /**
     * No-DRC stub: returns null. Caller must fall through to the legacy path
     * (super.loadTable() via UCProxy).
     */
    public LoadResult loadTable(String catalog, String schema, String table) {
        return null;
    }

    /**
     * No-DRC stub: returns false. Caller must fall through to the legacy path
     * (super.createTable() via UCProxy).
     */
    public boolean createTable(
            String name,
            String location,
            String tableType,
            String dataSourceFormat,
            String schemaJson,
            List<String> partitionColumns,
            Map<String, String> properties,
            int minReaderVersion,
            int minWriterVersion,
            Set<String> readerFeatures,
            Set<String> writerFeatures,
            Map<String, String> domainMetadata) {
        return false;
    }

    /**
     * No-DRC stub: delegates to legacy UCClient.commit().
     */
    public void commit(
            String tableId,
            URI tableUri,
            Optional<Commit> commit,
            Optional<Long> lastKnownBackfilledVersion,
            boolean disown,
            UpdatedActions updatedActions)
            throws IOException, CommitFailedException, UCCommitCoordinatorException {
        legacyClient.commit(
            tableId,
            tableUri,
            commit,
            lastKnownBackfilledVersion,
            disown,
            updatedActions.getNewMetadata() == updatedActions.getOldMetadata() ?
                Optional.empty() : Optional.of(updatedActions.getNewMetadata()),
            updatedActions.getNewProtocol() == updatedActions.getOldProtocol() ?
                Optional.empty() : Optional.of(updatedActions.getNewProtocol()),
            Optional.empty() // uniform — not supported in legacy path
        );
    }

    /**
     * No-DRC stub: delegates to legacy UCClient.getCommits().
     */
    public GetCommitsResponse getCommits(
            String tableId,
            URI tableUri,
            Optional<Long> startVersion,
            Optional<Long> endVersion)
            throws IOException, UCCommitCoordinatorException {
        return legacyClient.getCommits(tableId, tableUri, startVersion, endVersion);
    }
}
