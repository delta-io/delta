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

import io.unitycatalog.client.auth.TokenProvider;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * No-DRC stub of UCDeltaClient. Compiled when -DdeltaRestCatalog is not set (the default).
 *
 * Extends {@link UCTokenBasedRestClient} so it IS-A {@link UCClient} and can be used
 * as a drop-in replacement anywhere a UCClient is accepted. All inherited methods
 * (commit, getCommits, getMetastoreId, close) work via the parent's legacy API.
 *
 * Catalog operations (loadTable, createTable) return sentinel values signaling the
 * caller to fall through to the legacy path (super.loadTable() via UCProxy).
 */
public class UCDeltaClient extends UCTokenBasedRestClient {

    /**
     * Constructs a UCDeltaClient that delegates everything to the legacy API.
     *
     * @param baseUri the UC server base URI
     * @param tokenProvider the token provider for authentication
     * @param appVersions application version map for telemetry
     */
    public UCDeltaClient(
            String baseUri,
            TokenProvider tokenProvider,
            Map<String, String> appVersions) {
        super(baseUri, tokenProvider, appVersions);
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
}
