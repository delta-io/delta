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

package org.apache.spark.sql.delta.catalog.credentials.fs;

import java.net.URI;
import java.util.Objects;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.sql.delta.catalog.credentials.UCDeltaRestCatalogApiCredentialConf;

public interface CredScopedKey {

  static CredScopedKey create(URI uri, Configuration conf) {
    String type = conf.get(UCDeltaRestCatalogApiCredentialConf.UC_CREDENTIALS_TYPE_KEY);
    if (UCDeltaRestCatalogApiCredentialConf.UC_CREDENTIALS_TYPE_PATH_VALUE.equals(type)) {
      String path = conf.get(UCDeltaRestCatalogApiCredentialConf.UC_PATH_KEY);
      String pathOperation = conf.get(UCDeltaRestCatalogApiCredentialConf.UC_PATH_OPERATION_KEY);

      return new PathCredScopedKey(path, pathOperation);
    } else if (UCDeltaRestCatalogApiCredentialConf.UC_CREDENTIALS_TYPE_TABLE_VALUE.equals(type)) {
      String tableId = conf.get(UCDeltaRestCatalogApiCredentialConf.UC_TABLE_ID_KEY);
      String tableOperation = conf.get(UCDeltaRestCatalogApiCredentialConf.UC_TABLE_OPERATION_KEY);
      return new TableCredScopedKey(tableId, tableOperation);
    }

    return new DefaultCredScopedKey(uri, conf);
  }

  class PathCredScopedKey implements CredScopedKey {
    private final String path;
    private final String pathOperation;

    public PathCredScopedKey(String path, String pathOperation) {
      this.path = path;
      this.pathOperation = pathOperation;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof PathCredScopedKey)) return false;
      PathCredScopedKey that = (PathCredScopedKey) o;
      return Objects.equals(path, that.path) && Objects.equals(pathOperation, that.pathOperation);
    }

    @Override
    public int hashCode() {
      return Objects.hash(path, pathOperation);
    }

    @Override
    public String toString() {
      return "PathCredScopedKey{path=" + path + ", op=" + pathOperation + "}";
    }
  }

  class TableCredScopedKey implements CredScopedKey {
    private final String tableId;
    private final String tableOperation;

    public TableCredScopedKey(String tableId, String tableOperation) {
      this.tableId = tableId;
      this.tableOperation = tableOperation;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof TableCredScopedKey)) return false;
      TableCredScopedKey that = (TableCredScopedKey) o;
      return Objects.equals(tableId, that.tableId)
          && Objects.equals(tableOperation, that.tableOperation);
    }

    @Override
    public int hashCode() {
      return Objects.hash(tableId, tableOperation);
    }

    @Override
    public String toString() {
      return "TableCredScopedKey{tableId=" + tableId + ", op=" + tableOperation + "}";
    }
  }

  class DefaultCredScopedKey implements CredScopedKey {
    private final String scheme;
    private final String authority;

    public DefaultCredScopedKey(URI uri, Configuration conf) {
      if (uri.getScheme() == null && uri.getAuthority() == null) {
        URI defaultUri = FileSystem.getDefaultUri(conf);
        this.scheme = defaultUri.getScheme();
        this.authority = defaultUri.getAuthority();
      } else {
        this.scheme = uri.getScheme();
        this.authority = uri.getAuthority();
      }
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof DefaultCredScopedKey)) return false;
      DefaultCredScopedKey that = (DefaultCredScopedKey) o;
      return Objects.equals(scheme, that.scheme) && Objects.equals(authority, that.authority);
    }

    @Override
    public int hashCode() {
      return Objects.hash(scheme, authority);
    }

    @Override
    public String toString() {
      return "DefaultCredScopedKey{scheme=" + scheme + ", authority=" + authority + "}";
    }
  }
}
