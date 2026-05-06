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

package io.delta.kernel.unitycatalog;

import static java.util.Objects.requireNonNull;

/** Logical Unity Catalog table identifier used for create-time finalization. */
public final class UCTableIdentifier {
  private final String catalogName;
  private final String schemaName;
  private final String tableName;

  public UCTableIdentifier(String catalogName, String schemaName, String tableName) {
    this.catalogName = requireNonNull(catalogName, "catalogName is null");
    this.schemaName = requireNonNull(schemaName, "schemaName is null");
    this.tableName = requireNonNull(tableName, "tableName is null");
  }

  public String getCatalogName() {
    return catalogName;
  }

  public String getSchemaName() {
    return schemaName;
  }

  public String getTableName() {
    return tableName;
  }
}
