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

import java.util.Objects;

/**
 * A three-part table identifier returned by {@link UCDeltaClient#listTables}.
 * {@code dataSourceFormat} mirrors the server's string enum (e.g. {@code "DELTA"},
 * {@code "ICEBERG"}); callers that only care about Delta tables filter client-side.
 */
public final class UCTableIdentifier {

  private final String schema;
  private final String name;
  private final String dataSourceFormat;

  public UCTableIdentifier(String schema, String name, String dataSourceFormat) {
    this.schema = Objects.requireNonNull(schema, "schema");
    this.name = Objects.requireNonNull(name, "name");
    this.dataSourceFormat = Objects.requireNonNull(dataSourceFormat, "dataSourceFormat");
  }

  public String getSchema() { return schema; }
  public String getName() { return name; }
  public String getDataSourceFormat() { return dataSourceFormat; }

  @Override
  public String toString() {
    return schema + "." + name + "[" + dataSourceFormat + "]";
  }
}
