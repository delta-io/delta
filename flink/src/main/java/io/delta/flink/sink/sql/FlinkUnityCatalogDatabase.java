/*
 *  Copyright (2021) The Delta Lake Project Authors.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.delta.flink.sink.sql;

import io.unitycatalog.client.model.SchemaInfo;
import java.util.Map;
import java.util.Optional;
import org.apache.flink.table.catalog.CatalogDatabase;

public class FlinkUnityCatalogDatabase implements CatalogDatabase {

  final String name;
  final String description;
  final String comment;
  final Map<String, String> properties;

  public FlinkUnityCatalogDatabase(
      String name, String description, String comment, Map<String, String> properties) {
    this.name = name;
    this.description = description;
    this.comment = comment;
    this.properties = properties;
  }

  public FlinkUnityCatalogDatabase(SchemaInfo schemaInfo) {
    this(schemaInfo.getName(), "", schemaInfo.getComment(), schemaInfo.getProperties());
  }

  @Override
  public Map<String, String> getProperties() {
    return properties;
  }

  @Override
  public String getComment() {
    return comment;
  }

  @Override
  public CatalogDatabase copy() {
    return new FlinkUnityCatalogDatabase(name, description, comment, properties);
  }

  @Override
  public CatalogDatabase copy(Map<String, String> newProperties) {
    return new FlinkUnityCatalogDatabase(name, description, comment, newProperties);
  }

  @Override
  public Optional<String> getDescription() {
    return Optional.ofNullable(description);
  }

  @Override
  public Optional<String> getDetailedDescription() {
    return Optional.ofNullable(description);
  }
}
