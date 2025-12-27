/*
 *  Copyright (2021) The Delta Lake Project Authors.
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

package io.delta.flink.sink.dynamic;

import java.util.Map;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.factories.DynamicTableFactory;

public class TestDynamicTableSinkContext implements DynamicTableFactory.Context {

  private final ResolvedCatalogTable table;

  public TestDynamicTableSinkContext(ResolvedCatalogTable table) {
    this.table = table;
  }

  @Override
  public ObjectIdentifier getObjectIdentifier() {
    return ObjectIdentifier.of("default", "default", "t");
  }

  @Override
  public ResolvedCatalogTable getCatalogTable() {
    return table;
  }

  @Override
  public Map<String, String> getEnrichmentOptions() {
    return Map.of();
  }

  @Override
  public Configuration getConfiguration() {
    return new Configuration();
  }

  @Override
  public ClassLoader getClassLoader() {
    return Thread.currentThread().getContextClassLoader();
  }

  @Override
  public boolean isTemporary() {
    return true;
  }
}
