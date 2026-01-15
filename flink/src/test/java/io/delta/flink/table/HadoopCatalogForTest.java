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

package io.delta.flink.table;

import io.delta.kernel.types.StructType;
import java.net.URI;
import java.util.List;
import java.util.Map;

public class HadoopCatalogForTest implements DeltaCatalog {

  private final Map<String, String> configurations;

  public HadoopCatalogForTest(Map<String, String> conf) {
    this.configurations = conf;
  }

  @Override
  public TableDescriptor getTable(String tableId) {
    URI tablePath = AbstractKernelTable.normalize(URI.create(tableId));
    TableDescriptor info = new TableDescriptor();
    info.tableId = tableId;
    info.tablePath = tablePath;
    info.uuid = tableId;
    return info;
  }

  @Override
  public void createTable(
      String tableId, StructType schema, List<String> partitions, Map<String, String> properties) {}

  @Override
  public Map<String, String> getCredentials(String uuid) {
    return configurations;
  }
}
