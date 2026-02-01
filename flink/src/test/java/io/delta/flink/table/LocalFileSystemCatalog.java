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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public class LocalFileSystemCatalog implements DeltaCatalog {

  private final Map<String, String> configurations;

  public LocalFileSystemCatalog(Map<String, String> conf) {
    this.configurations = conf;
  }

  @Override
  public TableDescriptor getTable(String tableId) {
    URI tablePath = AbstractKernelTable.normalize(URI.create(tableId));
    if (!Files.exists(Path.of(tablePath.resolve("_delta_log")))) {
      throw new ExceptionUtils.ResourceNotFoundException("");
    }
    TableDescriptor info = new TableDescriptor();
    info.tableId = tableId;
    info.tablePath = tablePath;
    info.uuid = tableId;
    return info;
  }

  @Override
  public void createTable(
      String tableId,
      StructType schema,
      List<String> partitions,
      Map<String, String> properties,
      Consumer<TableDescriptor> callback) {
    TableDescriptor desc = new TableDescriptor(tableId, tableId, URI.create(tableId));
    callback.accept(desc);
  }

  @Override
  public Map<String, String> getCredentials(String uuid) {
    return configurations;
  }
}
