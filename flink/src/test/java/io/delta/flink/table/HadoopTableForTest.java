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

package io.delta.flink.table;

import io.delta.kernel.Snapshot;
import io.delta.kernel.TableManager;
import io.delta.kernel.types.StructType;
import java.net.URI;
import java.util.List;
import java.util.Map;

public class HadoopTableForTest extends AbstractKernelTable {

  public HadoopTableForTest(
      URI tablePath, Map<String, String> conf, StructType schema, List<String> partitionColumns) {
    super(new HadoopCatalogForTest(conf), tablePath.toString(), conf, schema, partitionColumns);
  }

  @Override
  protected Snapshot loadLatestSnapshot() {
    return TableManager.loadSnapshot(getTablePath().toString()).build(getEngine());
  }
}
