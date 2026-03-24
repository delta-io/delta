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
package io.delta.spark.internal.v2.catalog;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.utils.CloseableIterable;
import io.delta.spark.internal.v2.DeltaV2TestBase;
import java.io.File;
import java.util.HashMap;
import java.util.Map;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class CreateTableOperationExecutorTest extends DeltaV2TestBase {
  private final CreateTableOperationPlanner planner = new CreateTableOperationPlanner();
  private final CreateTableOperationExecutor executor = new CreateTableOperationExecutor();

  @Test
  public void testCommitPreparedCreate_commitsVersionZeroAndPersistsProperties(
      @TempDir File tempDir) throws Exception {
    String tablePath = tempDir.getAbsolutePath();
    Map<String, String> properties = new HashMap<>();
    properties.put("delta.appendOnly", "true");

    PreparedCreateTableOperation operation =
        planner.planCreateTable(
            Identifier.of(new String[0], tablePath),
            new StructType().add("id", DataTypes.IntegerType).add("name", DataTypes.StringType),
            new Transform[0],
            properties,
            spark,
            "unused",
            true);

    CommittedCreateTableOperation committed =
        executor.commitPreparedCreate(
            operation, "CreateTableOperationExecutorTest", CloseableIterable.emptyIterable());

    SnapshotImpl snapshot = committed.getPostCommitSnapshot();
    assertEquals(0L, snapshot.getVersion());
    assertEquals(operation.getKernelSchema(), snapshot.getSchema());
    assertEquals("true", snapshot.getMetadata().getConfiguration().get("delta.appendOnly"));
    assertEquals(defaultEngine.getFileSystemClient().resolvePath(tablePath), snapshot.getPath());
  }
}
