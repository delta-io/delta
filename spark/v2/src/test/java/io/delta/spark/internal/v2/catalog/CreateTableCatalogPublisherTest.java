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
import static org.junit.jupiter.api.Assertions.assertFalse;

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

public class CreateTableCatalogPublisherTest extends DeltaV2TestBase {
  private final CreateTableOperationPlanner planner = new CreateTableOperationPlanner();
  private final CreateTableOperationExecutor executor = new CreateTableOperationExecutor();
  private final CreateTableCatalogPublisher publisher = new CreateTableCatalogPublisher();

  @Test
  public void testBuildCatalogPublication_derivesColumnsAndCommittedProperties(
      @TempDir File tempDir) throws Exception {
    String tablePath = tempDir.getAbsolutePath();
    Map<String, String> properties = new HashMap<>();
    properties.put("delta.appendOnly", "true");
    properties.put("fs.s3a.access.key", "secret");

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
            operation, "CreateTableCatalogPublisherTest", CloseableIterable.emptyIterable());

    CreateTableCatalogPublication publication =
        publisher.buildCatalogPublication(properties, committed);

    assertEquals(2, publication.getColumns().length);
    assertEquals("id", publication.getColumns()[0].name());
    assertEquals("true", publication.getProperties().get("delta.appendOnly"));
    assertFalse(publication.getProperties().containsKey("fs.s3a.access.key"));
  }
}
