/*
 * Copyright (2025) The Delta Lake Project Authors.
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
package io.delta.spark.internal.v2.snapshot;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import io.delta.spark.internal.v2.DeltaV2TestBase;
import io.delta.spark.internal.v2.snapshot.unitycatalog.UCManagedTableSnapshotManager;
import io.delta.spark.internal.v2.snapshot.unitycatalog.UCTableInfo;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/** Tests for {@link SnapshotManagerFactory} routing logic. */
public class SnapshotManagerFactoryTest extends DeltaV2TestBase {

  private static final String UC_CATALOG_URI =
      "https://uc-factory-create.example.com/api/2.1/unity-catalog";
  private static final String UC_CATALOG_TOKEN = "dapi_factory_create_token_7kP3";

  @Test
  public void testForCreateTable_withoutUCTableInfo_returnsPathBased() {
    DeltaSnapshotManager manager =
        SnapshotManagerFactory.forCreateTable("/some/path", defaultEngine, Optional.empty());
    assertInstanceOf(PathBasedSnapshotManager.class, manager);
  }

  @Test
  public void testForCreateTable_withUCTableInfo_returnsUCManaged() {
    Map<String, String> authConfig = new HashMap<>();
    authConfig.put("type", "static");
    authConfig.put("token", UC_CATALOG_TOKEN);
    UCTableInfo ucTableInfo =
        new UCTableInfo("factory_create_table_id_9x2b", "/some/path", UC_CATALOG_URI, authConfig);

    DeltaSnapshotManager manager =
        SnapshotManagerFactory.forCreateTable(
            "/some/path", defaultEngine, Optional.of(ucTableInfo));
    assertInstanceOf(UCManagedTableSnapshotManager.class, manager);
  }
}
