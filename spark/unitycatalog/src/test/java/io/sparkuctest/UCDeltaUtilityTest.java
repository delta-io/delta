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

package io.sparkuctest;

import java.util.List;
import org.assertj.core.api.Assertions;

public class UCDeltaUtilityTest extends UCDeltaTableIntegrationBaseTest {

  @TestAllTableTypes
  public void testDescribeHistory(TableType tableType) throws Exception {
    withNewTable(
        "describe_history",
        "id INT, name STRING",
        tableType,
        tableName -> {
          List<List<String>> results = sql("DESCRIBE HISTORY %s", tableName);
          Assertions.assertThat(results).hasSize(1);
          Assertions.assertThat(results.get(0)).isEqualTo(List.of("id", "name"));

          // The 1st insert
          sql("INSERT INTO %s VALUES (1, 'AAA')", tableName);
          check(tableName, List.of(List.of("1", "AAA")));

          // Get the history again.
          results = sql("DESCRIBE HISTORY %s", tableName);
          Assertions.assertThat(results).hasSize(2);
        });
  }
}
