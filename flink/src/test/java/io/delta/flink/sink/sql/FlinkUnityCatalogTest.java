/*
 *  Copyright (2026) The Delta Lake Project Authors.
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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.delta.flink.MockHttp;
import java.util.UUID;
import org.apache.flink.table.catalog.ObjectPath;
import org.junit.jupiter.api.Test;

/** Test suite for {@link FlinkUnityCatalog}. */
class FlinkUnityCatalogTest {

  private static final ObjectPath TABLE_PATH = new ObjectPath("default", "tbl");

  @Test
  void tableExistsReturnsTrueWithoutFetchingCredentials() {
    String tableId = UUID.randomUUID().toString();
    MockHttp.withMock(
        MockHttp.forExistingUCTable(tableId, "s3://bucket/default/tbl"),
        mock -> {
          assertTrue(catalog(mock).tableExists(TABLE_PATH));
          mock.verifyCredentialRequests(0);
        });
  }

  @Test
  void tableExistsReturnsFalseWhenTableIsMissing() {
    MockHttp.withMock(
        MockHttp.forMissingUCTable(), mock -> assertFalse(catalog(mock).tableExists(TABLE_PATH)));
  }

  @Test
  void tableExistsReturnsTrueWhenTableFormatIsUnsupported() {
    MockHttp.withMock(
        MockHttp.forUnsupportedUCTable(),
        mock -> assertTrue(catalog(mock).tableExists(TABLE_PATH)));
  }

  private static FlinkUnityCatalog catalog(MockHttp mock) {
    FlinkUnityCatalog catalog =
        new FlinkUnityCatalog("main", "default", mock.uri().toString(), "token");
    catalog.open();
    return catalog;
  }
}
