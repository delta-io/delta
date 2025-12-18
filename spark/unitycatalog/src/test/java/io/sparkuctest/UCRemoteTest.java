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

package io.sparkuctest;

import com.google.common.collect.ImmutableMap;
import java.lang.reflect.Field;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class UCRemoteTest extends UnityCatalogSupport {

  private static final Map<String, String> ENV_VARS =
      ImmutableMap.of(
          UC_REMOTE, "true",
          UC_URI, "http://localhost:8080",
          UC_TOKEN, "TestRemoteToken",
          UC_CATALOG_NAME, "TestRemoteCatalog",
          UC_SCHEMA_NAME, "TestRemoteSchema",
          UC_BASE_TABLE_LOCATION, "s3://test-bucket/key");

  @BeforeAll
  public static void beforeAll() throws Exception {
    for (Map.Entry<String, String> e : ENV_VARS.entrySet()) {
      setEnv(e.getKey(), e.getValue());
    }
  }

  @AfterAll
  public static void afterAll() throws Exception {
    for (Map.Entry<String, String> e : ENV_VARS.entrySet()) {
      removeEnv(e.getKey());
    }
  }

  @Test
  public void testUnityCatalogInfo() {
    UnityCatalogInfo catalogInfo = unityCatalogInfo();
    Assertions.assertEquals("TestRemoteCatalog", catalogInfo.catalogName());
    Assertions.assertEquals("http://localhost:8080", catalogInfo.serverUri());
    Assertions.assertEquals("TestRemoteCatalog", catalogInfo.catalogName());
    Assertions.assertEquals("TestRemoteToken", catalogInfo.serverToken());
    Assertions.assertEquals("TestRemoteSchema", catalogInfo.schemaName());
    Assertions.assertEquals("s3://test-bucket/key", catalogInfo.baseTableLocation());
  }

  @SuppressWarnings("unchecked")
  private static void setEnv(String key, String value) throws Exception {
    Map<String, String> env = System.getenv();
    Field f = env.getClass().getDeclaredField("m");
    f.setAccessible(true);
    ((Map<String, String>) f.get(env)).put(key, value);
  }

  @SuppressWarnings("unchecked")
  private static void removeEnv(String key) throws Exception {
    Map<String, String> env = System.getenv();
    Field field = env.getClass().getDeclaredField("m");
    field.setAccessible(true);
    ((Map<String, String>) field.get(env)).remove(key);
  }
}
