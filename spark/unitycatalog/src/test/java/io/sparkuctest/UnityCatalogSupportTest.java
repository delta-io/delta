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

import static io.sparkuctest.UnityCatalogSupport.UC_BASE_TABLE_LOCATION;
import static io.sparkuctest.UnityCatalogSupport.UC_CATALOG_NAME;
import static io.sparkuctest.UnityCatalogSupport.UC_REMOTE;
import static io.sparkuctest.UnityCatalogSupport.UC_SCHEMA_NAME;
import static io.sparkuctest.UnityCatalogSupport.UC_TOKEN;
import static io.sparkuctest.UnityCatalogSupport.UC_URI;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.sparkuctest.UnityCatalogSupport.UnityCatalogInfo;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class UnityCatalogSupportTest {

  private static final List<String> ALL_ENVS =
      ImmutableList.of(
          UC_REMOTE, UC_URI, UC_TOKEN, UC_CATALOG_NAME, UC_SCHEMA_NAME, UC_BASE_TABLE_LOCATION);

  @Test
  public void testUnityCatalogInfo() throws Exception {
    withEnvTesting(
        ImmutableMap.of(
            UC_REMOTE,
            "true",
            UC_URI,
            "http://localhost:8080",
            UC_TOKEN,
            "TestRemoteToken",
            UC_CATALOG_NAME,
            "TestRemoteCatalog",
            UC_SCHEMA_NAME,
            "TestRemoteSchema",
            UC_BASE_TABLE_LOCATION,
            "s3://test-bucket/key"),
        () -> {
          TestingUCSupport ucSupport = new TestingUCSupport();
          UnityCatalogInfo uc = ucSupport.accessUnityCatalogInfo();
          assertThat(uc.catalogName()).isEqualTo("TestRemoteCatalog");
          assertThat(uc.serverUri()).isEqualTo("http://localhost:8080");
          assertThat(uc.serverToken()).isEqualTo("TestRemoteToken");
          assertThat(uc.schemaName()).isEqualTo("TestRemoteSchema");
          assertThat(uc.baseTableLocation()).isEqualTo("s3://test-bucket/key");
        });
  }

  @Test
  public void testNoUri() throws Exception {
    withEnvTesting(
        ImmutableMap.of(
            UC_REMOTE,
            "true",
            UC_TOKEN,
            "TestRemoteToken",
            UC_CATALOG_NAME,
            "TestRemoteCatalog",
            UC_SCHEMA_NAME,
            "TestRemoteSchema",
            UC_BASE_TABLE_LOCATION,
            "s3://test-bucket/key"),
        () -> {
          TestingUCSupport uc = new TestingUCSupport();
          assertThatThrownBy(uc::accessUnityCatalogInfo)
              .isInstanceOf(NullPointerException.class)
              .hasMessageContaining("UC_URI must be set when UC_REMOTE=true");
        });
  }

  @Test
  public void testNoCatalogName() throws Exception {
    withEnvTesting(
        ImmutableMap.of(
            UC_REMOTE,
            "true",
            UC_URI,
            "http://localhost:8080",
            UC_TOKEN,
            "TestRemoteToken",
            UC_SCHEMA_NAME,
            "TestRemoteSchema",
            UC_BASE_TABLE_LOCATION,
            "s3://test-bucket/key"),
        () -> {
          TestingUCSupport uc = new TestingUCSupport();
          assertThatThrownBy(uc::accessUnityCatalogInfo)
              .isInstanceOf(NullPointerException.class)
              .hasMessageContaining("UC_CATALOG_NAME must be set when UC_REMOTE=true");
        });
  }

  @Test
  public void testNoToken() throws Exception {
    withEnvTesting(
        ImmutableMap.of(
            UC_REMOTE,
            "true",
            UC_URI,
            "http://localhost:8080",
            UC_CATALOG_NAME,
            "TestRemoteCatalog",
            UC_SCHEMA_NAME,
            "TestRemoteSchema",
            UC_BASE_TABLE_LOCATION,
            "s3://test-bucket/key"),
        () -> {
          TestingUCSupport uc = new TestingUCSupport();
          assertThatThrownBy(uc::accessUnityCatalogInfo)
              .isInstanceOf(NullPointerException.class)
              .hasMessageContaining("UC_TOKEN must be set when UC_REMOTE=true");
        });
  }

  @Test
  public void testNoSchemaName() throws Exception {
    withEnvTesting(
        ImmutableMap.of(
            UC_REMOTE,
            "true",
            UC_URI,
            "http://localhost:8080",
            UC_TOKEN,
            "TestRemoteToken",
            UC_CATALOG_NAME,
            "TestRemoteCatalog",
            UC_BASE_TABLE_LOCATION,
            "s3://test-bucket/key"),
        () -> {
          TestingUCSupport uc = new TestingUCSupport();
          assertThatThrownBy(uc::accessUnityCatalogInfo)
              .isInstanceOf(NullPointerException.class)
              .hasMessageContaining("UC_SCHEMA_NAME must be set when UC_REMOTE=true");
        });
  }

  @Test
  public void testNoBaseTableLocation() throws Exception {
    withEnvTesting(
        ImmutableMap.of(
            UC_REMOTE,
            "true",
            UC_URI,
            "http://localhost:8080",
            UC_TOKEN,
            "TestRemoteToken",
            UC_CATALOG_NAME,
            "TestRemoteCatalog",
            UC_SCHEMA_NAME,
            "TestRemoteSchema"),
        () -> {
          TestingUCSupport uc = new TestingUCSupport();
          assertThatThrownBy(uc::accessUnityCatalogInfo)
              .isInstanceOf(NullPointerException.class)
              .hasMessageContaining("UC_BASE_TABLE_LOCATION must be set when UC_REMOTE=true");
        });
  }

  public interface TestCall {

    void call() throws Exception;
  }

  public void withEnvTesting(Map<String, String> envs, TestCall testCall) throws Exception {
    // Clear all UC-related environment variables first to ensure clean state
    ALL_ENVS.forEach(UnityCatalogSupportTest::removeEnv);
    envs.forEach(UnityCatalogSupportTest::setEnv);
    try {
      testCall.call();
    } finally {
      // Clean up all UC-related environment variables after test
      ALL_ENVS.forEach(UnityCatalogSupportTest::removeEnv);
    }
  }

  @SuppressWarnings("unchecked")
  private static void setEnv(String key, String value) {
    try {
      Map<String, String> env = System.getenv();
      Field f = env.getClass().getDeclaredField("m");
      f.setAccessible(true);
      ((Map<String, String>) f.get(env)).put(key, value);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @SuppressWarnings("unchecked")
  private static void removeEnv(String key) {
    try {
      Map<String, String> env = System.getenv();
      Field field = env.getClass().getDeclaredField("m");
      field.setAccessible(true);
      ((Map<String, String>) field.get(env)).remove(key);
    } catch (NoSuchFieldException e) {
      // Ignore if field doesn't exist (different JVM implementation)
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static class TestingUCSupport extends UnityCatalogSupport {

    public UnityCatalogInfo accessUnityCatalogInfo() throws Exception {
      setupServer();
      return unityCatalogInfo();
    }
  }
}
