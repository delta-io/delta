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
package io.delta.kernel.spark.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/** Tests for {@link UCCatalogConfig}. */
class UCCatalogConfigTest {

  @Test
  void testConstructor_ValidInputs_Success() {
    UCCatalogConfig config = new UCCatalogConfig("unity", "https://uc:8080", "token123");

    assertEquals("unity", config.getCatalogName());
    assertEquals("https://uc:8080", config.getUri());
    assertEquals("token123", config.getToken());
  }

  @Test
  void testConstructor_NullCatalogName_ThrowsException() {
    assertThrows(
        NullPointerException.class,
        () -> new UCCatalogConfig(null, "https://uc:8080", "token"),
        "Null catalogName should throw NullPointerException");
  }

  @Test
  void testConstructor_NullUri_ThrowsException() {
    assertThrows(
        NullPointerException.class,
        () -> new UCCatalogConfig("unity", null, "token"),
        "Null uri should throw NullPointerException");
  }

  @Test
  void testConstructor_NullToken_ThrowsException() {
    assertThrows(
        NullPointerException.class,
        () -> new UCCatalogConfig("unity", "https://uc:8080", null),
        "Null token should throw NullPointerException");
  }

  @Test
  void testToString_MasksToken() {
    UCCatalogConfig config = new UCCatalogConfig("unity", "https://uc:8080", "secret-token");
    String toString = config.toString();

    assertTrue(toString.contains("unity"), "toString should contain catalog name");
    assertTrue(toString.contains("https://uc:8080"), "toString should contain URI");
    assertTrue(toString.contains("***"), "toString should contain masked token placeholder");
    assertFalse(toString.contains("secret-token"), "toString should NOT contain actual token");
  }
}
