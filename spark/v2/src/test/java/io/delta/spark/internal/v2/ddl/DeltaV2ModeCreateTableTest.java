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
package io.delta.spark.internal.v2.ddl;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Map;
import org.apache.spark.sql.delta.DeltaV2Mode;
import org.apache.spark.sql.delta.sources.DeltaSQLConf$;
import org.apache.spark.sql.internal.SQLConf;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link DeltaV2Mode#shouldUseKernelForCreateTable}. Only STRICT routes through Kernel;
 * AUTO and NONE always use the V1 path until V2 snapshot publication reaches parity.
 */
public class DeltaV2ModeCreateTableTest {

  @Test
  public void testStrict_alwaysTrue() {
    DeltaV2Mode strict = mode("STRICT");
    assertTrue(strict.shouldUseKernelForCreateTable(false, Map.of()));
    assertTrue(
        strict.shouldUseKernelForCreateTable(
            true, Map.of("delta.feature.catalogManaged", "supported")));
  }

  @Test
  public void testNone_alwaysFalse() {
    DeltaV2Mode none = mode("NONE");
    assertFalse(none.shouldUseKernelForCreateTable(false, Map.of()));
    assertFalse(
        none.shouldUseKernelForCreateTable(
            true, Map.of("delta.feature.catalogManaged", "supported")));
  }

  @Test
  public void testAuto_requiresUCAndCatalogManaged() {
    DeltaV2Mode auto = mode("AUTO");
    assertTrue(
        auto.shouldUseKernelForCreateTable(
            true, Map.of("delta.feature.catalogManaged", "supported")));
    assertTrue(
        auto.shouldUseKernelForCreateTable(
            true, Map.of("delta.feature.catalogOwned-preview", "supported")));
  }

  @Test
  public void testAuto_nonUC_alwaysFalse() {
    DeltaV2Mode auto = mode("AUTO");
    assertFalse(auto.shouldUseKernelForCreateTable(false, Map.of()));
    assertFalse(
        auto.shouldUseKernelForCreateTable(
            false, Map.of("delta.feature.catalogManaged", "supported")));
  }

  @Test
  public void testAuto_UC_withoutCatalogManaged_false() {
    DeltaV2Mode auto = mode("AUTO");
    assertFalse(auto.shouldUseKernelForCreateTable(true, Map.of()));
    assertFalse(auto.shouldUseKernelForCreateTable(true, Map.of("other", "value")));
  }

  @Test
  public void testAuto_nullProperties_false() {
    DeltaV2Mode auto = mode("AUTO");
    assertFalse(auto.shouldUseKernelForCreateTable(true, null));
  }

  private static DeltaV2Mode mode(String modeValue) {
    SQLConf conf = new SQLConf();
    conf.setConfString(DeltaSQLConf$.MODULE$.V2_ENABLE_MODE().key(), modeValue);
    return new DeltaV2Mode(conf);
  }
}
