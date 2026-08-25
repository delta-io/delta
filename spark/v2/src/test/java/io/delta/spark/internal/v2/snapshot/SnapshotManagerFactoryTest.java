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
package io.delta.spark.internal.v2.snapshot;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import io.delta.kernel.Meta;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link SnapshotManagerFactory}. */
public class SnapshotManagerFactoryTest {

  @Test
  public void connectorAppVersions_batch_advertisesKernelAndConnectorOnly() {
    Map<String, String> appVersions =
        SnapshotManagerFactory.connectorAppVersions(WorkloadType.BATCH);

    assertEquals(Meta.KERNEL_VERSION, appVersions.get("appVersions.Kernel"));
    assertEquals("true", appVersions.get("appVersions.Delta V2 connector"));
    // A batch workload carries no streaming marker.
    assertFalse(appVersions.containsKey("appVersions.Streaming"));
  }

  @Test
  public void connectorAppVersions_streaming_addsStreamingMarker() {
    Map<String, String> appVersions =
        SnapshotManagerFactory.connectorAppVersions(WorkloadType.STREAMING);

    assertEquals(Meta.KERNEL_VERSION, appVersions.get("appVersions.Kernel"));
    assertEquals("true", appVersions.get("appVersions.Delta V2 connector"));
    assertEquals("true", appVersions.get("appVersions.Streaming"));
  }
}
