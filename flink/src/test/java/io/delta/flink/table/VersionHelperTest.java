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

package io.delta.flink.table;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Map;
import org.junit.jupiter.api.Test;

public class VersionHelperTest {

  @Test
  public void testAppVersion() {
    var result = VersionHelper.appVersions();

    Package kernelPkg = io.delta.kernel.Table.class.getPackage();
    String kernelVersion = kernelPkg.getImplementationVersion();
    Package ucPkg = io.unitycatalog.client.ApiClientBuilder.class.getPackage();
    String ucVersion = ucPkg.getImplementationVersion();
    Package flinkPkg = org.apache.flink.core.execution.JobClient.class.getPackage();
    String flinkVersion = flinkPkg.getImplementationVersion();
    assertEquals(Map.of("delta-kernel", kernelVersion, "flink", flinkVersion), result);
  }
}
