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

import java.util.Map;
import java.util.Objects;

/** Collect related library versions that needs to be reported */
public class VersionHelper {

  public static Map<String, String> appVersions() {
    Package flinkPkg = org.apache.flink.core.execution.JobClient.class.getPackage();
    String flinkVersion = flinkPkg.getImplementationVersion();
    Package kernelPkg = io.delta.kernel.Table.class.getPackage();
    String kernelVersion = kernelPkg.getImplementationVersion();
    return Map.of(
        "flink", Objects.toString(flinkVersion, "unknown"),
        "delta-kernel", Objects.toString(kernelVersion, "unknown"));
  }
}
