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

package org.apache.spark.sql.delta;

import java.util.Arrays;
import java.util.HashSet;
import org.apache.spark.internal.config.ConfigBuilder;
import org.apache.spark.internal.config.ConfigEntry;
import scala.jdk.javaapi.CollectionConverters;

/**
 * Configuration for enabling DataSourceV2 mode in Delta Lake.
 *
 * <p>This is a standalone configuration class that does not depend on DeltaSQLConf, allowing for
 * independent evolution of the DSv2 feature.
 *
 * <p>Note: This class uses {@code package org.apache.spark.sql.delta} (not {@code
 * io.delta.kernel.spark}) to access Spark's internal config API ({@link
 * org.apache.spark.internal.config.ConfigBuilder}), which is only accessible from {@code
 * org.apache.spark.*} packages.
 */
public class DeltaDsv2EnableConf {

  private static final String SQL_CONF_PREFIX = "spark.databricks.delta";

  /**
   * Configuration for enabling DataSourceV2 mode in Delta.
   *
   * <p>Valid values:
   *
   * <ul>
   *   <li>"NONE": DataSourceV2 is disabled, always use V1 (DeltaTableV2)
   *   <li>"STRICT": DataSourceV2 is strictly enforced, always use V2 (Kernel SparkTable)
   *   <li>"AUTO": Automatically determine based on query (default)
   * </ul>
   *
   * <p>Default value: "AUTO"
   */
  public static final ConfigEntry<String> DATASOURCEV2_ENABLE_MODE =
      new ConfigBuilder(SQL_CONF_PREFIX + ".datasourcev2.enableMode")
          .doc(
              "Controls the DataSourceV2 enable mode. "
                  + "Valid values: NONE (disabled), STRICT (always enabled), AUTO (automatic determination).")
          .stringConf()
          .checkValues(
              CollectionConverters.asScala(new HashSet<>(Arrays.asList("NONE", "STRICT", "AUTO")))
                  .toSet())
          .createWithDefault("AUTO");
}
