/*
 * Copyright (2021) The Delta Lake Project Authors.
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

package io.delta.flink.table;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;

/**
 * Per-table configuration for DeltaSink table maintenance behavior.
 *
 * <p>This class parses a raw string map (typically table options) into typed Flink {@link
 * ConfigOption} values and exposes:
 *
 * <ul>
 *   <li><b>Catalog config</b>: options that should be persisted with the table definition.
 *   <li><b>Engine config</b>: options that should be forwarded to the Delta Kernel engine at
 *       runtime.
 * </ul>
 */
public class TableConf implements Serializable {

  private static final long serialVersionUID = 1L;

  /** Probability in [0.0, 1.0] to create a checkpoint on a commit. */
  public static final ConfigOption<Double> CHECKPOINT_FREQUENCY =
      ConfigOptions.key("checkpoint.frequency")
          .doubleType()
          .defaultValue(0.0)
          .withDescription(
              "Probability in [0.0, 1.0] to create a checkpoint on a commit. "
                  + "0.0 disables checkpoint creation; 1.0 creates a checkpoint on every commit.");

  /** Whether checksum file creation is enabled for this table. */
  public static final ConfigOption<Boolean> CHECKSUM_ENABLED =
      ConfigOptions.key("checksum.enable")
          .booleanType()
          .defaultValue(false)
          .withDescription("Whether to generate checksum files for commits on this table.");

  private final Map<String, String> raw;
  private final Configuration cfg;

  private final Random randgen = new Random(System.currentTimeMillis());

  /**
   * Creates a {@link TableConf} from a raw key/value map (e.g., table options).
   *
   * <p>Unknown keys are preserved in {@link #raw} but ignored by typed accessors unless explicitly
   * surfaced via {@link #catalogConf()} or {@link #engineConf()}.
   *
   * @param conf raw configuration map; must not be null
   */
  public TableConf(Map<String, String> conf) {
    this.raw = Map.copyOf(Objects.requireNonNull(conf, "conf"));
    this.cfg = Configuration.fromMap(this.raw);

    validate();
  }

  /**
   * Configuration to be persisted in the catalog.
   *
   * <p>This returns a subset of options that are intended to be stored with the table definition so
   * that behavior is consistent across jobs and clusters.
   *
   * @return a map of catalog-persisted configuration entries
   */
  public Map<String, String> catalogConf() {
    return Map.of();
  }

  /**
   * Configuration to be forwarded to the Kernel engine.
   *
   * <p>This returns the subset of configuration entries that are relevant to engine-side behavior.
   * If your engine uses different option names, translate them here.
   *
   * @return a map of engine configuration entries
   */
  public Map<String, String> engineConf() {
    return Map.of();
  }

  /** @return whether checksum file creation is enabled for this table */
  public boolean isChecksumEnabled() {
    return cfg.get(CHECKSUM_ENABLED);
  }

  /**
   * Returns the checkpoint creation frequency as a probability.
   *
   * @return probability in [0.0, 1.0]
   */
  public double getCheckpointFrequency() {
    return cfg.get(CHECKPOINT_FREQUENCY);
  }

  /**
   * Returns whether a checkpoint should be created for the current commit attempt.
   *
   * <p>The decision is made by sampling a uniform random number in [0.0, 1.0) and comparing it to
   * {@link #getCheckpointFrequency()}.
   *
   * @return {@code true} if a random number is smaller than the configured frequency
   */
  public boolean shouldCreateCheckpoint() {
    double p = getCheckpointFrequency();
    if (p <= 0.0) return false;
    if (p >= 1.0) return true;
    return randgen.nextDouble() < p;
  }

  private void validate() {
    double p = cfg.get(CHECKPOINT_FREQUENCY);
    if (Double.isNaN(p) || p < 0.0 || p > 1.0) {
      throw new IllegalArgumentException(
          "Invalid checkpoint-frequency: " + p + " (expected a probability in [0.0, 1.0])");
    }
  }
}
