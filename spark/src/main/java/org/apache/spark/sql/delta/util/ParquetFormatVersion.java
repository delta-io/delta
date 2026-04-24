/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.delta.util;

import org.apache.parquet.SemanticVersion;
import org.apache.parquet.SemanticVersion.SemanticVersionParseException;
import org.apache.parquet.column.ParquetProperties.WriterVersion;

import org.apache.hadoop.conf.Configuration;

/**
 * Supported Parquet format versions for Delta tables. Each value maps to a version listed in
 * https://parquet.apache.org/blog/parquet-format/.
 */
public enum ParquetFormatVersion {
  // Must be in ascending SemanticVersion sorted order as resolve() assumes values() is sorted.
  V1_0_0(new SemanticVersion(1, 0, 0)),
  V2_12_0(new SemanticVersion(2, 12, 0));

  private final SemanticVersion semver;

  ParquetFormatVersion(SemanticVersion semver) {
    this.semver = semver;
  }

  public String getVersion() {
    return this.semver.toString();
  }

  public WriterVersion getWriterVersion() {
    if (this == V1_0_0) return WriterVersion.PARQUET_1_0;
    if (this == V2_12_0) return WriterVersion.PARQUET_2_0;
    throw new IllegalArgumentException("Unsupported Parquet format version: " + this);
  }

  public int compare(ParquetFormatVersion other) {
    return this.semver.compareTo(other.semver);
  }

  /**
   * Resolves a version string to the highest known ParquetFormatVersion whose version is <= the
   * input. Exact matches on known versions use a fast path. Arbitrary 3-part semver strings are
   * accepted and resolved to the closest known version. Throws if the input is not a valid 3-part
   * semver or if no known version is <= the input.
   */
  public static ParquetFormatVersion resolve(String s) {
    SemanticVersion target;
    try {
      target = SemanticVersion.parse(s);
    } catch (SemanticVersionParseException e) {
      throw new IllegalArgumentException("Invalid Parquet version string: " + s, e);
    }

    // Find the highest supported version that is <= the target.
    ParquetFormatVersion[] versions = values();
    for (int i = versions.length - 1; i >= 0; i--) {
      if (versions[i].semver.compareTo(target) <= 0) return versions[i];
    }
    throw new IllegalArgumentException("No matching Parquet format version <= " + s);
  }
}
