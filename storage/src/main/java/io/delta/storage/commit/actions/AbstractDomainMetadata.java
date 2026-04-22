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

package io.delta.storage.commit.actions;

/**
 * Storage-level view of a Delta {@code DomainMetadata} action. Used by
 * {@code UCDeltaClient.createTable} and the DRC commit path to pass per-domain configuration
 * (e.g. {@code delta.clustering}, {@code delta.rowTracking}) as first-class structured
 * parameters rather than flattening into the properties map.
 *
 * <p>Mirrors the Spark-side {@code org.apache.spark.sql.delta.actions.DomainMetadata} case
 * class, but lives in storage so that non-Spark consumers (Kernel, Flink) and the Java DRC
 * client can manipulate domain metadata without pulling Spark in.
 */
public interface AbstractDomainMetadata {

  /** Unique domain name, e.g. {@code "delta.clustering"}. */
  String getDomain();

  /** Opaque per-domain configuration JSON. */
  String getConfiguration();

  /** {@code true} if this action removes the domain from the table. */
  boolean isRemoved();
}
