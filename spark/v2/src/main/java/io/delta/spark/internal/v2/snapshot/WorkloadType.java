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

/**
 * The kind of Spark workload a snapshot manager (and, for catalog-managed tables, the Unity Catalog
 * client it builds) serves. It is threaded into {@link SnapshotManagerFactory} so the catalog
 * client can advertise the workload in its User-Agent, letting the catalog distinguish a Structured
 * Streaming read/write from a batch one.
 */
public enum WorkloadType {
  /** A batch read or write. */
  BATCH,
  /** A Structured Streaming micro-batch read or write. */
  STREAMING
}
