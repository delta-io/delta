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

package org.apache.spark.sql.connector.write.file

import org.apache.spark.sql.execution.metric.SQLMetric

/**
 * Summary of what a [[FileOutputWriter.write]] call committed. Carries connector-agnostic
 * file/row counts plus a metric bag the connector can use to surface engine-side metrics
 * (e.g. transaction version, bytes written, optimize counters) on the V2 write exec node.
 */
case class WriteResult(
    addedFiles: Long,
    removedFiles: Long,
    outputRows: Long,
    metrics: Map[String, SQLMetric] = Map.empty)
