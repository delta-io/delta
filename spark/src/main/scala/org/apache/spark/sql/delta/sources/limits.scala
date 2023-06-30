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

package org.apache.spark.sql.delta.sources


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.streaming.{ReadLimit, ReadMaxFiles}
import org.apache.spark.sql.internal.SQLConf

/** A read limit that admits a soft-max of `maxBytes` per micro-batch. */
case class ReadMaxBytes(maxBytes: Long) extends ReadLimit

/**
 * A read limit that admits the given soft-max of `bytes` or max `maxFiles`, once `minFiles`
 * has been reached. Prior to that anything is admitted.
 */
case class CompositeLimit(
  bytes: ReadMaxBytes,
  maxFiles: ReadMaxFiles,
  minFiles: ReadMinFiles = ReadMinFiles(-1)) extends ReadLimit


/** A read limit that admits a min of `minFiles` per micro-batch. */
case class ReadMinFiles(minFiles: Int) extends ReadLimit
