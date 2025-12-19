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

/*
 * This file contains code from the Apache Spark project (original license above).
 * It contains modifications, which are licensed as follows:
 */

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

package org.apache.spark.sql.delta.logging

// DeltaLogKey is provided by LogKeyShims (see scala-shims/<SparkShortVersion>/LogKeyShims.scala)
// to handle the difference between Spark versions:
// - Spark 4.0: LogKey is a Scala trait with a default `name` implementation
// - Spark 4.1: LogKey is a Java interface requiring explicit `name()` implementation

/**
 * Various keys used for mapped diagnostic contexts(MDC) in logging. All structured logging keys
 * should be defined here for standardization.
 */
trait DeltaLogKeysBase {
  case object APP_ID extends DeltaLogKey
  case object ATTEMPT extends DeltaLogKey
  case object BATCH_ID extends DeltaLogKey
  case object BATCH_SIZE extends DeltaLogKey
  case object CATALOG extends DeltaLogKey
  case object CLONE_SOURCE_DESC extends DeltaLogKey
  case object CONFIG extends DeltaLogKey
  case object CONFIG_KEY extends DeltaLogKey
  case object COORDINATOR_CONF extends DeltaLogKey
  case object COORDINATOR_NAME extends DeltaLogKey
  case object COUNT extends DeltaLogKey
  case object DATA_FILTER extends DeltaLogKey
  case object DATE extends DeltaLogKey
  case object DELTA_COMMIT_INFO extends DeltaLogKey
  case object DELTA_METADATA extends DeltaLogKey
  case object DIR extends DeltaLogKey
  case object DURATION extends DeltaLogKey
  case object ERROR_ID extends DeltaLogKey
  case object END_INDEX extends DeltaLogKey
  case object END_OFFSET extends DeltaLogKey
  case object END_VERSION extends DeltaLogKey
  case object ERROR extends DeltaLogKey
  case object EXCEPTION extends DeltaLogKey
  case object EXECUTOR_ID extends DeltaLogKey
  case object EXPR extends DeltaLogKey
  case object FILE_INDEX extends DeltaLogKey
  case object FILE_NAME extends DeltaLogKey
  case object FILE_STATUS extends DeltaLogKey
  case object FILE_SYSTEM_SCHEME extends DeltaLogKey
  case object FILTER extends DeltaLogKey
  case object FILTER2 extends DeltaLogKey
  case object HOOK_NAME extends DeltaLogKey
  case object INVARIANT_CHECK_INFO extends DeltaLogKey
  case object ISOLATION_LEVEL extends DeltaLogKey
  case object IS_DRY_RUN extends DeltaLogKey
  case object IS_INIT_SNAPSHOT extends DeltaLogKey
  case object IS_PATH_TABLE extends DeltaLogKey
  case object JOB_ID extends DeltaLogKey
  case object LOG_SEGMENT extends DeltaLogKey
  case object MAX_SIZE extends DeltaLogKey
  case object METADATA_ID extends DeltaLogKey
  case object METADATA_NEW extends DeltaLogKey
  case object METADATA_OLD extends DeltaLogKey
  case object METRICS extends DeltaLogKey
  case object METRIC_NAME extends DeltaLogKey
  case object MIN_SIZE extends DeltaLogKey
  case object NUM_ACTIONS extends DeltaLogKey
  case object NUM_ACTIONS2 extends DeltaLogKey
  case object NUM_ATTEMPT extends DeltaLogKey
  case object NUM_BYTES extends DeltaLogKey
  case object NUM_DIRS extends DeltaLogKey
  case object NUM_FILES extends DeltaLogKey
  case object NUM_FILES2 extends DeltaLogKey
  case object NUM_PARTITIONS extends DeltaLogKey
  case object NUM_PREDICATES extends DeltaLogKey
  case object NUM_RECORDS extends DeltaLogKey
  case object NUM_RECORDS2 extends DeltaLogKey
  case object NUM_SKIPPED extends DeltaLogKey
  case object OFFSET extends DeltaLogKey
  case object OPERATION extends DeltaLogKey
  case object OP_NAME extends DeltaLogKey
  case object PARTITION_FILTER extends DeltaLogKey
  case object PATH extends DeltaLogKey
  case object PATH2 extends DeltaLogKey
  case object PATHS extends DeltaLogKey
  case object PATHS2 extends DeltaLogKey
  case object PATHS3 extends DeltaLogKey
  case object PATHS4 extends DeltaLogKey
  case object PROTOCOL extends DeltaLogKey
  case object QUERY_ID extends DeltaLogKey
  case object SCHEMA extends DeltaLogKey
  case object SCHEMA_DIFF extends DeltaLogKey
  case object SNAPSHOT extends DeltaLogKey
  case object START_INDEX extends DeltaLogKey
  case object START_VERSION extends DeltaLogKey
  case object STATS extends DeltaLogKey
  case object STATUS extends DeltaLogKey
  case object STATUS_MESSAGE extends DeltaLogKey
  case object SYSTEM_CLASS_NAME extends DeltaLogKey
  case object TABLE_FEATURES extends DeltaLogKey
  case object TABLE_ID extends DeltaLogKey
  case object TABLE_NAME extends DeltaLogKey
  case object TBL_PROPERTIES extends DeltaLogKey
  case object THREAD_NAME extends DeltaLogKey
  case object TIMESTAMP extends DeltaLogKey
  case object TIMESTAMP2 extends DeltaLogKey
  case object TIME_MS extends DeltaLogKey
  case object TIME_STATS extends DeltaLogKey
  case object TXN_ID extends DeltaLogKey
  case object URI extends DeltaLogKey
  case object VACUUM_STATS extends DeltaLogKey
  case object VERSION extends DeltaLogKey
  case object VERSION2 extends DeltaLogKey
}

object DeltaLogKeys extends DeltaLogKeysBase
