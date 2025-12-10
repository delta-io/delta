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

import org.apache.spark.internal.LogKey

/**
 * Various keys used for mapped diagnostic contexts(MDC) in logging. All structured logging keys
 * should be defined here for standardization.
 */
trait DeltaLogKeysBase {
  case object APP_ID extends LogKey
  case object ATTEMPT extends LogKey
  case object BATCH_ID extends LogKey
  case object BATCH_SIZE extends LogKey
  case object CATALOG extends LogKey
  case object CLONE_SOURCE_DESC extends LogKey
  case object CONFIG extends LogKey
  case object CONFIG_KEY extends LogKey
  case object COORDINATOR_CONF extends LogKey
  case object COORDINATOR_NAME extends LogKey
  case object COUNT extends LogKey
  case object DATA_FILTER extends LogKey
  case object DATE extends LogKey
  case object DELTA_COMMIT_INFO extends LogKey
  case object DELTA_METADATA extends LogKey
  case object DIR extends LogKey
  case object DURATION extends LogKey
  case object ERROR_ID extends LogKey
  case object END_INDEX extends LogKey
  case object END_OFFSET extends LogKey
  case object END_VERSION extends LogKey
  case object ERROR extends LogKey
  case object EXCEPTION extends LogKey
  case object EXECUTOR_ID extends LogKey
  case object EXPR extends LogKey
  case object FILE_INDEX extends LogKey
  case object FILE_NAME extends LogKey
  case object FILE_STATUS extends LogKey
  case object FILE_SYSTEM_SCHEME extends LogKey
  case object FILTER extends LogKey
  case object FILTER2 extends LogKey
  case object HOOK_NAME extends LogKey
  case object INVARIANT_CHECK_INFO extends LogKey
  case object ISOLATION_LEVEL extends LogKey
  case object IS_DRY_RUN extends LogKey
  case object IS_INIT_SNAPSHOT extends LogKey
  case object IS_PATH_TABLE extends LogKey
  case object JOB_ID extends LogKey
  case object LOG_SEGMENT extends LogKey
  case object MAX_SIZE extends LogKey
  case object METADATA_ID extends LogKey
  case object METADATA_NEW extends LogKey
  case object METADATA_OLD extends LogKey
  case object METRICS extends LogKey
  case object METRIC_NAME extends LogKey
  case object MIN_SIZE extends LogKey
  case object NUM_ACTIONS extends LogKey
  case object NUM_ACTIONS2 extends LogKey
  case object NUM_ATTEMPT extends LogKey
  case object NUM_BYTES extends LogKey
  case object NUM_DIRS extends LogKey
  case object NUM_FILES extends LogKey
  case object NUM_FILES2 extends LogKey
  case object NUM_PARTITIONS extends LogKey
  case object NUM_PREDICATES extends LogKey
  case object NUM_RECORDS extends LogKey
  case object NUM_RECORDS2 extends LogKey
  case object NUM_SKIPPED extends LogKey
  case object OFFSET extends LogKey
  case object OPERATION extends LogKey
  case object OP_NAME extends LogKey
  case object PARTITION_FILTER extends LogKey
  case object PATH extends LogKey
  case object PATH2 extends LogKey
  case object PATHS extends LogKey
  case object PATHS2 extends LogKey
  case object PATHS3 extends LogKey
  case object PATHS4 extends LogKey
  case object PROTOCOL extends LogKey
  case object QUERY_ID extends LogKey
  case object SCHEMA extends LogKey
  case object SCHEMA_DIFF extends LogKey
  case object SNAPSHOT extends LogKey
  case object START_INDEX extends LogKey
  case object START_VERSION extends LogKey
  case object STATS extends LogKey
  case object STATUS extends LogKey
  case object STATUS_MESSAGE extends LogKey
  case object SYSTEM_CLASS_NAME extends LogKey
  case object TABLE_FEATURES extends LogKey
  case object TABLE_ID extends LogKey
  case object TABLE_NAME extends LogKey
  case object TBL_PROPERTIES extends LogKey
  case object THREAD_NAME extends LogKey
  case object TIMESTAMP extends LogKey
  case object TIMESTAMP2 extends LogKey
  case object TIME_MS extends LogKey
  case object TIME_STATS extends LogKey
  case object TXN_ID extends LogKey
  case object URI extends LogKey
  case object VACUUM_STATS extends LogKey
  case object VERSION extends LogKey
  case object VERSION2 extends LogKey
}

object DeltaLogKeys extends DeltaLogKeysBase
