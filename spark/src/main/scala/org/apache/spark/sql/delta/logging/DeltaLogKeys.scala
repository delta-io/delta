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

import org.apache.spark.internal.LogKeyShims

/**
 * Various keys used for mapped diagnostic contexts(MDC) in logging. All structured logging keys
 * should be defined here for standardization.
 */
trait DeltaLogKeysBase {
  case object APP_ID extends LogKeyShims
  case object ATTEMPT extends LogKeyShims
  case object BATCH_ID extends LogKeyShims
  case object CLASS_NAME extends LogKeyShims
  case object COLUMN_NAME extends LogKeyShims
  case object COMPACTION_INFO_NEW extends LogKeyShims
  case object COMPACTION_INFO_OLD extends LogKeyShims
  case object CONFIG extends LogKeyShims
  case object COORDINATOR_CONF extends LogKeyShims
  case object COORDINATOR_NAME extends LogKeyShims
  case object COUNT extends LogKeyShims
  case object DATA_FILTER extends LogKeyShims
  case object DATE extends LogKeyShims
  case object DIR extends LogKeyShims
  case object DRY_RUN extends LogKeyShims
  case object DURATION extends LogKeyShims
  case object END_INDEX extends LogKeyShims
  case object END_OFFSET extends LogKeyShims
  case object END_VERSION extends LogKeyShims
  case object ERROR extends LogKeyShims
  case object EXECUTOR_ID extends LogKeyShims
  case object EXPR extends LogKeyShims
  case object FILE_ACTION_NEW extends LogKeyShims
  case object FILE_ACTION_OLD extends LogKeyShims
  case object FILE_INDEX extends LogKeyShims
  case object FILE_NAME extends LogKeyShims
  case object FILE_STATUS extends LogKeyShims
  case object FILE_SYSTEM_SCHEME extends LogKeyShims
  case object FILTER extends LogKeyShims
  case object HOOK_NAME extends LogKeyShims
  case object INDEX extends LogKeyShims
  case object ISOLATION_LEVEL extends LogKeyShims
  case object IS_INIT_SNAPSHOT extends LogKeyShims
  case object IS_PATH_TABLE extends LogKeyShims
  case object JOB_ID extends LogKeyShims
  case object MAX_SIZE extends LogKeyShims
  case object MESSAGE extends LogKeyShims
  case object METADATA_ID extends LogKeyShims
  case object METADATA_NEW extends LogKeyShims
  case object METADATA_OLD extends LogKeyShims
  case object METRICS extends LogKeyShims
  case object MIN_SIZE extends LogKeyShims
  case object NUM_ACTIONS extends LogKeyShims
  case object NUM_ACTIONS2 extends LogKeyShims
  case object NUM_BYTES extends LogKeyShims
  case object NUM_DIRS extends LogKeyShims
  case object NUM_FILES extends LogKeyShims
  case object NUM_FILES2 extends LogKeyShims
  case object NUM_PARTITIONS extends LogKeyShims
  case object NUM_PREDICATES extends LogKeyShims
  case object NUM_RECORDS extends LogKeyShims
  case object NUM_RECORDS_ACTUAL extends LogKeyShims
  case object NUM_RECORDS_EXPECTED extends LogKeyShims
  case object OFFSET extends LogKeyShims
  case object OPERATION extends LogKeyShims
  case object OPERATION2 extends LogKeyShims
  case object OP_NAME extends LogKeyShims
  case object OP_TYPE extends LogKeyShims
  case object PARTITION_FILTER extends LogKeyShims
  case object PATH extends LogKeyShims
  case object PATH2 extends LogKeyShims
  case object PATHS extends LogKeyShims
  case object PROTOCOL extends LogKeyShims
  case object QUERY_ID extends LogKeyShims
  case object RDD_ID extends LogKeyShims
  case object SCHEMA extends LogKeyShims
  case object SEGMENT extends LogKeyShims
  case object SIZE extends LogKeyShims
  case object SNAPSHOT extends LogKeyShims
  case object START_INDEX extends LogKeyShims
  case object START_VERSION extends LogKeyShims
  case object STATS extends LogKeyShims
  case object STATUS extends LogKeyShims
  case object TABLE_DESC extends LogKeyShims
  case object TABLE_FEATURES extends LogKeyShims
  case object TABLE_ID extends LogKeyShims
  case object TABLE_NAME extends LogKeyShims
  case object TAG extends LogKeyShims
  case object TBL_PROPERTIES extends LogKeyShims
  case object THREAD_NAME extends LogKeyShims
  case object TIMESTAMP extends LogKeyShims
  case object TIMESTAMP2 extends LogKeyShims
  case object TIME_MS extends LogKeyShims
  case object TIME_STATS extends LogKeyShims
  case object TXN_ID extends LogKeyShims
  case object URI extends LogKeyShims
  case object VACUUM_STATS extends LogKeyShims
  case object VERSION extends LogKeyShims
  case object VERSION2 extends LogKeyShims
}

object DeltaLogKeys extends DeltaLogKeysBase
