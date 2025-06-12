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

package org.apache.spark.internal

/**
 * Guidelines for the Structured Logging Framework - Scala Logging
 * <p>
 *
 * Use the `org.apache.spark.internal.Logging` trait for logging in Scala code:
 * Logging Messages with Variables:
 *   When logging a message with variables, wrap all the variables with `MDC`s and they will be
 *   automatically added to the Mapped Diagnostic Context (MDC).
 * This allows for structured logging and better log analysis.
 * <p>
 *
 * logInfo(log"Trying to recover app: ${MDC(DeltaLogKeys.APP_ID, app.id)}")
 * <p>
 *
 * Constant String Messages:
 *   If you are logging a constant string message, use the log methods that accept a constant
 *   string.
 * <p>
 *
 * logInfo("StateStore stopped")
 * <p>
 *
 * Exceptions:
 *   To ensure logs are compatible with Spark SQL and log analysis tools, avoid
 *   `Exception.printStackTrace()`. Use `logError`, `logWarning`, and `logInfo` methods from
 *   the `Logging` trait to log exceptions, maintaining structured and parsable logs.
 * <p>
 *
 * If you want to output logs in `scala code` through the structured log framework,
 * you can define `custom LogKey` and use it in `scala` code as follows:
 * <p>
 *
 * // To add a `custom LogKey`, implement `LogKey`
 * case object CUSTOM_LOG_KEY extends LogKey
 * import org.apache.spark.internal.MDC;
 * logInfo(log"${MDC(CUSTOM_LOG_KEY, "key")}")
 */
trait LoggingShims extends Logging
