/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.functions.sink.filesystem;

/**
 * Wrapper class for part files in the {@link org.apache.flink.connector.delta.sink.DeltaSink}.
 * Part files are files that are currently "opened" for writing new data.
 * Similar behaviour might be observed in the {@link org.apache.flink.connector.file.sink.FileSink}
 * however as opposite to the FileSink, in DeltaSink we need to keep the name of the file
 * attached to the opened file in order to be further able to transform
 * {@link DeltaInProgressPart} instance into {@link DeltaPendingFile} instance and finally to commit
 * the written file to the {@link io.delta.standalone.DeltaLog} during global commit phase.
 *
 * @param <IN> The type of input elements.
 */
public class DeltaInProgressPart<IN> {

    public DeltaInProgressPart() {}
}
