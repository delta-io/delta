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
 * Wrapper class for {@link InProgressFileWriter.PendingFileRecoverable} object.
 * This class carries the internal committable information to be used during the checkpoint/commit
 * phase.
 * <p>
 * As similar to {@link org.apache.flink.connector.file.sink.FileSink} we need to carry
 * {@link InProgressFileWriter.PendingFileRecoverable} information to perform "local" commit
 * on file that the sink has written data to. However, as opposite to mentioned FileSink,
 * in DeltaSink we need to perform also "global" commit to the {@link io.delta.standalone.DeltaLog}
 * and for that additional file metadata must be provided. Hence, this class provides the required
 * information for both types of commits by wrapping pending file and attaching file's metadata.
 */
public class DeltaPendingFile {

    public DeltaPendingFile() {}
}
