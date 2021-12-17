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
 * Wrapper class for part files in the {@link io.delta.flink.sink.DeltaSink}.
 * Part files are files that are currently "opened" for writing new data.
 * Similar behaviour might be observed in the {@link org.apache.flink.connector.file.sink.FileSink}
 * however as opposite to the FileSink, in DeltaSink we need to keep the name of the file
 * attached to the opened file in order to be further able to transform
 * {@link DeltaInProgressPart} instance into {@link DeltaPendingFile} instance and finally to commit
 * the written file to the {@link io.delta.standalone.DeltaLog} during global commit phase.
 * <p>
 * Additionally, we need a custom implementation of {@link DeltaBulkPartWriter} as a workaround
 * for getting actual file size (what is currently not possible for bulk formats when operating
 * on an interface level of {@link PartFileInfo}, see {@link DeltaBulkPartWriter} for details).
 * <p>
 * Lifecycle of instances of this class is as follows:
 * <ol>
 *     <li>Instances of this class are being created inside
 *         {@link io.delta.flink.sink.internal.writer.DeltaWriterBucket#rollPartFile}
 *         method every time a bucket processes the first event or if the previously opened file
 *         met conditions for rolling (e.g. size threshold)</li>
 *     <li>It's life span holds as long as the underlying file stays in an in-progress state (so
 *         until it's "rolled"), but no longer then single checkpoint interval.</li>
 *     <li>During pre-commit phase every existing {@link DeltaInProgressPart} instance is
 *         automatically transformed ("rolled") into a {@link DeltaPendingFile} instance</li>
 * </ol>
 *
 * @param <IN> The type of input elements.
 */
public class DeltaInProgressPart<IN> {

    private final String fileName;
    private final DeltaBulkPartWriter<IN, String> bulkPartWriter;

    public DeltaInProgressPart(String fileName,
                               DeltaBulkPartWriter<IN, String> bulkPartWriter) {
        this.fileName = fileName;
        this.bulkPartWriter = bulkPartWriter;
    }

    public String getFileName() {
        return fileName;
    }

    public DeltaBulkPartWriter<IN, String> getBulkPartWriter() {
        return bulkPartWriter;
    }
}
