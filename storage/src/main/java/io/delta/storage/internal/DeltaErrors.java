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


package io.delta.storage.internal;

import java.io.IOException;

public class DeltaErrors {
    public static IOException incorrectLogStoreImplementationException(Throwable cause) {
        return new IOException(
            "The error typically occurs when the default LogStore implementation, that\n" +
            "is, HDFSLogStore, is used to write into a Delta table on a non-HDFS storage system.\n" +
            "In order to get the transactional ACID guarantees on table updates, you have to use the\n" +
            "correct implementation of LogStore that is appropriate for your storage system.\n" +
            "See https://docs.delta.io/latest/delta-storage.html for details.",
            cause
        );
    }
}
