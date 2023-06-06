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

public class LogStoreErrors {

    /**
     * Returns true if the provided Throwable is to be considered non-fatal, or false if it is to be
     * considered fatal
     */
    public static boolean isNonFatal(Throwable t) {
        // VirtualMachineError includes OutOfMemoryError and other fatal errors
        if (t instanceof VirtualMachineError ||
            t instanceof ThreadDeath ||
            t instanceof InterruptedException ||
            t instanceof LinkageError) {
            return false;
        }

        return true;
    }

    public static IOException incorrectLogStoreImplementationException(Throwable cause) {
        return new IOException(
            String.join("\n",
                "The error typically occurs when the default LogStore implementation, that",
                "is, HDFSLogStore, is used to write into a Delta table on a non-HDFS storage system.",
                "In order to get the transactional ACID guarantees on table updates, you have to use the",
                "correct implementation of LogStore that is appropriate for your storage system.",
                "See https://docs.delta.io/latest/delta-storage.html for details."
            ),
            cause
        );
    }
}
