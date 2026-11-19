/*
 * Copyright (2026) The Delta Lake Project Authors.
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

package io.delta.storage;

import java.io.IOException;

/**
 * Raised when an S3 write may have committed and recovery cannot establish its outcome.
 * Check whether the write committed before retrying the transaction.
 */
public final class S3UnknownWriteOutcomeException extends IOException {
    private static final long serialVersionUID = 1L;

    public S3UnknownWriteOutcomeException(String message) {
        super(message);
    }
}
