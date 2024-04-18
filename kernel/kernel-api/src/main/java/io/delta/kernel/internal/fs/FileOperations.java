/*
 * Copyright (2023) The Delta Lake Project Authors.
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
package io.delta.kernel.internal.fs;

import java.net.URI;

public class FileOperations {
    private FileOperations() {}

    /**
     * Relativize the given child path with respect to the given root URI. If the child path is
     * already a relative path, it is returned as is.
     *
     * @param child
     * @param root Root directory as URI. Relativization is done with respect to this root.
     *             The relativize operation requires conversion to URI, so the caller is expected to
     *             convert the root directory to URI once and use it for relativizing for multiple
     *             child paths.
     * @return
     */
    public static Path relativizePath(Path child, URI root) {
        if (child.isAbsolute()) {
            return new Path(root.relativize(child.toUri()));
        }
        return child;
    }
}
