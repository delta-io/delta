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
package io.delta.kernel.utils;

import java.io.File;

public class DefaultKernelTestUtils
{
    private DefaultKernelTestUtils() {}

    public static String getTestResourceFilePath(String resourcePath) {
        return DefaultKernelTestUtils.class.getClassLoader().getResource(resourcePath).getFile();
    }

    public static String goldenTablePath(String goldenTable) {
        // TODO: this is a hack to avoid copying all golden tables from the connectors directory
        // The golden files needs to be a separate module which the kernel and connectors modules
        // can depend on.

        // Returns <repo-root>/kernel/kernel-default/target/test-classes/json-files
        String jsonFilesDirectory =
            DefaultKernelTestUtils.class.getClassLoader().getResource("json-files").getFile();

        // Need to get to <repo-root>/connectors/golden-tables/src/test/resources/golden

        // Get to repo root first.
        File repoRoot = new File(jsonFilesDirectory);
        for (int i = 0; i < 5; i++) {
            repoRoot = repoRoot.getParentFile();
        }

        File goldenTablesRoot =
            new File(repoRoot, "connectors/golden-tables/src/test/resources/golden");

        return new File(goldenTablesRoot, goldenTable).toString();
    }
}
