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
package io.delta.kernel.defaults.internal.coordinatedcommits;

import java.util.HashMap;
import java.util.Map;

import io.delta.storage.commit.CommitCoordinatorClient;

/** Factory to get the correct {@link CommitCoordinatorClient} for a table */
public class CommitCoordinatorProvider {
    // mapping from different commit-coordinator names to the corresponding
    // {@link CommitCoordinatorBuilder}s.
    private static final Map<String, CommitCoordinatorBuilder> nameToBuilderMapping =
            new HashMap<>();

    /**
     * Registers a new {@link CommitCoordinatorBuilder} with the {@link CommitCoordinatorProvider}.
     */
    public static synchronized void registerBuilder(
            CommitCoordinatorBuilder commitCoordinatorBuilder) {
        String name = commitCoordinatorBuilder.getName();
        if (nameToBuilderMapping.containsKey(name)) {
            throw new IllegalArgumentException(
                    "commit-coordinator: " +
                            name +
                            " already registered with builder " +
                            commitCoordinatorBuilder.getClass().getName());
        } else {
            nameToBuilderMapping.put(name, commitCoordinatorBuilder);
        }
    }

    /** Returns a {@link CommitCoordinatorClient} for the given `name` and `conf` */
    public static synchronized CommitCoordinatorClient getCommitCoordinatorClient(
            String name, Map<String, String> conf) {
        CommitCoordinatorBuilder builder = nameToBuilderMapping.get(name);
        if (builder == null) {
            throw new IllegalArgumentException("Unknown commit-coordinator: " + name);
        } else {
            return builder.build(conf);
        }
    }

    // Visible only for UTs
    protected static synchronized void clearBuilders() {
        nameToBuilderMapping.clear();
    }
}
