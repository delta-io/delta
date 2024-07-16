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

import java.util.Map;

import io.delta.storage.commit.CommitCoordinatorClient;

import io.delta.kernel.internal.lang.Lazy;

public class InMemoryCommitCoordinatorBuilder implements CommitCoordinatorBuilder {
    private final long batchSize;
    private Lazy<InMemoryCommitCoordinator> inMemoryStore;

    public InMemoryCommitCoordinatorBuilder(long batchSize) {
        this.batchSize = batchSize;
        this.inMemoryStore = new Lazy<>(() -> new InMemoryCommitCoordinator(batchSize));
    }

    /** Name of the commit-coordinator */
    public String getName() {
        return "in-memory";
    }

    /** Returns a commit-coordinator based on the given conf */
    public CommitCoordinatorClient build(Map<String, String> conf) {
        return inMemoryStore.get();
    }
}
