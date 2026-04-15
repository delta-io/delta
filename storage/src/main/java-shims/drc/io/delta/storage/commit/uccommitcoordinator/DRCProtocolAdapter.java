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

package io.delta.storage.commit.uccommitcoordinator;

import io.delta.storage.commit.actions.AbstractProtocol;
import io.unitycatalog.client.delta.model.DeltaProtocol;

import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Adapts a DRC {@link DeltaProtocol} (UC SDK model) to {@link AbstractProtocol}
 * (storage module interface). Zero-copy delegation -- no data is duplicated.
 * Lives in java-shims/drc/ because it depends on UC SDK types.
 */
class DRCProtocolAdapter implements AbstractProtocol {
    private final DeltaProtocol delegate;

    DRCProtocolAdapter(DeltaProtocol delegate) {
        this.delegate = delegate;
    }

    @Override
    public int getMinReaderVersion() {
        return delegate.getMinReaderVersion() != null
            ? delegate.getMinReaderVersion() : 1;
    }

    @Override
    public int getMinWriterVersion() {
        return delegate.getMinWriterVersion() != null
            ? delegate.getMinWriterVersion() : 2;
    }

    @Override
    public Set<String> getReaderFeatures() {
        return delegate.getReaderFeatures() != null
            ? new LinkedHashSet<>(delegate.getReaderFeatures()) : null;
    }

    @Override
    public Set<String> getWriterFeatures() {
        return delegate.getWriterFeatures() != null
            ? new LinkedHashSet<>(delegate.getWriterFeatures()) : null;
    }
}
