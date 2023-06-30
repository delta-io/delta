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
package io.delta.kernel.internal.actions;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Objects;

import io.delta.kernel.internal.lang.Lazy;

import static java.util.Objects.requireNonNull;

public abstract class FileAction implements Action
{
    protected final String path;
    protected final boolean dataChange;
    private final Lazy<URI> pathAsUri;

    public FileAction(String path, boolean dataChange)
    {
        this.path = requireNonNull(path, "path is null");
        this.dataChange = dataChange;

        this.pathAsUri = new Lazy<>(() -> {
            try {
                return new URI(path);
            }
            catch (URISyntaxException ex) {
                throw new RuntimeException(ex);
            }
        });
    }

    public String getPath()
    {
        return path;
    }

    public boolean isDataChange()
    {
        return dataChange;
    }

    public URI toURI()
    {
        return pathAsUri.get();
    }

    public abstract FileAction copyWithDataChange(boolean dataChange);
}
