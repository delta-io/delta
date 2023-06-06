/*
 * Copyright (2020-present) The Delta Lake Project Authors.
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

package io.delta.standalone.actions;

/**
 * Generic interface for {@link Action}s pertaining to the addition and removal of files.
 */
public interface FileAction extends Action {

    /**
     @return the relative path or the absolute path of the file being added or removed by this
      *      action. If it's a relative path, it's relative to the root of the table. Note: the path
      *      is encoded and should be decoded by {@code new java.net.URI(path)} when using it.
     */
    String getPath();

    /**
     * @return whether any data was changed as a result of this file being added or removed.
     */
    boolean isDataChange();
}
