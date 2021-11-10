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
 * A marker interface for all Actions that can be applied to a Delta Table.
 * Each action represents a single change to the state of a Delta table.
 * <p>
 * You can use the following code to extract the concrete type of an {@link Action}.
 * <pre>{@code
 *   // {@link io.delta.standalone.DeltaLog.getChanges} is one way to get such actions
 *   List<Action> actions = ...
 *   actions.forEach(x -> {
 *       if (x instanceof AddFile) {
 *          AddFile addFile = (AddFile) x;
 *          ...
 *       } else if (x instanceof AddCDCFile) {
 *          AddCDCFile addCDCFile = (AddCDCFile)x;
 *          ...
 *       } else if ...
 *   });
 * }</pre>
 */
public interface Action {

    /** The maximum version of the protocol that this version of Delta Standalone understands. */
    int readerVersion = 1;
    int writerVersion = 2;
}
