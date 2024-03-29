/*
 * Copyright (2024) The Delta Lake Project Authors.
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
package io.delta.kernel.defaults.internal;

import static java.lang.String.format;

public class DefaultTableClientErrors {

    // TODO update to be table client exception with future exception framework
    //  (see delta-io/delta#2231)
    public static IllegalArgumentException canNotInstantiateLogStore(String logStoreClassName) {
        return new IllegalArgumentException(
                format("Can not instantiate `LogStore` class: %s", logStoreClassName));
    }
}
