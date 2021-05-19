/*
 * Copyright (2020) The Delta Lake Project Authors.
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

import org.apache.spark.annotation.DeveloperApi;

import java.io.Closeable;
import java.util.Iterator;

/**
 * :: DeveloperApi ::
 *
 * An iterator that may contain resources which should be released after use. Users of
 * CloseableIterator are responsible for closing the iterator if they are done with it.
 *
 * @since 1.0.0
 */
@DeveloperApi
public interface CloseableIterator<T> extends Iterator<T>, Closeable {}
