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
package io.delta.standalone.data;

import java.io.Closeable;
import java.util.Iterator;

/**
 * An {@link Iterator} that also need to implement the {@link Closeable} interface. The caller
 * should call {@link #close()} method to free all resources properly after using the iterator.
 */
public interface CloseableIterator<T> extends Iterator<T>, Closeable { }
