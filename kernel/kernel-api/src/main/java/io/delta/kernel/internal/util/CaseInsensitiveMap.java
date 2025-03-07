/*
 * Copyright (2025) The Delta Lake Project Authors.
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
package io.delta.kernel.internal.util;

import static io.delta.kernel.internal.util.Preconditions.checkArgument;

import java.util.Collection;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * A map that is case-insensitive in its keys. This map is not thread-safe, and key is
 * case-insensitive and of string type.
 *
 * @param <V>
 */
public class CaseInsensitiveMap<V> implements Map<String, V> {
  private final Map<String, V> innerMap = new HashMap<>();

  @Override
  public V get(Object key) {
    return innerMap.get(toLowerCase(key));
  }

  @Override
  public V put(String key, V value) {
    return innerMap.put(toLowerCase(key), value);
  }

  @Override
  public void putAll(Map<? extends String, ? extends V> m) {
    // behavior of this method is not defined on how to handle duplicates
    // don't support this use case, as it is not needed in Kernel
    throw new UnsupportedOperationException("putAll");
  }

  @Override
  public V remove(Object key) {
    return innerMap.remove(toLowerCase(key));
  }

  @Override
  public boolean containsKey(Object key) {
    return innerMap.containsKey(toLowerCase(key));
  }

  @Override
  public boolean containsValue(Object value) {
    return innerMap.containsValue(value);
  }

  @Override
  public Set<String> keySet() {
    // no need to convert to lower case here as the inserted keys are already in lower case
    return innerMap.keySet();
  }

  @Override
  public Set<Entry<String, V>> entrySet() {
    // no need to convert to lower case here as the inserted keys are already in lower case
    return innerMap.entrySet();
  }

  @Override
  public Collection<V> values() {
    return innerMap.values();
  }

  @Override
  public int size() {
    return innerMap.size();
  }

  @Override
  public boolean isEmpty() {
    return innerMap.isEmpty();
  }

  @Override
  public void clear() {
    innerMap.clear();
  }

  private String toLowerCase(Object key) {
    if (key == null) {
      return null;
    }
    checkArgument(key instanceof String, "Key must be a string");
    return ((String) key).toLowerCase(Locale.ROOT);
  }
}
