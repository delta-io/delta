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
package io.delta.kernel.internal.lang;

import io.delta.kernel.internal.util.Tuple2;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public final class ListUtils {
  private ListUtils() {}

  public static <T> Tuple2<List<T>, List<T>> partition(
      List<T> list, Predicate<? super T> predicate) {
    final Map<Boolean, List<T>> partitionMap =
        list.stream().collect(Collectors.partitioningBy(predicate));
    return new Tuple2<>(partitionMap.get(true), partitionMap.get(false));
  }

  public static <T> T last(List<T> list) {
    return list.get(list.size() - 1);
  }

  /** Remove once supported JDK (build) version is 21 or above */
  public static <T> T getFirst(List<T> list) {
    if (list.isEmpty()) {
      throw new NoSuchElementException();
    } else {
      return list.get(0);
    }
  }

  /** Remove once supported JDK (build) version is 21 or above */
  public static <T> T getLast(List<T> list) {
    if (list.isEmpty()) {
      throw new NoSuchElementException();
    } else {
      return list.get(list.size() - 1);
    }
  }
}
