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
package io.delta.kernel.exceptions;

import io.delta.kernel.annotation.Evolving;
import java.util.Optional;

/**
 * Thrown when trying to create a Delta table at a location where a Delta table already exists.
 *
 * @since 3.2.0
 */
@Evolving
public class TableAlreadyExistsException extends KernelException {
  private final String tablePath;
  private final Optional<String> context;

  public TableAlreadyExistsException(String tablePath, String context) {
    this.tablePath = tablePath;
    this.context = Optional.ofNullable(context);
  }

  public TableAlreadyExistsException(String tablePath) {
    this(tablePath, null);
  }

  @Override
  public String getMessage() {
    return String.format(
        "Delta table already exists at `%s`.%s",
        tablePath, context.map(c -> " Context: " + c).orElse(""));
  }
}
