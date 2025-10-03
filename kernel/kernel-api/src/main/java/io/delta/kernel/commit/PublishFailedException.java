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

package io.delta.kernel.commit;

/** Exception thrown when publishing catalog commits to the Delta log fails. */
public class PublishFailedException extends RuntimeException {

  /**
   * Constructs a new PublishFailedException with the specified detail message.
   *
   * @param message the detail message
   */
  public PublishFailedException(String message) {
    super(message);
  }

  /**
   * Constructs a new PublishFailedException with the specified detail message and cause.
   *
   * @param message the detail message
   * @param cause the cause of the exception
   */
  public PublishFailedException(String message, Throwable cause) {
    super(message, cause);
  }
}
