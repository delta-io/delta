/*
 * Copyright (2021) The Delta Lake Project Authors.
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

package io.delta.storage.commit.uccommitcoordinator;

/**
 * This exception is thrown by the UC client in case a commit attempt tried to add
 * a UUID-based commit to the wrong table. The table is wrong if the path prefixes
 * of the table and the UUID commit do not match.
 * For example, adding /path/to/table1/_commits/01-uuid.json to the table at
 * /path/to/table2 is not allowed.
 */
public class InvalidTargetTableException extends UCCommitCoordinatorException {
  public InvalidTargetTableException(String message) {
    super(message);
  }
}
