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

package io.delta.flink.sink;

import io.delta.kernel.types.StructType;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * {@code DeltaSchemaDigest} computes a deterministic, compact digest representation of a Delta
 * table schema.
 *
 * <p>The digest captures the essential structural aspects of a schema—such as column names, data
 * types, nullability, and nested field structure—so that two schemas with the same logical
 * definition produce the same digest value.
 *
 * <p>This abstraction is primarily used to enable efficient schema evolution detection without
 * performing expensive deep schema comparisons. By comparing digests, callers can quickly determine
 * whether a schema has changed in a way that may require action (for example, triggering a schema
 * update, compatibility check, or commit-time validation).
 *
 * <p>{@code DeltaSchemaDigest} is intended for internal use by components involved in schema
 * tracking, commit coordination, or metadata validation, and does not replace full schema
 * inspection when detailed differences are required.
 */
public class DeltaSchemaDigest {
  private final StructType schema;

  public DeltaSchemaDigest(StructType schema) {
    this.schema = schema;
  }

  /**
   * Generate a SHA-256 digest
   *
   * @return schema digest in SHA256
   */
  public String digest() {
    byte[] schemaBytes = schema.toJson().getBytes(StandardCharsets.UTF_8);
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      byte[] digestBytes = digest.digest(schemaBytes);
      StringBuilder sb = new StringBuilder(digestBytes.length * 2);
      for (byte b : digestBytes) {
        sb.append(String.format("%02x", b));
      }
      return sb.toString();
    } catch (NoSuchAlgorithmException e) {
      // SHA-256 is guaranteed to exist in Java
      throw new RuntimeException(e);
    }
  }
}
