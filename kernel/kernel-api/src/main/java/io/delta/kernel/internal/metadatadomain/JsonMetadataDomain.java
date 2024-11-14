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
package io.delta.kernel.internal.metadatadomain;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.*;
import io.delta.kernel.exceptions.KernelException;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.actions.DomainMetadata;
import io.delta.kernel.internal.rowtracking.RowTrackingMetadataDomain;
import java.util.Optional;

/**
 * Abstract class representing a JSON metadata domain, whose configuration string is a JSON
 * serialization of a domain object. This class provides methods to serialize and deserialize a
 * metadata domain to and from JSON. Concrete implementations, such as {@link
 * RowTrackingMetadataDomain}, should extend this class to define a specific metadata domain.
 */
public abstract class JsonMetadataDomain {
  // We explicitly set the ObjectMapper to fail on missing properties and unknown properties
  private static final ObjectMapper OBJECT_MAPPER =
      new ObjectMapper()
          .configure(DeserializationFeature.FAIL_ON_MISSING_CREATOR_PROPERTIES, true)
          .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true);

  /**
   * Deserializes a JSON string into an instance of the specified metadata domain.
   *
   * @param json the JSON string to deserialize
   * @param clazz the concrete class of the metadata domain object to deserialize into
   * @param <T> the type of the object
   * @return the deserialized object
   * @throws KernelException if the JSON string cannot be parsed
   */
  protected static <T> T fromJsonConfiguration(String json, Class<T> clazz) {
    try {
      return OBJECT_MAPPER.readValue(json, clazz);
    } catch (JsonProcessingException e) {
      throw new KernelException(
          String.format(
              "Failed to parse JSON string into a %s instance. JSON content: %s",
              clazz.getSimpleName(), json),
          e);
    }
  }

  /**
   * Retrieves the domain metadata from a snapshot for a given domain, and deserializes it into an
   * instance of the specified metadata domain class.
   *
   * @param snapshot the snapshot to read from
   * @param clazz the metadata domain class of the object to deserialize into
   * @param domainName the name of the domain
   * @param <T> the type of the metadata domain object
   * @return an Optional containing the deserialized object if the domain metadata is found,
   *     otherwise an empty Optional
   */
  protected static <T> Optional<T> fromSnapshot(
      SnapshotImpl snapshot, Class<T> clazz, String domainName) {
    return Optional.ofNullable(snapshot.getDomainMetadataMap().get(domainName))
        .filter(domainMetadata -> !domainMetadata.isRemoved())
        .map(domainMetadata -> fromJsonConfiguration(domainMetadata.getConfiguration(), clazz));
  }

  /**
   * Returns the name of the domain.
   *
   * @return the domain name
   */
  @JsonIgnore
  public abstract String getDomainName();

  /**
   * Serializes this object into a JSON string.
   *
   * @return the JSON string representation of this object
   * @throws KernelException if the object cannot be serialized
   */
  public String toJsonConfiguration() {
    try {
      return OBJECT_MAPPER.writeValueAsString(this);
    } catch (JsonProcessingException e) {
      throw new KernelException(
          String.format(
              "Could not serialize %s (domain: %s) to JSON",
              this.getClass().getSimpleName(), getDomainName()),
          e);
    }
  }

  /**
   * Generate a {@link DomainMetadata} action from this metadata domain.
   *
   * @return the DomainMetadata action instance
   */
  public DomainMetadata toDomainMetadata() {
    return new DomainMetadata(getDomainName(), toJsonConfiguration(), false);
  }
}
