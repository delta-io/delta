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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import io.delta.kernel.exceptions.KernelException;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.actions.DomainMetadata;
import io.delta.kernel.internal.rowtracking.RowTrackingMetadataDomain;
import java.util.Optional;

/**
 * Abstract class representing a metadata domain, whose configuration string is a JSON serialization
 * of a domain object. This class provides methods to serialize and deserialize a metadata domain to
 * and from JSON. Concrete implementations, such as {@link RowTrackingMetadataDomain}, should extend
 * this class to define a specific metadata domain.
 *
 * <p>A metadata domain differs from {@link DomainMetadata}: {@link DomainMetadata} represents an
 * action that modifies the table's state by updating the configuration of a named metadata domain.
 * A metadata domain is a named domain used to organize configurations related to a specific table
 * feature.
 *
 * <p>For example, the row tracking feature uses a {@link RowTrackingMetadataDomain} to store the
 * highest assigned fresh row id of the table. When updated, the row tracking feature creates and
 * commits a new {@link DomainMetadata} action to reflect the change.
 *
 * <p>Serialization and deserialization are handled using Jackson's annotations. By default, all
 * public fields and getters are included in the serialization. When creating subclasses, ensure
 * that all fields to be serialized are accessible either through public fields or getters.
 *
 * <p>To control this behavior:
 *
 * <ul>
 *   <li>Annotate methods/fields with {@link JsonIgnore} if they should be excluded from
 *       serialization/deserialization.
 *   <li>Annotate constructor with {@link JsonCreator} to specify which constructor to use during
 *       deserialization.
 *   <li>Use {@link JsonProperty} on constructor parameters to define the JSON field names during
 *       deserialization.
 * </ul>
 */
public abstract class JsonMetadataDomain {
  // Configure the ObjectMapper with the same settings used in Delta-Spark
  protected static final ObjectMapper OBJECT_MAPPER =
      new ObjectMapper()
          .registerModule(new Jdk8Module()) // To support Optional
          .setSerializationInclusion(JsonInclude.Include.NON_ABSENT) // Exclude empty Optionals
          .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false /* state */);

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
    return new DomainMetadata(getDomainName(), toJsonConfiguration(), false /* removed */);
  }
}
