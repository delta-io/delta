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
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Optional;

/**
 * A test implementation of {@link JsonMetadataDomain} for testing purposes. It has two Optional
 * fields and one primitive field.
 */
final public class TestJsonMetadataDomain extends JsonMetadataDomain {
  final private Optional<String> field1;
  final private Optional<String> field2;
  final private int field3;

  @JsonCreator
  public TestJsonMetadataDomain(
      @JsonProperty("field1") Optional<String> field1,
      @JsonProperty("field2") Optional<String> field2,
      @JsonProperty("field3") int field3) {
    this.field1 = field1;
    this.field2 = field2;
    this.field3 = field3;
  }

  @Override
  public String getDomainName() {
    return "testDomain";
  }

  public Optional<String> getField1() {
    return field1;
  }

  public Optional<String> getField2() {
    return field2;
  }

  public int getField3() {
    return field3;
  }

  public static TestJsonMetadataDomain fromJsonConfiguration(String json) {
    return JsonMetadataDomain.fromJsonConfiguration(json, TestJsonMetadataDomain.class);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof TestJsonMetadataDomain) {
      TestJsonMetadataDomain other = (TestJsonMetadataDomain) obj;
      return field1.equals(other.field1) && field2.equals(other.field2) && field3 == other.field3;
    }
    return false;
  }
}
