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
package io.delta.kernel.types

import org.assertj.core.api.Assertions.{assertThat, assertThatThrownBy}
import org.scalatest.funsuite.AnyFunSuite

class FieldMetadataSuite extends AnyFunSuite {

  test("retrieving non-existing key returns null") {
    assertThat(FieldMetadata.builder().build().get("non-existing")).isNull()
    assertThat(FieldMetadata.builder().putBoolean("key", false).build()
      .getLong("non-existing")).isNull()
  }

  test("retrieving key with null value should never throw") {
    val meta = FieldMetadata.builder().putNull("nullKey").build()
    assertThat(meta.getLong("nullKey")).isNull()
    assertThat(meta.getBoolean("nullKey")).isNull()
  }

  test("retrieving key with wrong type throws exception") {
    val longs : Seq[java.lang.Long] = Seq(1L, 2L, 3L)
    val doubles : Seq[java.lang.Double] = Seq(1.0, 2.0, 3.0)
    val booleans : Seq[java.lang.Boolean] = Seq(true, false, true)
    val strings : Seq[java.lang.String] = Seq("a", "b", "c")
    val innerMeta = FieldMetadata.builder().putBoolean("key", true).build()
    val meta = FieldMetadata.builder()
      .putLong("longKey", 23L)
      .putDouble("doubleKey", 23.0)
      .putBoolean("booleanKey", true)
      .putString("stringKey", "random")
      .putFieldMetadata("fieldMetadataKey", innerMeta)
      .putLongArray("longArrayKey", longs.toArray)
      .putDoubleArray("doubleArrayKey", doubles.toArray)
      .putBooleanArray("booleanArrayKey", booleans.toArray)
      .putStringArray("stringArrayKey", strings.toArray)
      .putFieldMetadataArray("fieldMetadataArrayKey", Seq(innerMeta).toArray)
      .build

    assertThatThrownBy(() => meta.getLongArray("longKey"))
      .isInstanceOf(classOf[IllegalArgumentException])

    assertThatThrownBy(() => meta.getDoubleArray("doubleKey"))
      .isInstanceOf(classOf[IllegalArgumentException])

    assertThatThrownBy(() => meta.getBooleanArray("booleanKey"))
      .isInstanceOf(classOf[IllegalArgumentException])

    assertThatThrownBy(() => meta.getStringArray("stringKey"))
      .isInstanceOf(classOf[IllegalArgumentException])

    assertThatThrownBy(() => meta.getMetadataArray("fieldMetadataKey"))
      .isInstanceOf(classOf[IllegalArgumentException])

    assertThatThrownBy(() => meta.getLong("longArrayKey"))
      .isInstanceOf(classOf[IllegalArgumentException])

    assertThatThrownBy(() => meta.getDouble("doubleArrayKey"))
      .isInstanceOf(classOf[IllegalArgumentException])

    assertThatThrownBy(() => meta.getBoolean("booleanArrayKey"))
      .isInstanceOf(classOf[IllegalArgumentException])

    assertThatThrownBy(() => meta.getString("stringArrayKey"))
      .isInstanceOf(classOf[IllegalArgumentException])

    assertThatThrownBy(() => meta.getMetadata("fieldMetadataArrayKey"))
      .isInstanceOf(classOf[IllegalArgumentException])
  }

  test("retrieving key with correct type returns value") {
    val longs : Seq[java.lang.Long] = Seq(1L, 2L, 3L)
    val doubles : Seq[java.lang.Double] = Seq(1.0, 2.0, 3.0)
    val booleans : Seq[java.lang.Boolean] = Seq(true, false, true)
    val strings : Seq[java.lang.String] = Seq("a", "b", "c")
    val innerMeta = FieldMetadata.builder().putBoolean("key", true).build()
    val meta = FieldMetadata.builder()
      .putLong("longKey", 23L)
      .putDouble("doubleKey", 23.0)
      .putBoolean("booleanKey", true)
      .putString("stringKey", "random")
      .putFieldMetadata("fieldMetadataKey", innerMeta)
      .putLongArray("longArrayKey", longs.toArray)
      .putDoubleArray("doubleArrayKey", doubles.toArray)
      .putBooleanArray("booleanArrayKey", booleans.toArray)
      .putStringArray("stringArrayKey", strings.toArray)
      .putFieldMetadataArray("fieldMetadataArrayKey", Seq(innerMeta).toArray)
      .build

    assertThat(meta.getLong("longKey")).isEqualTo(23L)
    assertThat(meta.getDouble("doubleKey")).isEqualTo(23.0)
    assertThat(meta.getBoolean("booleanKey")).isTrue
    assertThat(meta.getString("stringKey")).isEqualTo("random")
    assertThat(meta.getMetadata("fieldMetadataKey")).isEqualTo(innerMeta)
    assertThat(meta.getLongArray("longArrayKey")).isEqualTo(longs.toArray)
    assertThat(meta.getDoubleArray("doubleArrayKey")).isEqualTo(doubles.toArray)
    assertThat(meta.getBooleanArray("booleanArrayKey")).isEqualTo(booleans.toArray)
    assertThat(meta.getStringArray("stringArrayKey")).isEqualTo(strings.toArray)
    assertThat(meta.getMetadataArray("fieldMetadataArrayKey"))
      .isEqualTo(Seq(innerMeta).toArray)
  }
}
