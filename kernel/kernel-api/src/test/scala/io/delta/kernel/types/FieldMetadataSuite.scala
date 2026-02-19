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

import scala.jdk.CollectionConverters._

import io.delta.kernel.exceptions.KernelException

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
    val longs: Seq[java.lang.Long] = Seq(1L, 2L, 3L)
    val doubles: Seq[java.lang.Double] = Seq(1.0, 2.0, 3.0)
    val booleans: Seq[java.lang.Boolean] = Seq(true, false, true)
    val strings: Seq[java.lang.String] = Seq("a", "b", "c")
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
    val longs: Seq[java.lang.Long] = Seq(1L, 2L, 3L)
    val doubles: Seq[java.lang.Double] = Seq(1.0, 2.0, 3.0)
    val booleans: Seq[java.lang.Boolean] = Seq(true, false, true)
    val strings: Seq[java.lang.String] = Seq("a", "b", "c")
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

  test("builder.getMetadata handles null correctly") {
    val builder = FieldMetadata.builder()
    assertThat(builder.getMetadata("non-existing")).isNull()
  }

  test("builder.getMetadata with wrong type throws KernelException") {
    val builder = FieldMetadata.builder()
      .putLong("longKey", 23L)

    val err = intercept[KernelException] {
      builder.getMetadata("longKey")
    }

    assert(err.getMessage.contains("Expected '23' to be of type 'FieldMetadata'"))
  }

  test("builder.getMetadata with correct type returns value") {
    val innerMeta = FieldMetadata.builder().putBoolean("key", true).build()
    val builder = FieldMetadata.builder()
      .putFieldMetadata("fieldMetadataKey", innerMeta)

    assertThat(builder.getMetadata("fieldMetadataKey")).isEqualTo(innerMeta)
  }

  test("toString handles empty metadata") {
    val fieldMetadata = FieldMetadata.builder().build()
    val result = fieldMetadata.toString

    assertThat(result).isEqualTo("{}")
  }

  test("toString handles null values and arrays with null elements") {
    val fieldMetadata = FieldMetadata.builder()
      .putString("nullValueKey", null)
      .putStringArray("arrayWithNulls", Array("a", null, "b"))
      .putString("validValue", "test")
      .putStringArray("validArray", Array("x", "y", "z"))
      .build()

    val result = fieldMetadata.toString

    assertThat(result).contains("nullValueKey=null")
    assertThat(result).contains("[a, null, b]")
    assertThat(result).contains("validValue=test")
    assertThat(result).contains("[x, y, z]")
  }

  test("equalsIgnoreKeys ignores specified keys while validating others") {
    val meta1 = FieldMetadata.builder()
      .putString("collation", "UTF8_BINARY")
      .putString("otherKey", "same")
      .putLongArray("arr", Seq[java.lang.Long](1L, 2L).toArray)
      .build()

    val meta2 = FieldMetadata.builder()
      .putString("collation", "EN_CI") // different but should be ignored
      .putString("otherKey", "same")
      .putLongArray("arr", Seq[java.lang.Long](1L, 2L).toArray)
      .build()

    val ignoreCollation: java.util.Set[String] = Set("collation").asJava
    val emptyIgnore: java.util.Set[String] = new java.util.HashSet[String]()

    assertThat(meta1.equalsIgnoreKeys(meta2, ignoreCollation)).isTrue
    assertThat(meta1.equalsIgnoreKeys(meta2, emptyIgnore)).isFalse
  }

  test("equalsIgnoreKeys deepEquals handles arrays properly") {
    val longs: Seq[java.lang.Long] = Seq(1L, 2L, 3L)
    val doubles: Seq[java.lang.Double] = Seq(1.0, 2.0, 3.0)
    val booleans: Seq[java.lang.Boolean] = Seq(true, false, true)
    val strings: Seq[java.lang.String] = Seq("x", "y", "z")
    val stringsWithNulls: Seq[java.lang.String] = Seq("a", null, "b")
    val inner1 = FieldMetadata.builder().putBoolean("k", true).build()
    val inner2 = FieldMetadata.builder().putBoolean("k", true).build()

    val meta1 = FieldMetadata.builder()
      .putLongArray("longArrayKey", longs.toArray)
      .putDoubleArray("doubleArrayKey", doubles.toArray)
      .putBooleanArray("booleanArrayKey", booleans.toArray)
      .putStringArray("stringArrayKey", strings.toArray)
      .putStringArray("stringArrayWithNulls", stringsWithNulls.toArray)
      .putFieldMetadataArray("fieldMetadataArrayKey", Array(inner1))
      .build()

    val meta2 = FieldMetadata.builder()
      .putLongArray("longArrayKey", longs.toArray)
      .putDoubleArray("doubleArrayKey", doubles.toArray)
      .putBooleanArray("booleanArrayKey", booleans.toArray)
      .putStringArray("stringArrayKey", strings.toArray)
      .putStringArray("stringArrayWithNulls", stringsWithNulls.toArray)
      .putFieldMetadataArray("fieldMetadataArrayKey", Array(inner2))
      .build()

    val emptyIgnore: java.util.Set[String] = new java.util.HashSet[String]()
    assertThat(meta1.equalsIgnoreKeys(meta2, emptyIgnore)).isTrue

    // Change one array element to ensure inequality is detected
    val meta3 = FieldMetadata.builder()
      .putLongArray("longArrayKey", Seq[java.lang.Long](1L, 2L, 99L).toArray)
      .putDoubleArray("doubleArrayKey", doubles.toArray)
      .putBooleanArray("booleanArrayKey", booleans.toArray)
      .putStringArray("stringArrayKey", strings.toArray)
      .putStringArray("stringArrayWithNulls", stringsWithNulls.toArray)
      .putFieldMetadataArray("fieldMetadataArrayKey", Array(inner2))
      .build()

    val ignoreLongArrayKey: java.util.Set[String] = Set("longArrayKey").asJava

    assertThat(meta1.equalsIgnoreKeys(meta3, emptyIgnore)).isFalse
    assertThat(meta1.equalsIgnoreKeys(meta3, ignoreLongArrayKey)).isTrue
  }

  test("equalsIgnoreKeys handles case where only one side has the ignored key") {
    val meta1 = FieldMetadata.builder()
      .putString("collation", "EN_CI")
      .putString("common", "v")
      .build()
    val meta2 = FieldMetadata.builder()
      .putString("common", "v")
      .build()

    val ignoreCollation: java.util.Set[String] = Set("collation").asJava
    val emptyIgnore: java.util.Set[String] = new java.util.HashSet[String]()

    assertThat(meta1.equalsIgnoreKeys(meta2, ignoreCollation)).isTrue
    assertThat(meta1.equalsIgnoreKeys(meta2, emptyIgnore)).isFalse
  }

  test("equalsIgnoreKeys handles entries with null value") {
    val meta1 = FieldMetadata.builder()
      .putString("nullableKey", null)
      .putString("same", "x")
      .build()
    val meta2 = FieldMetadata.builder()
      .putString("nullableKey", null)
      .putString("same", "x")
      .build()
    val meta3 = FieldMetadata.builder()
      .putString("nullableKey", "value")
      .putString("same", "x")
      .build()

    val emptyIgnore: java.util.Set[String] = new java.util.HashSet[String]()
    val ignoreNullable: java.util.Set[String] = Set("nullableKey").asJava

    assertThat(meta1.equalsIgnoreKeys(meta2, emptyIgnore)).isTrue
    assertThat(meta1.equalsIgnoreKeys(meta2, ignoreNullable)).isTrue
    assertThat(meta1.equalsIgnoreKeys(meta3, emptyIgnore)).isFalse
    assertThat(meta1.equalsIgnoreKeys(meta3, ignoreNullable)).isTrue
  }

  test("equalsIgnoreKeys handles entries with null key") {
    val meta1 = FieldMetadata.builder()
      .putString(null, "A")
      .putString("same", "x")
      .build()
    val meta2 = FieldMetadata.builder()
      .putString(null, "A")
      .putString("same", "x")
      .build()
    val meta3 = FieldMetadata.builder()
      .putString(null, "B")
      .putString("same", "x")
      .build()
    val meta4 = FieldMetadata.builder()
      .putString("same", "x")
      .build()

    val emptyIgnore: java.util.Set[String] = new java.util.HashSet[String]()
    val ignoreNullKey: java.util.Set[String] = Set[String](null).asJava

    // same key/value pairs -> equal
    assertThat(meta1.equalsIgnoreKeys(meta2, emptyIgnore)).isTrue
    // different values under null key -> unequal unless null key is ignored
    assertThat(meta1.equalsIgnoreKeys(meta3, emptyIgnore)).isFalse
    assertThat(meta1.equalsIgnoreKeys(meta3, ignoreNullKey)).isTrue
    // one side missing the null key -> unequal unless null key is ignored
    assertThat(meta1.equalsIgnoreKeys(meta4, emptyIgnore)).isFalse
    assertThat(meta1.equalsIgnoreKeys(meta4, ignoreNullKey)).isTrue
  }

  test("equalsIgnoreKeys handles entry with null key and null value") {
    val meta1 = FieldMetadata.builder()
      .putString(null, null)
      .putString("same", "x")
      .build()
    val meta2 = FieldMetadata.builder()
      .putString(null, null)
      .putString("same", "x")
      .build()

    val emptyIgnore: java.util.Set[String] = new java.util.HashSet[String]()
    val ignoreNullKey: java.util.Set[String] = Set[String](null).asJava

    assertThat(meta1.equalsIgnoreKeys(meta2, emptyIgnore)).isTrue
    assertThat(meta1.equalsIgnoreKeys(meta2, ignoreNullKey)).isTrue
  }

  test("equalsIgnoreKeys throws when keys is null") {
    val meta1 = FieldMetadata.builder().putString("k", "v").build()
    val meta2 = FieldMetadata.builder().putString("k", "v").build()

    val e = intercept[IllegalArgumentException] {
      meta1.equalsIgnoreKeys(meta2, null.asInstanceOf[java.util.Set[String]])
    }
    assert(e.getMessage == "keys must not be null")
  }

  test("equals validates all keys and values") {
    val meta1 = FieldMetadata.builder()
      .putString("collation", "UTF8_BINARY")
      .putString("otherKey", "same")
      .putLongArray("arr", Seq[java.lang.Long](1L, 2L).toArray)
      .build()

    val meta2 = FieldMetadata.builder()
      .putString("collation", "UTF8_BINARY")
      .putString("otherKey", "same")
      .putLongArray("arr", Seq[java.lang.Long](1L, 2L).toArray)
      .build()

    val meta3 = FieldMetadata.builder()
      .putString("collation", "EN_CI") // different -> should not be equal
      .putString("otherKey", "same")
      .putLongArray("arr", Seq[java.lang.Long](1L, 2L).toArray)
      .build()

    assertThat(meta1.equals(meta2)).isTrue
    assertThat(meta1.equals(meta3)).isFalse
  }

  test("equals handles arrays properly") {
    val longs: Seq[java.lang.Long] = Seq(1L, 2L, 3L)
    val doubles: Seq[java.lang.Double] = Seq(1.0, 2.0, 3.0)
    val booleans: Seq[java.lang.Boolean] = Seq(true, false, true)
    val strings: Seq[java.lang.String] = Seq("x", "y", "z")
    val stringsWithNulls: Seq[java.lang.String] = Seq("a", null, "b")
    val inner1 = FieldMetadata.builder().putBoolean("k", true).build()
    val inner2 = FieldMetadata.builder().putBoolean("k", true).build()

    val meta1 = FieldMetadata.builder()
      .putLongArray("longArrayKey", longs.toArray)
      .putDoubleArray("doubleArrayKey", doubles.toArray)
      .putBooleanArray("booleanArrayKey", booleans.toArray)
      .putStringArray("stringArrayKey", strings.toArray)
      .putStringArray("stringArrayWithNulls", stringsWithNulls.toArray)
      .putFieldMetadataArray("fieldMetadataArrayKey", Array(inner1))
      .build()

    val meta2 = FieldMetadata.builder()
      .putLongArray("longArrayKey", longs.toArray)
      .putDoubleArray("doubleArrayKey", doubles.toArray)
      .putBooleanArray("booleanArrayKey", booleans.toArray)
      .putStringArray("stringArrayKey", strings.toArray)
      .putStringArray("stringArrayWithNulls", stringsWithNulls.toArray)
      .putFieldMetadataArray("fieldMetadataArrayKey", Array(inner2))
      .build()

    assertThat(meta1.equals(meta2)).isTrue

    // Change one array element to ensure inequality is detected
    val meta3 = FieldMetadata.builder()
      .putLongArray("longArrayKey", Seq[java.lang.Long](1L, 2L, 99L).toArray)
      .putDoubleArray("doubleArrayKey", doubles.toArray)
      .putBooleanArray("booleanArrayKey", booleans.toArray)
      .putStringArray("stringArrayKey", strings.toArray)
      .putStringArray("stringArrayWithNulls", stringsWithNulls.toArray)
      .putFieldMetadataArray("fieldMetadataArrayKey", Array(inner2))
      .build()

    assertThat(meta1.equals(meta3)).isFalse
  }

  test("equals handles case where only one side has the key") {
    val meta1 = FieldMetadata.builder()
      .putString("collation", "EN_CI")
      .putString("common", "v")
      .build()
    val meta2 = FieldMetadata.builder()
      .putString("common", "v")
      .build()

    assertThat(meta1.equals(meta2)).isFalse
    assertThat(meta2.equals(meta1)).isFalse
  }

  test("equals handles entries with null value") {
    val meta1 = FieldMetadata.builder()
      .putString("nullableKey", null)
      .putString("same", "x")
      .build()
    val meta2 = FieldMetadata.builder()
      .putString("nullableKey", null)
      .putString("same", "x")
      .build()
    val meta3 = FieldMetadata.builder()
      .putString("nullableKey", "value")
      .putString("same", "x")
      .build()

    assertThat(meta1.equals(meta2)).isTrue
    assertThat(meta1.equals(meta3)).isFalse
  }

  test("equals handles entries with null key") {
    val meta1 = FieldMetadata.builder()
      .putString(null, "A")
      .putString("same", "x")
      .build()
    val meta2 = FieldMetadata.builder()
      .putString(null, "A")
      .putString("same", "x")
      .build()
    val meta3 = FieldMetadata.builder()
      .putString(null, "B")
      .putString("same", "x")
      .build()
    val meta4 = FieldMetadata.builder()
      .putString("same", "x")
      .build()

    // same key/value pairs -> equal
    assertThat(meta1.equals(meta2)).isTrue
    // different values under null key -> unequal
    assertThat(meta1.equals(meta3)).isFalse
    // one side missing the null key -> unequal
    assertThat(meta1.equals(meta4)).isFalse
  }

  test("equals handles entry with null key and null value") {
    val meta1 = FieldMetadata.builder()
      .putString(null, null)
      .putString("same", "x")
      .build()
    val meta2 = FieldMetadata.builder()
      .putString(null, null)
      .putString("same", "x")
      .build()

    assertThat(meta1.equals(meta2)).isTrue
  }

  test("equals returns false for null or different class") {
    val meta = FieldMetadata.builder().putString("k", "v").build()
    assertThat(meta.equals(null)).isFalse
    assertThat(meta.equals("not-metadata")).isFalse
  }
}
