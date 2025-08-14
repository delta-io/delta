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
package io.delta.kernel.defaults

import scala.collection.immutable.Seq
import scala.reflect.ClassTag

import io.delta.kernel.exceptions.KernelException
import io.delta.kernel.internal.TableConfig
import io.delta.kernel.internal.util.{ColumnMapping, ColumnMappingSuiteBase}
import io.delta.kernel.internal.util.ColumnMapping.ColumnMappingMode
import io.delta.kernel.types.{ArrayType, DataType, FieldMetadata, IntegerType, LongType, MapType, StringType, StructField, StructType}

class DeltaReplaceTableColumnMappingNameModeSuite extends DeltaReplaceTableColumnMappingSuiteBase {

  override def tblPropertiesCmEnabled: Map[String, String] =
    Map(TableConfig.COLUMN_MAPPING_MODE.getKey -> "name")
}

class DeltaReplaceTableColumnMappingIdModeSuite extends DeltaReplaceTableColumnMappingSuiteBase {

  override def tblPropertiesCmEnabled: Map[String, String] =
    Map(TableConfig.COLUMN_MAPPING_MODE.getKey -> "id")

  // We only need to run the below tests once since they check combos of id and name mode, put them
  // in this suite for this reason
  ColumnMapping.ColumnMappingMode.values().foreach { initialCmMode =>
    ColumnMapping.ColumnMappingMode.values().foreach { replaceCmMode =>
      if (initialCmMode != replaceCmMode) {
        test(s"Cannot change CM mode from $initialCmMode to $replaceCmMode") {
          withTempDirAndEngine { (tablePath, engine) =>
            createInitialTable(
              engine,
              tablePath,
              tableProperties = cmModeTblProperties(initialCmMode),
              includeData = false)
            assert(intercept[UnsupportedOperationException] {
              commitReplaceTable(
                engine,
                tablePath,
                tableProperties = cmModeTblProperties(replaceCmMode))
            }.getMessage.contains(
              s"Changing column mapping mode from $initialCmMode to $replaceCmMode is not " +
                s"currently supported in Kernel during REPLACE TABLE"))
          }
        }
      } else if (initialCmMode != ColumnMappingMode.NONE) {
        test(s"Replace with entirely new schema for cmMode=$initialCmMode assigns CM info") {
          withTempDirAndEngine { (tablePath, engine) =>
            createInitialTable(
              engine,
              tablePath,
              schema = new StructType().add("col1", StringType.STRING),
              tableProperties = cmModeTblProperties(initialCmMode),
              includeData = false)
            commitReplaceTable(
              engine,
              tablePath,
              cmTestSchema(),
              tableProperties = cmModeTblProperties(replaceCmMode))
            verifyCMTestSchemaHasValidColumnMappingInfo(
              getMetadata(engine, tablePath),
              enableIcebergCompatV2 = false,
              initialFieldId = 1)
          }
        }
      }
    }
  }
}

trait DeltaReplaceTableColumnMappingSuiteBase extends DeltaReplaceTableSuiteBase
    with ColumnMappingSuiteBase {

  // Child suites override this to run tests with either id or name based column mapping
  def tblPropertiesCmEnabled: Map[String, String]

  /* ------ Test helpers ------- */

  def cmModeTblProperties(mode: ColumnMappingMode): Map[String, String] = {
    Map(
      TableConfig.COLUMN_MAPPING_MODE.getKey -> mode.value)
  }

  implicit class StructFieldOps(field: StructField) {
    def withCMMetadata(physicalName: String, fieldId: Long): StructField = {
      field.withNewMetadata(
        FieldMetadata.builder()
          .fromMetadata(field.getMetadata)
          .putLong(ColumnMapping.COLUMN_MAPPING_ID_KEY, fieldId)
          .putString(ColumnMapping.COLUMN_MAPPING_PHYSICAL_NAME_KEY, physicalName)
          .build())
    }
  }

  def singletonSchema(colName: String, dataType: DataType, withCmMetadata: Boolean): StructType = {
    var topLevelCol = new StructField(colName, dataType, true)
    if (withCmMetadata) {
      topLevelCol = topLevelCol.withCMMetadata(colName + "-physicalName", 4)
    }
    new StructType().add(topLevelCol)
  }

  def nestedStructSchema(nestedField: StructField, withCmMetadata: Boolean): StructType = {
    singletonSchema("top-struct", new StructType().add(nestedField), withCmMetadata)
  }

  def nestedArraySchema(nestedField: StructField, withCmMetadata: Boolean): StructType = {
    singletonSchema(
      "array-col",
      new ArrayType(new StructType().add(nestedField), true),
      withCmMetadata)
  }

  def nestedMapKeySchema(nestedField: StructField, withCmMetadata: Boolean): StructType = {
    singletonSchema(
      "map-col",
      new MapType(new StructType().add(nestedField), StringType.STRING, true),
      true)
  }

  def nestedMapValueSchema(nestedField: StructField, withCmMetadata: Boolean): StructType = {
    singletonSchema(
      "map-col",
      new MapType(StringType.STRING, new StructType().add(nestedField), true),
      true)
  }

  implicit val exceptionType = ClassTag(classOf[KernelException])

  def checkReplaceThrowsException[T <: Throwable](
      initialSchema: StructType,
      replaceSchema: StructType,
      expectedErrorMessageContains: String)(implicit exceptionType: ClassTag[T]): Unit = {
    withTempDirAndEngine { (tablePath, engine) =>
      createInitialTable(
        engine,
        tablePath,
        schema = initialSchema,
        includeData = false,
        tableProperties = tblPropertiesCmEnabled)
      val e = intercept[T] {
        commitReplaceTable(
          engine,
          tablePath,
          schema = replaceSchema,
          tableProperties = tblPropertiesCmEnabled)
      }
      assert(e.getMessage.contains(expectedErrorMessageContains))
    }
  }

  def checkReplaceSucceeds(
      initialSchema: StructType,
      replaceSchema: StructType): Unit = {
    withTempDirAndEngine { (tablePath, engine) =>
      createInitialTable(
        engine,
        tablePath,
        schema = initialSchema,
        includeData = false,
        tableProperties = tblPropertiesCmEnabled)
      commitReplaceTable(
        engine,
        tablePath,
        schema = replaceSchema,
        tableProperties = tblPropertiesCmEnabled)
      assert(getMetadata(engine, tablePath).getSchema == replaceSchema)
    }
  }

  def testCompatibleFieldIdReuseDifferentNestings(
      testDescription: String,
      initialField: StructField,
      replaceField: StructField): Unit = {
    val initialFieldComplete = initialField.withCMMetadata("col-1", 1)
    val replaceFieldComplete = replaceField.withCMMetadata("col-1", 1)

    test(s"$testDescription - top-level field") {
      val initialSchema = new StructType().add(initialFieldComplete)
      val replaceSchema = new StructType().add(replaceFieldComplete)
      checkReplaceSucceeds(initialSchema, replaceSchema)
    }

    test(s"$testDescription - nested within a struct, with struct fieldIdReuse") {
      val initialSchema = nestedStructSchema(initialFieldComplete, withCmMetadata = true)
      val replaceSchema = nestedStructSchema(replaceFieldComplete, withCmMetadata = true)
      checkReplaceSucceeds(initialSchema, replaceSchema)
    }

    test(s"$testDescription - nested within a struct in an array, with array fieldIdReuse") {
      val initialSchema = nestedArraySchema(initialFieldComplete, withCmMetadata = true)
      val replaceSchema = nestedArraySchema(replaceFieldComplete, withCmMetadata = true)
      checkReplaceSucceeds(initialSchema, replaceSchema)
    }

    test(s"$testDescription - nested within a struct in an map (key), with array fieldIdReuse") {
      val initialSchema = nestedMapKeySchema(initialFieldComplete, withCmMetadata = true)
      val replaceSchema = nestedMapKeySchema(replaceFieldComplete, withCmMetadata = true)
      checkReplaceSucceeds(initialSchema, replaceSchema)
    }

    test(s"$testDescription - nested within a struct in an map (value), with array fieldIdReuse") {
      val initialSchema = nestedMapValueSchema(initialFieldComplete, withCmMetadata = true)
      val replaceSchema = nestedMapValueSchema(replaceFieldComplete, withCmMetadata = true)
      checkReplaceSucceeds(initialSchema, replaceSchema)
    }
  }

  def testIncompatibleFieldIdReuseDifferentNestings[T <: Throwable](
      testDescription: String,
      initialField: StructField,
      replaceField: StructField,
      expectedErrorMessageContains: String,
      initialPhysicalName: String = "col-1",
      replacePhysicalName: String = "col-1")(implicit exceptionType: ClassTag[T]): Unit = {
    val initialFieldComplete = initialField.withCMMetadata(initialPhysicalName, 1)
    val replaceFieldComplete = replaceField.withCMMetadata(replacePhysicalName, 1)

    test(s"$testDescription - top-level field") {
      val initialSchema = new StructType().add(initialFieldComplete)
      val replaceSchema = new StructType().add(replaceFieldComplete)
      checkReplaceThrowsException[T](
        initialSchema,
        replaceSchema,
        expectedErrorMessageContains)
    }

    test(s"$testDescription - nested within a struct, with struct fieldIdReuse") {
      val initialSchema = nestedStructSchema(initialFieldComplete, withCmMetadata = true)
      val replaceSchema = nestedStructSchema(replaceFieldComplete, withCmMetadata = true)
      checkReplaceThrowsException[T](
        initialSchema,
        replaceSchema,
        expectedErrorMessageContains)
    }

    test(s"$testDescription - nested within a struct, without struct fieldIdReuse") {
      val initialSchema = nestedStructSchema(initialFieldComplete, withCmMetadata = false)
      val replaceSchema = nestedStructSchema(replaceFieldComplete, withCmMetadata = false)
      checkReplaceThrowsException[T](
        initialSchema,
        replaceSchema,
        expectedErrorMessageContains)
    }

    test(s"$testDescription - nested within a struct in an array, with array fieldIdReuse") {
      val initialSchema = nestedArraySchema(initialFieldComplete, true)
      val replaceSchema = nestedArraySchema(replaceFieldComplete, true)
      checkReplaceThrowsException[T](
        initialSchema,
        replaceSchema,
        expectedErrorMessageContains)
    }

    test(s"$testDescription - nested within a struct in an array, without array fieldIdReuse") {
      val initialSchema = nestedArraySchema(initialFieldComplete, false)
      val replaceSchema = nestedArraySchema(replaceFieldComplete, false)
      checkReplaceThrowsException[T](
        initialSchema,
        replaceSchema,
        expectedErrorMessageContains)
    }

    test(s"$testDescription - nested within a struct in a map (key), with array fieldIdReuse") {
      val initialSchema = nestedMapKeySchema(initialFieldComplete, true)
      val replaceSchema = nestedMapKeySchema(replaceFieldComplete, true)
      checkReplaceThrowsException[T](
        initialSchema,
        replaceSchema,
        expectedErrorMessageContains)
    }

    test(s"$testDescription - nested within a struct in a map (key), without array fieldIdReuse") {
      val initialSchema = nestedMapKeySchema(initialFieldComplete, false)
      val replaceSchema = nestedMapKeySchema(replaceFieldComplete, false)
      checkReplaceThrowsException[T](
        initialSchema,
        replaceSchema,
        expectedErrorMessageContains)
    }

    test(s"$testDescription - nested within a struct in a map (value), with array fieldIdReuse") {
      val initialSchema = nestedMapKeySchema(initialFieldComplete, true)
      val replaceSchema = nestedMapKeySchema(replaceFieldComplete, true)
      checkReplaceThrowsException[T](
        initialSchema,
        replaceSchema,
        expectedErrorMessageContains)
    }

    test(
      s"$testDescription - nested within a struct in a map (value), without array fieldIdReuse") {
      val initialSchema = nestedMapValueSchema(initialFieldComplete, false)
      val replaceSchema = nestedMapValueSchema(replaceFieldComplete, false)
      checkReplaceThrowsException[T](
        initialSchema,
        replaceSchema,
        expectedErrorMessageContains)
    }
  }

  /* ---------------- TEST CASES ---------------- */

  testIncompatibleFieldIdReuseDifferentNestings(
    "Cannot reuse fieldId incompatible primitive type",
    new StructField("col1", StringType.STRING, true),
    new StructField("col1", IntegerType.INTEGER, true),
    "Cannot change the type of existing field")

  testIncompatibleFieldIdReuseDifferentNestings(
    "Cannot reuse fieldId incompatible primitive type w/logical name change",
    new StructField("col1", StringType.STRING, true),
    new StructField("col2", IntegerType.INTEGER, true),
    "Cannot change the type of existing field")

  testIncompatibleFieldIdReuseDifferentNestings(
    "Cannot reuse fieldId incompatible nullability",
    new StructField("col1", StringType.STRING, true),
    new StructField("col1", StringType.STRING, false),
    "Cannot tighten the nullability of existing field")

  testIncompatibleFieldIdReuseDifferentNestings(
    "Cannot reuse fieldId incompatible nullability for array-type field",
    new StructField("col1", new ArrayType(StringType.STRING, true), true),
    new StructField("col1", new ArrayType(StringType.STRING, false), true),
    "Cannot tighten the nullability of existing field")

  testIncompatibleFieldIdReuseDifferentNestings(
    "Cannot reuse fieldId incompatible nullability for map-type field",
    new StructField("col1", new MapType(StringType.STRING, StringType.STRING, true), true),
    new StructField("col1", new MapType(StringType.STRING, StringType.STRING, false), true),
    "Cannot tighten the nullability of existing field")

  testIncompatibleFieldIdReuseDifferentNestings(
    "Cannot reuse fieldId incompatible nullability for struct of struct",
    new StructField(
      "col1",
      new StructType()
        .add("col2", StringType.STRING, true),
      true),
    new StructField(
      "col1",
      new StructType()
        .add("col2", StringType.STRING, true),
      false),
    "Cannot tighten the nullability of existing field")

  testIncompatibleFieldIdReuseDifferentNestings(
    "Cannot reuse fieldId incompatible type for array-type field",
    new StructField("col1", new ArrayType(StringType.STRING, true), true),
    new StructField("col1", new ArrayType(IntegerType.INTEGER, true), true),
    "Cannot change the type of existing field")

  testIncompatibleFieldIdReuseDifferentNestings(
    "Cannot reuse fieldId incompatible key-type for map-type field",
    new StructField("col1", new MapType(StringType.STRING, StringType.STRING, true), true),
    new StructField("col1", new MapType(IntegerType.INTEGER, StringType.STRING, true), true),
    "Cannot change the type of existing field")

  testIncompatibleFieldIdReuseDifferentNestings(
    "Cannot reuse fieldId incompatible value-type for map-type field",
    new StructField("col1", new MapType(StringType.STRING, StringType.STRING, true), true),
    new StructField("col1", new MapType(StringType.STRING, IntegerType.INTEGER, true), true),
    "Cannot change the type of existing field")

  testIncompatibleFieldIdReuseDifferentNestings(
    "Cannot reuse fieldId incompatible primitive type, type-widening supported change",
    new StructField("col1", IntegerType.INTEGER, true),
    new StructField("col1", LongType.LONG, true),
    "Cannot change the type of existing field")

  test("Cannot add a new field with a fieldId <= maxColId") {
    val initialSchema = new StructType()
      .add(new StructField("col1", StringType.STRING, true).withCMMetadata("col-200", 200))
    val replaceSchema = new StructType()
      .add(new StructField("col1", StringType.STRING, true).withCMMetadata("col-200", 200))
      .add(new StructField("col2", StringType.STRING, true).withCMMetadata("col-1", 1))
    checkReplaceThrowsException[IllegalArgumentException](
      initialSchema,
      replaceSchema,
      "Cannot add a new column with a fieldId <= maxFieldId")
  }

  testIncompatibleFieldIdReuseDifferentNestings[IllegalArgumentException](
    "Cannot reuse fieldId with change in physical name",
    new StructField("col1", StringType.STRING, true),
    new StructField("col1", StringType.STRING, true),
    "Existing field with id 1 in current schema has physical name " +
      "col1-physical-name which is different",
    initialPhysicalName = "col1-physical-name",
    replacePhysicalName = "0001111023383922")

  val validNullabilityChanges = Seq(
    (true, true),
    (false, false),
    (false, true))

  validNullabilityChanges.foreach { case (initialNullable, replaceNullable) =>
    testCompatibleFieldIdReuseDifferentNestings(
      s"Valid nullability + type change: initialNullable=$initialNullable to " +
        s"replaceNullable$replaceNullable - primitive type",
      new StructField("col1", StringType.STRING, initialNullable),
      new StructField("col1", StringType.STRING, replaceNullable))

    testCompatibleFieldIdReuseDifferentNestings(
      s"Valid nullability + type change: initialNullable=$initialNullable to " +
        s"replaceNullable$replaceNullable - array type",
      new StructField("col1", new ArrayType(StringType.STRING, initialNullable), true),
      new StructField("col1", new ArrayType(StringType.STRING, replaceNullable), true))

    testCompatibleFieldIdReuseDifferentNestings(
      s"Valid nullability + type change: initialNullable=$initialNullable to " +
        s"replaceNullable$replaceNullable - map type",
      new StructField(
        "col1",
        new MapType(StringType.STRING, StringType.STRING, initialNullable),
        true),
      new StructField(
        "col1",
        new MapType(StringType.STRING, StringType.STRING, replaceNullable),
        true))

    testCompatibleFieldIdReuseDifferentNestings(
      s"Valid nullability + type change: initialNullable=$initialNullable to " +
        s"replaceNullable$replaceNullable - struct of struct",
      new StructField(
        "col1",
        new StructType()
          .add(new StructField("col2", StringType.STRING, true)
            // we must set the CM metadata for nested field so it doesn't update for replace
            .withCMMetadata("col-200", 200)),
        initialNullable),
      new StructField(
        "col1",
        new StructType()
          .add(new StructField("col2", StringType.STRING, true)
            .withCMMetadata("col-200", 200)),
        replaceNullable))
  }

  testCompatibleFieldIdReuseDifferentNestings(
    "No type change with logical name change - primitive type",
    new StructField("col1", StringType.STRING, true),
    new StructField("col2", StringType.STRING, true))

  test("Can add a new non-nullable column - top level primitive") {
    val initialSchema = new StructType()
      .add(new StructField("col1", StringType.STRING, true)
        .withCMMetadata("col-1", 1))
    val replaceSchema = new StructType()
      .add(new StructField("col2", StringType.STRING, false)
        .withCMMetadata("col-2", 2))
    checkReplaceSucceeds(initialSchema, replaceSchema)
  }

  testCompatibleFieldIdReuseDifferentNestings(
    "Can add a new non-nullable column - nested within a struct",
    new StructField(
      "struct",
      new StructType()
        .add(new StructField("col1", StringType.STRING, true)
          .withCMMetadata("col-100", 100)),
      true),
    new StructField(
      "struct",
      new StructType()
        .add(new StructField("col1", StringType.STRING, true)
          .withCMMetadata("col-100", 100))
        .add(new StructField("col2", StringType.STRING, false)
          .withCMMetadata("col-200", 200)),
      true))

  test("Cannot provide just a colId without physicalName") {
    val initialSchema = new StructType()
      .add(new StructField("col1", StringType.STRING, true).withCMMetadata("col-0", 0))
    val replaceSchema = new StructType()
      .add(
        new StructField(
          "col1",
          StringType.STRING,
          true,
          FieldMetadata.builder()
            .putLong(ColumnMapping.COLUMN_MAPPING_ID_KEY, 0)
            .build()))
    checkReplaceThrowsException[IllegalArgumentException](
      initialSchema,
      replaceSchema,
      "Both columnId and physicalName must be present if one is present")
  }

  test("Assigns colId to new fields correctly based on previous maxFieldId with partial" +
    " fieldId reuse") {
    withTempDirAndEngine { (tablePath, engine) =>
      val baseSchema = new StructType()
        .add(new StructField(
          "col1",
          StringType.STRING,
          true).withCMMetadata("col1-physical-name", 0))
      val initialSchema = baseSchema
        .add("col2", StringType.STRING, true)
        .add("col3", StringType.STRING, true)
      createInitialTable(
        engine,
        tablePath,
        schema = initialSchema,
        tableProperties = tblPropertiesCmEnabled,
        includeData = false)
      assert(TableConfig.COLUMN_MAPPING_MAX_COLUMN_ID.fromMetadata(getMetadata(
        engine,
        tablePath)) == 2)
      // Update the schema such that the only present fieldId is 0, but the max should still be 2
      updateTableMetadata(
        engine,
        tablePath,
        baseSchema)
      assert(TableConfig.COLUMN_MAPPING_MAX_COLUMN_ID.fromMetadata(getMetadata(
        engine,
        tablePath)) == 2)
      // Replace the table with a schema with some fieldId re-use, but also some new columns without
      // a fieldId
      val replaceSchema = baseSchema
        .add("col2", StringType.STRING, true)
        .add("col4", StringType.STRING, true)
      commitReplaceTable(
        engine,
        tablePath,
        schema = replaceSchema,
        tableProperties = tblPropertiesCmEnabled)
      assert(TableConfig.COLUMN_MAPPING_MAX_COLUMN_ID.fromMetadata(getMetadata(
        engine,
        tablePath)) == 4)
      val resultSchema = getMetadata(engine, tablePath).getSchema
      assert(ColumnMapping.getColumnId(resultSchema.get("col1")) == 0)
      assert(ColumnMapping.getColumnId(resultSchema.get("col2")) == 3)
      assert(ColumnMapping.getColumnId(resultSchema.get("col4")) == 4)
      assert(ColumnMapping.getPhysicalName(resultSchema.get("col1")) == "col1-physical-name")
      // These should have UUID physical names which start with "col-"
      assert(ColumnMapping.getPhysicalName(resultSchema.get("col2")).startsWith("col-"))
      assert(ColumnMapping.getPhysicalName(resultSchema.get("col4")).startsWith("col-"))
    }
  }

  test("Replace correctly updates the maxFieldId when providing fieldId in the replace schema") {
    withTempDirAndEngine { (tablePath, engine) =>
      val initialSchema = new StructType()
        .add(new StructField(
          "col1",
          StringType.STRING,
          true).withCMMetadata("col1-physical-name", 0))
      createInitialTable(
        engine,
        tablePath,
        schema = initialSchema,
        tableProperties = tblPropertiesCmEnabled,
        includeData = false)
      assert(TableConfig.COLUMN_MAPPING_MAX_COLUMN_ID.fromMetadata(getMetadata(
        engine,
        tablePath)) == 0)
      // Replace the table with a schema with new column with provided fieldId
      val replaceSchema = initialSchema
        .add(new StructField(
          "new-col",
          StringType.STRING,
          true).withCMMetadata("col-200", 200))
      commitReplaceTable(
        engine,
        tablePath,
        schema = replaceSchema,
        tableProperties = tblPropertiesCmEnabled)
      assert(TableConfig.COLUMN_MAPPING_MAX_COLUMN_ID.fromMetadata(getMetadata(
        engine,
        tablePath)) == 200)
    }
  }
}
