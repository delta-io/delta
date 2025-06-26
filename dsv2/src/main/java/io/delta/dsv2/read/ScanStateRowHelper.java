package io.delta.dsv2.read;

import io.delta.kernel.internal.data.ScanStateRow;
import io.delta.kernel.types.StructType;
import java.lang.reflect.Field;

class ScanStateRowHelper {
  private static Field schemaField;

  static {
    try {
      schemaField = ScanStateRow.class.getDeclaredField("SCHEMA");
      schemaField.setAccessible(true);
    } catch (NoSuchFieldException e) {
      throw new RuntimeException("Failed to initialize ScanStateRowHelper", e);
    }
  }

  public static StructType getSchema() {
    try {
      return (StructType) schemaField.get(null);
    } catch (IllegalAccessException e) {
      throw new RuntimeException("Failed to access SCHEMA field", e);
    }
  }
}
