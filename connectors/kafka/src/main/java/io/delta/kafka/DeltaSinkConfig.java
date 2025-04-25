package io.delta.kafka;

import java.util.Map;
import org.apache.iceberg.connect.IcebergSinkConfig;

public class DeltaSinkConfig extends IcebergSinkConfig {
  public DeltaSinkConfig(Map<String, String> originalProps) {
    super(originalProps);
  }

  public static String version() {
    return "3.3.0-SNAPSHOT";
  }
}
