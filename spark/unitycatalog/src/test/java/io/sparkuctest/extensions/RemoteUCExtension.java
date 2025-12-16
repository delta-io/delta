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

package io.sparkuctest.extensions;

import com.google.common.base.Preconditions;
import java.util.Map;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.Strings;
import org.junit.jupiter.api.extension.ExtensionContext;

public class RemoteUCExtension implements UCExtension {

  // The unity catalog URI endpoint environment variable.
  public static final String UC_URI = "UC_URI";
  public static final String UC_CATALOG_NAME = "UC_CATALOG_NAME";
  public static final String UC_SCHEMA_NAME = "UC_SCHEMA_NAME";
  public static final String UC_BASE_LOCATION = "UC_BASE_LOCATION";

  // The unity catalog token environment variable, for personal access token (PAT).
  public static final String UC_TOKEN = "UC_TOKEN";

  private String ucUri;
  private String catalogName;
  private String schemaName;
  private String ucBaseLocation;
  private String ucToken;

  @Override
  public void beforeAll(ExtensionContext context) {
    ucUri = System.getenv(UC_URI);
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(ucUri), "Environment variable '%s' cannot be null or empty", UC_URI);

    catalogName = System.getenv(UC_CATALOG_NAME);
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(catalogName),
        "Environment variable '%s' cannot be null or empty",
        UC_CATALOG_NAME);

    schemaName = System.getenv(UC_SCHEMA_NAME);
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(schemaName),
        "Environment variable '%s' cannot be null or empty",
        UC_SCHEMA_NAME);

    ucBaseLocation = System.getenv(UC_BASE_LOCATION);
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(ucBaseLocation),
        "Environment variable '%s' cannot be null or empty",
        UC_BASE_LOCATION);

    ucToken = System.getenv(UC_TOKEN);
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(ucToken),
        "Environment variable '%s' cannot be null or empty",
        UC_TOKEN);
  }

  @Override
  public void afterAll(ExtensionContext context) {
  }

  @Override
  public String catalogName() {
    return catalogName;
  }

  @Override
  public String schemaName() {
    return schemaName;
  }

  @Override
  public String catalogUri() {
    return ucUri;
  }

  @Override
  public void withTempDir(TempDirCode code) throws Exception {
    Path tempDir = new Path(ucBaseLocation, UUID.randomUUID().toString());
    try {
      code.run(tempDir);
    } finally {
      // TODO: How to set the hadoop configuration for different cloud storage ???
      // TODO: Seems like that's serious problem ???
      tempDir.getFileSystem(new Configuration()).delete(tempDir, true);
    }
  }

  @Override
  public Map<String, String> catalogSparkConf() {
    String catalogKey = String.format("spark.sql.catalog.%s", catalogName());
    return Map.of(
        // TODO: Maybe we can abstract another extraSparkConf() method to set those non-catalog
        // TODO: properties, such as filesystem setting.
        "spark.hadoop.fs.s3.impl",
        "org.apache.hadoop.fs.s3a.S3AFileSystem",
        catalogKey,
        "io.unitycatalog.spark.UCSingleCatalog",
        catalogKey + ".uri",
        catalogUri(),
        catalogKey + ".token",
        ucToken);
  }
}
