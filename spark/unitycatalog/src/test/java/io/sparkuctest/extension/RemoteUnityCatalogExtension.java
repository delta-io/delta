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

package io.sparkuctest.extension;

import com.google.common.base.Preconditions;
import java.util.Map;
import org.apache.parquet.Strings;
import org.junit.jupiter.api.extension.ExtensionContext;

public class RemoteUnityCatalogExtension implements UnityCatalogExtension {

  private String ucUri;
  private String catalogName;
  private String schemaName;
  private String ucBaseLocation;
  private String ucToken;

  @Override
  public void beforeAll(ExtensionContext context) {
    ucUri = System.getenv(UnityCatalogExtensionUtil.UC_URI);
    Preconditions.checkArgument(!Strings.isNullOrEmpty(ucUri),
        "Environment variable '%s' cannot be null or empty",
        UnityCatalogExtensionUtil.UC_URI);

    catalogName = System.getenv(UnityCatalogExtensionUtil.UC_CATALOG_NAME);
    Preconditions.checkArgument(!Strings.isNullOrEmpty(catalogName),
        "Environment variable '%s' cannot be null or empty",
        UnityCatalogExtensionUtil.UC_CATALOG_NAME);

    schemaName = System.getenv(UnityCatalogExtensionUtil.UC_SCHEMA_NAME);
    Preconditions.checkArgument(!Strings.isNullOrEmpty(schemaName),
        "Environment variable '%s' cannot be null or empty",
        UnityCatalogExtensionUtil.UC_SCHEMA_NAME);

    ucBaseLocation = System.getenv(UnityCatalogExtensionUtil.UC_BASE_LOCATION);
    Preconditions.checkArgument(!Strings.isNullOrEmpty(ucBaseLocation),
        "Environment variable '%s' cannot be null or empty",
        UnityCatalogExtensionUtil.UC_BASE_LOCATION);

    ucToken = System.getenv(UnityCatalogExtensionUtil.UC_TOKEN);
    Preconditions.checkArgument(!Strings.isNullOrEmpty(ucToken),
        "Environment variable '%s' cannot be null or empty",
        UnityCatalogExtensionUtil.UC_TOKEN);
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
  public String rootTestingDir() {
    return ucBaseLocation;
  }

  @Override
  public Map<String, String> catalogSparkConf() {
    String catalogKey = String.format("spark.sql.catalog.%s", catalogName());
    return Map.of(
        // TODO: Maybe we can abstract another extraSparkConf() method to set those non-catalog
        // TODO: properties, such as filesystem setting.
        "spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem",
        catalogKey, "io.unitycatalog.spark.UCSingleCatalog",
        catalogKey + ".uri", catalogUri(),
        catalogKey + ".token", ucToken
    );
  }
}
