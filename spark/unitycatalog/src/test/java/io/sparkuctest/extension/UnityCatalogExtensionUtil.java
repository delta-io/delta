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

import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UnityCatalogExtensionUtil {

  private static final Logger LOG = LoggerFactory.getLogger(UnityCatalogExtensionUtil.class);

  public static final String UC_EXTENSION_CLASS = "UC_EXTENSION_CLASS";

  // The unity catalog URI endpoint environment variable.
  public static final String UC_URI = "UC_URI";
  public static final String UC_CATALOG_NAME = "UC_CATALOG_NAME";
  public static final String UC_SCHEMA_NAME = "UC_SCHEMA_NAME";
  public static final String UC_BASE_LOCATION = "UC_BASE_LOCATION";

  // The unity catalog token environment variable, for personal access token (PAT).
  public static final String UC_TOKEN = "UC_TOKEN";

  public static UnityCatalogExtension initialize() {
    UnityCatalogExtension extension;

    String implClass = System.getenv(UC_EXTENSION_CLASS);
    if (!Strings.isNullOrEmpty(implClass)) {
      LOG.info("THe initializing UnityCatalogExtension is: {}", implClass);
      try {
        extension = (UnityCatalogExtension) Class.forName(implClass)
            .getDeclaredConstructor()
            .newInstance();
      } catch (Exception e) {
        throw new RuntimeException(
            "Cannot initialize UnityCatalogExtension for class name: " + implClass, e);
      }
    } else {
      extension = new LocalUnityCatalogExtension();
    }

    return extension;
  }
}
