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

import com.google.common.base.Strings;
import java.util.Map;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;

public interface UCExtension extends BeforeAllCallback, AfterAllCallback {

  String catalogName();

  String schemaName();

  String catalogUri();


  /**
   * Functional interface for test code that takes a temporary directory.
   */
  @FunctionalInterface
  interface TempDirCode {

    void run(Path dir) throws Exception;
  }

  void withTempDir(TempDirCode code) throws Exception;

  Map<String, String> catalogSparkConf();

  // ==========================================================================
  // Environment variables and create() to initialize the UCExtension instance.
  // ==========================================================================

  String UC_EXTENSION_CLASS = "UC_EXTENSION_CLASS";

  static UCExtension initialize() {
    UCExtension extension;

    String implClass = System.getenv(UC_EXTENSION_CLASS);
    if (!Strings.isNullOrEmpty(implClass)) {
      try {
        extension = (UCExtension) Class.forName(implClass).getDeclaredConstructor().newInstance();
      } catch (Exception e) {
        throw new RuntimeException(
            "Cannot initialize UnityCatalogExtension for class name: " + implClass, e);
      }
    } else {
      extension = new LocalUCExtension();
    }

    return extension;
  }
}
