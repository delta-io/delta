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
package io.delta.kernel.internal.tablefeatures;

import io.delta.kernel.data.Row;
import io.delta.kernel.internal.data.TransactionStateRow;

public class TableFeatureSupport {
  /** The string constant "supported" for uses in table properties. */
  public static String FEATURE_PROP_SUPPORTED = "supported";

  public static boolean supports(Row transactionState, TableFeature feature) {
    return FEATURE_PROP_SUPPORTED.equals(
        TransactionStateRow.getConfiguration(transactionState)
            .getOrDefault(
                TableFeatures.SET_TABLE_FEATURE_SUPPORTED_PREFIX + feature.featureName(),
                "unknown"));
  }
}
