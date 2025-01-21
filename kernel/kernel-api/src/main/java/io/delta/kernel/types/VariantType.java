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
package io.delta.kernel.types;

import io.delta.kernel.annotation.Evolving;

/**
 * A logical variant type.
 *
 * <p>The RFC for the variant data type can be found at
 * https://github.com/delta-io/delta/blob/master/protocol_rfcs/variant-type.md.
 *
 * @since 3.3.0
 */
@Evolving
public class VariantType extends BasePrimitiveType {
  public static final VariantType VARIANT = new VariantType();

  private VariantType() {
    super("variant");
  }
}
