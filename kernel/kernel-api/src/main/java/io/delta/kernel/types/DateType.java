/*
 * Copyright (2023) The Delta Lake Project Authors.
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
 * A date type, supporting "0001-01-01" through "9999-12-31".
 * Internally, this is represented as the number of days from 1970-01-01.
 *
 * @since 3.0.0
 */
@Evolving
public class DateType extends BasePrimitiveType {
    private static final byte typePromotionGroup = PromotionGroup.TIME_GROUP;
    private static final byte typePromotionPrecedenceInGroup = PromotionGroup.TIME_PRECEDENCE_DATE;

    public static final DateType DATE = new DateType();

    private DateType() {
        super("date");
    }

    @Override
    public byte getPromotionPrecedence(DataType dataType) {
        return typePromotionPrecedenceInGroup;
    }
    @Override
    public byte getPromotionGroup(DataType dataType) {
        return typePromotionGroup;
    }
}
