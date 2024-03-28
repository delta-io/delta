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


public class PromotionGroup {
    public static final byte NUMBER_GROUP = 1;
    public static final byte STRING_GROUP = 2;
    public static final byte TIME_GROUP = 3;
    public static final byte BINARY_GROUP = 4;
    public static final byte BOOLEAN_GROUP = 5;
    public static final byte INTERVAL_GROUP = 6;
    public static final byte ARRAY_GROUP = 7;
    public static final byte MAP_GROUP = 8;
    public static final byte STRUCT_GROUP = 9;

    // Number Precedence
    public static final byte NUMBER_PRECEDENCE_BYTE = 1;
    public static final byte NUMBER_PRECEDENCE_SHORT = 2;
    public static final byte NUMBER_PRECEDENCE_INT = 3;
    public static final byte NUMBER_PRECEDENCE_LONG = 4;
    public static final byte NUMBER_PRECEDENCE_DECIMAL = 5;
    public static final byte NUMBER_PRECEDENCE_FLOAT = 6;
    public static final byte NUMBER_PRECEDENCE_DOUBLE = 7;

    // Time Precedence
    public static final byte TIME_PRECEDENCE_DATE = 1;
    public static final byte TIME_PRECEDENCE_TIMESTAMP_NTZ = 2;
    public static final byte TIME_PRECEDENCE_TIMESTAMP = 3;

}
