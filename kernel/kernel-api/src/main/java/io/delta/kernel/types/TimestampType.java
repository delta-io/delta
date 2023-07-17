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

/**
 * A timestamp type, supporting [0001-01-01T00:00:00.000000Z, 9999-12-31T23:59:59.999999Z]
 * where the left/right-bound is a date and time of the proleptic Gregorian
 * calendar in UTC+00:00.
 * Internally, this is represented as the number of microseconds since the Unix epoch,
 * 1970-01-01 00:00:00 UTC..
 */
public class TimestampType extends BasePrimitiveType
{
    public static final TimestampType INSTANCE = new TimestampType();

    private TimestampType()
    {
        super("timestamp");
    }
}
