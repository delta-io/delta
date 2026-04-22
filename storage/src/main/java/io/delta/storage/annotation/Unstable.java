/*
 * Copyright (2026) The Delta Lake Project Authors.
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
package io.delta.storage.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marker for APIs in {@code delta-storage} that are not yet stable. Method signatures and class
 * shapes may change in any minor release until the API graduates. External consumers of a
 * {@code @Unstable}-annotated type must pin exact Delta versions.
 *
 * <p>Storage cannot depend on {@code org.apache.spark.annotation.Unstable} because the module
 * forbids a Spark dependency (it must remain usable by non-Spark engines like Flink and Kernel).
 * This annotation plays the same role within the storage module and is surfaced in generated
 * Javadoc via {@code @Documented}.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({
    ElementType.TYPE,
    ElementType.METHOD,
    ElementType.FIELD,
    ElementType.CONSTRUCTOR,
    ElementType.PACKAGE
})
public @interface Unstable {}
