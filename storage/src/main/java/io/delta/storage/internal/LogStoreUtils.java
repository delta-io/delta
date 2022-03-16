/*
 * Copyright (2021) The Delta Lake Project Authors.
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

package io.delta.storage.internal;

import io.delta.storage.LogStore;
import org.apache.hadoop.conf.Configuration;

import java.lang.reflect.InvocationTargetException;

public class LogStoreUtils {

    public static String logStoreSchemeConfKey(String scheme) {
        return String.format("spark.delta.logStore.%s.impl", scheme);
    }

    public static LogStore createLogStoreWithClassName(
            String className,
            Configuration hadoopConf) throws ClassNotFoundException, NoSuchMethodException,
                                             InvocationTargetException, InstantiationException,
                                             IllegalAccessException {
        Class<?> logStoreClass = Class.forName(
                className, true, Thread.currentThread().getContextClassLoader());

        return (LogStore) logStoreClass.getConstructor(Configuration.class).newInstance(hadoopConf);
    }
}
