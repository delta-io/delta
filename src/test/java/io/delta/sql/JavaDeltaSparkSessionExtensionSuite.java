/*
 * Copyright (2020) The Delta Lake Project Authors.
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

package io.delta.sql;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.Utils;
import org.junit.Test;

import java.io.IOException;

public class JavaDeltaSparkSessionExtensionSuite {

    @Test
    public void testSQLConf() throws IOException {
        SparkSession spark = SparkSession.builder()
                .appName("JavaDeltaSparkSessionExtensionSuiteUsingSQLConf")
                .master("local[2]")
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .getOrCreate();
        try {
            String input = Utils.createTempDir(System.getProperty("java.io.tmpdir"), "input")
                    .getCanonicalPath();
            spark.range(1, 10).write().format("delta").save(input);
            spark.sql("vacuum delta.`" + input + "`");
        } finally {
            spark.stop();
        }
    }
}
