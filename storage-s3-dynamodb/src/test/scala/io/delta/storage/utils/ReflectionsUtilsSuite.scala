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

package io.delta.storage.utils

import io.delta.storage.utils.ReflectionsUtilsSuiteHelper.TestOnlyAWSCredentialsProviderWithHadoopConf
import org.apache.hadoop.conf.Configuration
import org.scalatest.funsuite.AnyFunSuite
import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider

class ReflectionsUtilsSuite extends AnyFunSuite {
  private val emptyHadoopConf = new Configuration()

  test("support AWS credentials provider with hadoop Configuration as constructor parameter") {
    val awsProvider = ReflectionUtils.createAwsCredentialsProvider(
      "io.delta.storage.utils.ReflectionsUtilsSuiteHelper" +
        "$TestOnlyAWSCredentialsProviderWithHadoopConf",
      emptyHadoopConf
    )
    assert(
      awsProvider.isInstanceOf[TestOnlyAWSCredentialsProviderWithHadoopConf]
    )
  }

  test("support AWS credentials provider with empty constructor(default from aws lib)") {
    val awsProvider = ReflectionUtils.createAwsCredentialsProvider(
      classOf[EnvironmentVariableCredentialsProvider].getCanonicalName,
      emptyHadoopConf
    )
    assert(awsProvider.isInstanceOf[EnvironmentVariableCredentialsProvider])
  }

  test("do not support AWS credentials provider with unexpected constructors parameters") {
    assertThrows[NoSuchMethodException] {
      ReflectionUtils.createAwsCredentialsProvider(
        "io.delta.storage.utils.ReflectionsUtilsSuiteHelper" +
          "$TestOnlyAWSCredentialsProviderWithUnexpectedConstructor",
        emptyHadoopConf
      )
    }
  }

}
