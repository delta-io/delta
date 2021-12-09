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

package org.apache.spark.sql.interface.system.structured.relational.connection

import java.security.PrivilegedExceptionAction
import java.sql.{Connection, Driver}
import java.util.Properties

import org.apache.hadoop.security.UserGroupInformation

import org.apache.spark.sql.interface.system.structured.relational.JDBCOptions

private[sql] class OracleConnectionProvider extends SecureConnectionProvider {
  override val driverClass = "oracle.jdbc.OracleDriver"

  override val name: String = "oracle"

  override def appEntry(driver: Driver, options: JDBCOptions): String = "kprb5module"

  override def getConnection(driver: Driver, options: Map[String, String]): Connection = {
    val jdbcOptions = new JDBCOptions(options)
    setAuthenticationConfig(driver, jdbcOptions)
    UserGroupInformation.loginUserFromKeytabAndReturnUGI(jdbcOptions.principal, jdbcOptions.keytab)
      .doAs(
        new PrivilegedExceptionAction[Connection]() {
          override def run(): Connection = {
            OracleConnectionProvider.super.getConnection(driver, options)
          }
        }
      )
  }

  override def getAdditionalProperties(options: JDBCOptions): Properties = {
    val result = new Properties()
    // This prop is needed to turn on kerberos authentication in the JDBC driver.
    // The possible values can be found in AnoServices public interface
    // The value is coming from AUTHENTICATION_KERBEROS5 final String in driver version 19.6.0.0
    result.put("oracle.net.authentication_services", "(KERBEROS5)");
    result
  }
}
