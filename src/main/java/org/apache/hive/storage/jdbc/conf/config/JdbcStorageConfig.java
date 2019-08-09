/*
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hive.storage.jdbc.conf.config;


import static org.apache.hive.storage.jdbc.conf.config.Constants.*;

public enum JdbcStorageConfig {
  DATABASE_TYPE(JDBC_DATABASE_TYPE, true),
  JDBC_URL(Constants.JDBC_URL, true),
  JDBC_DRIVER_CLASS(JDBC_DRIVER, true),
  QUERY(JDBC_QUERY, false),
  TABLE(JDBC_TABLE, false),
  JDBC_FETCH_SIZE(JDBC_CONFIG_PREFIX + ".jdbc.fetch.size", false),
  COLUMN_MAPPING(JDBC_CONFIG_PREFIX + ".column.mapping", false);

  private String propertyName;
  private boolean required = false;


  JdbcStorageConfig(String propertyName, boolean required) {
    this.propertyName = propertyName;
    this.required = required;
  }

  public String getPropertyName() {
    return propertyName;
  }

  public boolean isRequired() {
    return required;
  }

}
