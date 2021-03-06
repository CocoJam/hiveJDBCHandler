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
package org.apache.hive.storage.jdbc.dao.dataBase;

import com.google.common.base.Preconditions;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hive.storage.jdbc.util.Utilies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hive.storage.jdbc.conf.config.JdbcStorageConfig;
import org.apache.hive.storage.jdbc.conf.config.JdbcStorageConfigManager;
import org.apache.hive.storage.jdbc.exception.HiveJdbcDatabaseAccessException;
import static org.apache.hive.storage.jdbc.conf.config.Constants.*;
import javax.sql.DataSource;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.*;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A data accessor that should in theory work with all JDBC compliant database drivers.
 */
public class GenericJdbcDatabaseAccessor implements DatabaseAccessor {

  protected static final String DBCP_CONFIG_PREFIX = JDBC_CONFIG_PREFIX + ".dbcp";
  protected static final int DEFAULT_FETCH_SIZE = 1000;
  protected static final Logger LOGGER = LoggerFactory.getLogger(GenericJdbcDatabaseAccessor.class);
  protected DataSource dbcpDataSource = null;
  static final Pattern fromPattern = Pattern.compile("(.*?\\sfrom\\s)(.*+)", Pattern.CASE_INSENSITIVE|Pattern.DOTALL);


  public GenericJdbcDatabaseAccessor() {
  }


  @Override
  public List<String> getColumnNames(Configuration conf) throws HiveJdbcDatabaseAccessException {
    Connection conn = null;
    PreparedStatement ps = null;
    ResultSet rs = null;

    try {
      initializeDatabaseConnection(conf);
      String query = JdbcStorageConfigManager.getOrigQueryToExecute(conf);
      String metadataQuery = getMetaDataQuery(query);
      LOGGER.debug("Query to execute is [{}]", metadataQuery);

      conn = dbcpDataSource.getConnection();
      ps = conn.prepareStatement(metadataQuery);
      rs = ps.executeQuery();

      ResultSetMetaData metadata = rs.getMetaData();
      int numColumns = metadata.getColumnCount();
      List<String> columnNames = new ArrayList<String>(numColumns);
      for (int i = 0; i < numColumns; i++) {
        columnNames.add(metadata.getColumnName(i + 1));
      }

      return columnNames;
    }
    catch (Exception e) {
      LOGGER.error("Error while trying to get column names.", e);
      throw new HiveJdbcDatabaseAccessException("Error while trying to get column names: " + e.getMessage(), e);
    }
    finally {
      cleanupResources(conn, ps, rs);
    }

  }


  protected String getMetaDataQuery(String sql) {
    return addLimitToQuery(sql, 1);
  }

  @Override
  public int getTotalNumberOfRecords(Configuration conf) throws HiveJdbcDatabaseAccessException {
    Connection conn = null;
    PreparedStatement ps = null;
    ResultSet rs = null;

    try {
      initializeDatabaseConnection(conf);
      String sql = JdbcStorageConfigManager.getQueryToExecute(conf);
      String countQuery = "SELECT COUNT(*) FROM (" + sql + ") tmptable";
      LOGGER.info("Query to execute is [{}]", countQuery);

      conn = dbcpDataSource.getConnection();
      ps = conn.prepareStatement(countQuery);
      rs = ps.executeQuery();
      if (rs.next()) {
        return rs.getInt(1);
      }
      else {
        LOGGER.warn("The count query did not return any results.", countQuery);
        throw new HiveJdbcDatabaseAccessException("Count query did not return any results.");
      }
    }
    catch (HiveJdbcDatabaseAccessException he) {
      throw he;
    }
    catch (Exception e) {
      LOGGER.error("Caught exception while trying to get the number of records", e);
      throw new HiveJdbcDatabaseAccessException(e);
    }
    finally {
      cleanupResources(conn, ps, rs);
    }
  }


  @Override
  public JdbcRecordIterator
    getRecordIterator(Configuration conf, String partitionColumn, String lowerBound, String upperBound, int limit, int
          offset) throws
          HiveJdbcDatabaseAccessException {

    Connection conn = null;
    PreparedStatement ps = null;
    ResultSet rs = null;

    try {
      initializeDatabaseConnection(conf);
      String tableName = conf.get(JDBC_TABLE);
      String sql = JdbcStorageConfigManager.getQueryToExecute(conf);
      String partitionQuery;
      if (partitionColumn != null) {
        partitionQuery = addBoundaryToQuery(tableName, sql, partitionColumn, lowerBound, upperBound);
      } else {
        partitionQuery = addLimitAndOffsetToQuery(sql, limit, offset);
      }
      LOGGER.info("Query to execute is [{}]", partitionQuery);

      conn = dbcpDataSource.getConnection();
      ps = conn.prepareStatement(partitionQuery, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
      ps.setFetchSize(getFetchSize(conf));
      rs = ps.executeQuery();

      return new JdbcRecordIterator(conn, ps, rs, conf);
    }
    catch (Exception e) {
      LOGGER.error("Caught exception while trying to execute query", e);
      cleanupResources(conn, ps, rs);
      throw new HiveJdbcDatabaseAccessException("Caught exception while trying to execute query:" + e.getMessage(), e);
    }
  }


  /**
   * Uses generic JDBC escape functions to add a limit and offset clause to a query string
   *
   * @param sql
   * @param limit
   * @param offset
   * @return
   */
  protected String addLimitAndOffsetToQuery(String sql, int limit, int offset) {
    if (offset == 0) {
      return addLimitToQuery(sql, limit);
    } else if (limit != -1) {
      return sql + " {LIMIT " + limit + " OFFSET " + offset + "}";
    } else {
      return sql + " {OFFSET " + offset + "}";
    }
  }

  /*
   * Uses generic JDBC escape functions to add a limit clause to a query string
   */
  protected String addLimitToQuery(String sql, int limit) {
    if (limit == -1) {
      return sql;
    }
    return sql + " {LIMIT " + limit + "}";
  }

  protected String addBoundaryToQuery(String tableName, String sql, String partitionColumn, String lowerBound,
          String upperBound) {
    String boundaryQuery;
    if (tableName != null) {
      boundaryQuery = "SELECT * FROM " + tableName + " WHERE ";
    } else {
      boundaryQuery = "SELECT * FROM (" + sql + ") tmptable WHERE ";
    }
    if (lowerBound != null) {
      boundaryQuery += quote() + partitionColumn + quote() + " >= " + lowerBound;
    }
    if (upperBound != null) {
      if (lowerBound != null) {
        boundaryQuery += " AND ";
      }
      boundaryQuery += quote() + partitionColumn + quote() + " < " + upperBound;
    }
    if (lowerBound == null && upperBound != null) {
      boundaryQuery += " OR " + quote() + partitionColumn + quote() + " IS NULL";
    }
    String result;
    if (tableName != null) {
      // Looking for table name in from clause, replace with the boundary query
      // TODO consolidate this
      // Currently only use simple string match, this should be improved by looking
      // for only table name in from clause
      String tableString = null;
      Matcher m = fromPattern.matcher(sql);
      Preconditions.checkArgument(m.matches());
      String queryBeforeFrom = m.group(1);
      String queryAfterFrom = " " + m.group(2) + " ";

      Character[] possibleDelimits = new Character[] {'`', '\"', ' '};
      for (Character possibleDelimit : possibleDelimits) {
        if (queryAfterFrom.contains(possibleDelimit + tableName + possibleDelimit)) {
          tableString = possibleDelimit + tableName + possibleDelimit;
          break;
        }
      }
      if (tableString == null) {
        throw new RuntimeException("Cannot find " + tableName + " in sql query " + sql);
      }
      result = queryBeforeFrom + queryAfterFrom.replace(tableString, " (" + boundaryQuery + ") " + tableName + " ");
    } else {
      result = boundaryQuery;
    }
    return result;
  }

  protected void cleanupResources(Connection conn, PreparedStatement ps, ResultSet rs) {
    try {
      if (rs != null) {
        rs.close();
      }
    } catch (SQLException e) {
      LOGGER.warn("Caught exception during resultset cleanup.", e);
    }

    try {
      if (ps != null) {
        ps.close();
      }
    } catch (SQLException e) {
      LOGGER.warn("Caught exception during statement cleanup.", e);
    }

    try {
      if (conn != null) {
        conn.close();
      }
    } catch (SQLException e) {
      LOGGER.warn("Caught exception during connection cleanup.", e);
    }
  }

  protected void initializeDatabaseConnection(Configuration conf) throws Exception {
    if (dbcpDataSource == null) {
      synchronized (this) {
        if (dbcpDataSource == null) {
          Properties props = getConnectionPoolProperties(conf);
          dbcpDataSource = createDataSource(props);
        }
      }
    }
  }
  private static Collection<String> parseList(String value, char delimiter) {
    StringTokenizer tokenizer = new StringTokenizer(value, Character.toString(delimiter));
    ArrayList tokens = new ArrayList(tokenizer.countTokens());

    while(tokenizer.hasMoreTokens()) {
      tokens.add(tokenizer.nextToken());
    }

    return tokens;
  }
  private static Properties getProperties(String propText) throws Exception {
    Properties p = new Properties();
    if (propText != null) {
      p.load(new ByteArrayInputStream(propText.replace(';', '\n').getBytes(StandardCharsets.ISO_8859_1)));
    }

    return p;
  }


  public static BasicDataSource createDataSource(Properties properties) throws Exception {
    BasicDataSource dataSource = new BasicDataSource();
    String value = null;
    value = properties.getProperty("defaultAutoCommit");
    if (value != null) {
      dataSource.setDefaultAutoCommit(Boolean.valueOf(value));
    }

    value = properties.getProperty("defaultReadOnly");
    if (value != null) {
      dataSource.setDefaultReadOnly(Boolean.valueOf(value));
    }

    value = properties.getProperty("defaultTransactionIsolation");
    if (value != null) {
//      int level = true;
      int level = 1;
      if ("NONE".equalsIgnoreCase(value)) {
        level = 0;
      } else if ("READ_COMMITTED".equalsIgnoreCase(value)) {
        level = 2;
      } else if ("READ_UNCOMMITTED".equalsIgnoreCase(value)) {
        level = 1;
      } else if ("REPEATABLE_READ".equalsIgnoreCase(value)) {
        level = 4;
      } else if ("SERIALIZABLE".equalsIgnoreCase(value)) {
        level = 8;
      } else {
        try {
          level = Integer.parseInt(value);
        } catch (NumberFormatException var6) {
          System.err.println("Could not parse defaultTransactionIsolation: " + value);
          System.err.println("WARNING: defaultTransactionIsolation not set");
          System.err.println("using default value of database driver");
          level = -1;
        }
      }
      dataSource.setDefaultTransactionIsolation(level);
    }

    value = properties.getProperty("defaultCatalog");
    if (value != null) {
      dataSource.setDefaultCatalog(value);
    }

    value = properties.getProperty("defaultSchema");
    if (value != null) {
      dataSource.setDefaultSchema(value);
    }

    value = properties.getProperty("cacheState");
    if (value != null) {
      dataSource.setCacheState(Boolean.valueOf(value));
    }

    value = properties.getProperty("driverClassName");
    if (value != null) {
      dataSource.setDriverClassName(value);
    }

    value = properties.getProperty("lifo");
    if (value != null) {
      dataSource.setLifo(Boolean.valueOf(value));
    }

    value = properties.getProperty("maxTotal");
    if (value != null) {
      dataSource.setMaxTotal(Integer.parseInt(value));
    }

    value = properties.getProperty("maxIdle");
    if (value != null) {
      dataSource.setMaxIdle(Integer.parseInt(value));
    }

    value = properties.getProperty("minIdle");
    if (value != null) {
      dataSource.setMinIdle(Integer.parseInt(value));
    }

    value = properties.getProperty("initialSize");
    if (value != null) {
      dataSource.setInitialSize(Integer.parseInt(value));
    }

    value = properties.getProperty("maxWaitMillis");
    if (value != null) {
      dataSource.setMaxWaitMillis(Long.parseLong(value));
    }

    value = properties.getProperty("testOnCreate");
    if (value != null) {
      dataSource.setTestOnCreate(Boolean.valueOf(value));
    }

    value = properties.getProperty("testOnBorrow");
    if (value != null) {
      dataSource.setTestOnBorrow(Boolean.valueOf(value));
    }

    value = properties.getProperty("testOnReturn");
    if (value != null) {
      dataSource.setTestOnReturn(Boolean.valueOf(value));
    }

    value = properties.getProperty("timeBetweenEvictionRunsMillis");
    if (value != null) {
      dataSource.setTimeBetweenEvictionRunsMillis(Long.parseLong(value));
    }

    value = properties.getProperty("numTestsPerEvictionRun");
    if (value != null) {
      dataSource.setNumTestsPerEvictionRun(Integer.parseInt(value));
    }

    value = properties.getProperty("minEvictableIdleTimeMillis");
    if (value != null) {
      dataSource.setMinEvictableIdleTimeMillis(Long.parseLong(value));
    }

    value = properties.getProperty("softMinEvictableIdleTimeMillis");
    if (value != null) {
      dataSource.setSoftMinEvictableIdleTimeMillis(Long.parseLong(value));
    }

    value = properties.getProperty("evictionPolicyClassName");
    if (value != null) {
      dataSource.setEvictionPolicyClassName(value);
    }

    value = properties.getProperty("testWhileIdle");
    if (value != null) {
      dataSource.setTestWhileIdle(Boolean.valueOf(value));
    }

    value = properties.getProperty("password");
    if (value != null) {
      dataSource.setPassword(value);
    }

    value = properties.getProperty("url");
    if (value != null) {
      dataSource.setUrl(value);
    }

    value = properties.getProperty("username");
    if (value != null) {
      dataSource.setUsername(value);
    }

    value = properties.getProperty("validationQuery");
    if (value != null) {
      dataSource.setValidationQuery(value);
    }

    value = properties.getProperty("validationQueryTimeout");
    if (value != null) {
      dataSource.setValidationQueryTimeout(Integer.parseInt(value));
    }

    value = properties.getProperty("accessToUnderlyingConnectionAllowed");
    if (value != null) {
      dataSource.setAccessToUnderlyingConnectionAllowed(Boolean.valueOf(value));
    }

    value = properties.getProperty("removeAbandonedOnBorrow");
    if (value != null) {
      dataSource.setRemoveAbandonedOnBorrow(Boolean.valueOf(value));
    }

    value = properties.getProperty("removeAbandonedOnMaintenance");
    if (value != null) {
      dataSource.setRemoveAbandonedOnMaintenance(Boolean.valueOf(value));
    }

    value = properties.getProperty("removeAbandonedTimeout");
    if (value != null) {
      dataSource.setRemoveAbandonedTimeout(Integer.parseInt(value));
    }

    value = properties.getProperty("logAbandoned");
    if (value != null) {
      dataSource.setLogAbandoned(Boolean.valueOf(value));
    }

    value = properties.getProperty("abandonedUsageTracking");
    if (value != null) {
      dataSource.setAbandonedUsageTracking(Boolean.valueOf(value));
    }

    value = properties.getProperty("poolPreparedStatements");
    if (value != null) {
      dataSource.setPoolPreparedStatements(Boolean.valueOf(value));
    }

    value = properties.getProperty("maxOpenPreparedStatements");
    if (value != null) {
      dataSource.setMaxOpenPreparedStatements(Integer.parseInt(value));
    }

    value = properties.getProperty("connectionInitSqls");
    if (value != null) {
      dataSource.setConnectionInitSqls(parseList(value, ';'));
    }

    value = properties.getProperty("connectionProperties");
    if (value != null) {
      Properties p = getProperties(value);
      Enumeration e = p.propertyNames();

      while(e.hasMoreElements()) {
        String propertyName = (String)e.nextElement();
        dataSource.addConnectionProperty(propertyName, p.getProperty(propertyName));
      }
    }

    value = properties.getProperty("maxConnLifetimeMillis");
    if (value != null) {
      dataSource.setMaxConnLifetimeMillis(Long.parseLong(value));
    }

    value = properties.getProperty("logExpiredConnections");
    if (value != null) {
      dataSource.setLogExpiredConnections(Boolean.valueOf(value));
    }

    value = properties.getProperty("jmxName");
    if (value != null) {
      dataSource.setJmxName(value);
    }

    value = properties.getProperty("enableAutoCommitOnReturn");
    if (value != null) {
      dataSource.setAutoCommitOnReturn(Boolean.valueOf(value));
    }

    value = properties.getProperty("rollbackOnReturn");
    if (value != null) {
      dataSource.setRollbackOnReturn(Boolean.valueOf(value));
    }

    value = properties.getProperty("defaultQueryTimeout");
    if (value != null) {
      dataSource.setDefaultQueryTimeout(Integer.valueOf(value));
    }

    value = properties.getProperty("fastFailValidation");
    if (value != null) {
      dataSource.setFastFailValidation(Boolean.valueOf(value));
    }

    value = properties.getProperty("disconnectionSqlCodes");
    if (value != null) {
      dataSource.setDisconnectionSqlCodes(parseList(value, ','));
    }

    value = properties.getProperty("connectionFactoryClassName");
    if (value != null) {
      dataSource.setConnectionFactoryClassName(value);
    }

    if (dataSource.getInitialSize() > 0) {
      dataSource.getLogWriter();
    }

    return dataSource;
  }


  private String getFromProperties(Properties dbProperties, String key) {
    return dbProperties.getProperty(key.replaceFirst(DBCP_CONFIG_PREFIX + "\\.", ""));
  }

  protected Properties getConnectionPoolProperties(Configuration conf) throws Exception {
    // Create the default properties object
    Properties dbProperties = getDefaultDBCPProperties();

    // override with user defined properties
    Map<String, String> userProperties = conf.getValByRegex(DBCP_CONFIG_PREFIX + "\\.*");
    if ((userProperties != null) && (!userProperties.isEmpty())) {
      for (Entry<String, String> entry : userProperties.entrySet()) {
        dbProperties.put(entry.getKey().replaceFirst(DBCP_CONFIG_PREFIX + "\\.", ""), entry.getValue());
      }
    }

    // handle password
    String passwd = getFromProperties(dbProperties, JdbcStorageConfigManager.CONFIG_PWD);
    if (passwd == null) {
      String keystore = getFromProperties(dbProperties, JdbcStorageConfigManager.CONFIG_PWD_KEYSTORE);
      String key = getFromProperties(dbProperties, JdbcStorageConfigManager.CONFIG_PWD_KEY);
      passwd = Utilies.getPasswdFromKeystore(keystore, key);
    }

    if (passwd != null) {
      dbProperties.put(JdbcStorageConfigManager.CONFIG_PWD.replaceFirst(DBCP_CONFIG_PREFIX + "\\.", ""), passwd);
    }

    // essential properties that shouldn't be overridden by users
    dbProperties.put("url", conf.get(JdbcStorageConfig.JDBC_URL.getPropertyName()));
    dbProperties.put("driverClassName", conf.get(JdbcStorageConfig.JDBC_DRIVER_CLASS.getPropertyName()));
    dbProperties.put("type", "javax.sql.DataSource");
    return dbProperties;
  }


  protected Properties getDefaultDBCPProperties() {
    Properties props = new Properties();
    props.put("initialSize", "1");
    props.put("maxActive", "3");
    props.put("maxIdle", "0");
    props.put("maxWait", "10000");
    props.put("timeBetweenEvictionRunsMillis", "30000");
    return props;
  }


  protected int getFetchSize(Configuration conf) {
    return conf.getInt(JdbcStorageConfig.JDBC_FETCH_SIZE.getPropertyName(), DEFAULT_FETCH_SIZE);
  }

  @Override
  public Pair<String, String> getBounds(Configuration conf, String partitionColumn, boolean retrieveMin, boolean
          retrieveMax) throws HiveJdbcDatabaseAccessException {
    Connection conn = null;
    PreparedStatement ps = null;
    ResultSet rs = null;

    try {
      Preconditions.checkArgument(retrieveMin || retrieveMax);
      initializeDatabaseConnection(conf);
      String sql = JdbcStorageConfigManager.getOrigQueryToExecute(conf);
      String minClause = "MIN(" + quote() + partitionColumn  + quote() + ")";
      String maxClause = "MAX(" + quote() + partitionColumn  + quote() + ")";
      String countQuery = "SELECT ";
      if (retrieveMin) {
        countQuery += minClause;
      }
      if (retrieveMax) {
        if (retrieveMin) {
          countQuery += ",";
        }
        countQuery += maxClause;
      }
      countQuery += " FROM (" + sql + ") tmptable " + "WHERE " + quote() + partitionColumn + quote() + " IS NOT NULL";

      LOGGER.debug("MIN/MAX Query to execute is [{}]", countQuery);

      conn = dbcpDataSource.getConnection();
      ps = conn.prepareStatement(countQuery);
      rs = ps.executeQuery();
      String lower = null, upper = null;
      int pos = 1;
      if (rs.next()) {
        if (retrieveMin) {
          lower = rs.getString(pos);
          pos++;
        }
        if (retrieveMax) {
          upper = rs.getString(pos);
        }
        return new ImmutablePair<>(lower, upper);
      }
      else {
        LOGGER.warn("The count query did not return any results.", countQuery);
        throw new HiveJdbcDatabaseAccessException("MIN/MAX query did not return any results.");
      }
    }
    catch (HiveJdbcDatabaseAccessException he) {
      throw he;
    }
    catch (Exception e) {
      LOGGER.error("Caught exception while trying to get MIN/MAX of " + partitionColumn, e);
      throw new HiveJdbcDatabaseAccessException(e);
    }
    finally {
      cleanupResources(conn, ps, rs);
    }
  }

  private String quote() {
    if (needColumnQuote()) {
      return "\"";
    } else {
      return "";
    }
  }
  @Override
  public boolean needColumnQuote() {
    return true;
  }
}
