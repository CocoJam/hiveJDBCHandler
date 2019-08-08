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
package org.apache.hive.storage.jdbc;

import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.Constants;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.metadata.JarUtils;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Progressable;
import org.apache.hive.storage.jdbc.dao.DatabaseAccessor;
import org.apache.hive.storage.jdbc.dao.DatabaseAccessorFactory;
import org.apache.hive.storage.jdbc.spitter.IntervalSplitter;
import org.apache.hive.storage.jdbc.spitter.IntervalSplitterFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hive.storage.jdbc.conf.JdbcStorageConfigManager;
import java.io.IOException;
import java.lang.IllegalArgumentException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.hive.storage.jdbc.conf.Constants.JDBC_TABLE;

public class JdbcTextHandler implements HiveStorageHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(JdbcTextHandler.class);
  private Configuration conf;


  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }


  @Override
  public Configuration getConf() {
    return this.conf;
  }


  @SuppressWarnings("rawtypes")
  @Override
  public Class<? extends InputFormat> getInputFormatClass() {

    return (new HiveInputFormat<LongWritable, JdbcWritable> (){
      private DatabaseAccessor dbAccessor = null;

      /**
       * {@inheritDoc}
       */
      @Override
      public RecordReader<LongWritable, JdbcWritable>
      getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException {

        if (!(split instanceof JdbcInputSplit)) {
          throw new RuntimeException("Incompatible split type " + split.getClass().getName() + ".");
        }

        return new JdbcRecordReader(job, (JdbcInputSplit) split);
      }


      /**
       * {@inheritDoc}
       */
      @Override
      public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
        try {

          String partitionColumn = job.get(org.apache.hive.storage.jdbc.conf.Constants.JDBC_PARTITION_COLUMN);
          int numPartitions = job.getInt(org.apache.hive.storage.jdbc.conf.Constants.JDBC_NUM_PARTITIONS, -1);
          String lowerBound = job.get(org.apache.hive.storage.jdbc.conf.Constants.JDBC_LOW_BOUND);
          String upperBound = job.get(org.apache.hive.storage.jdbc.conf.Constants.JDBC_UPPER_BOUND);

          InputSplit[] splits;

          if (!job.getBoolean(org.apache.hive.storage.jdbc.conf.Constants.JDBC_SPLIT_QUERY, true) || numPartitions <= 1) {
            // We will not split this query if:
            // 1. hive.sql.query.split is set to false (either manually or automatically by calcite
            // 2. numPartitions == 1
            splits = new InputSplit[1];
            splits[0] = new JdbcInputSplit(FileInputFormat.getInputPaths(job)[0]);
            LOGGER.info("Creating 1 input split " + splits[0]);
            return splits;
          }

          dbAccessor = DatabaseAccessorFactory.getAccessor(job);
          Path[] tablePaths = FileInputFormat.getInputPaths(job);

          // We will split this query into n splits
          LOGGER.debug("Creating {} input splits", numPartitions);

          if (partitionColumn != null) {
            List<String> columnNames = dbAccessor.getColumnNames(job);
            if (!columnNames.contains(partitionColumn)) {
              throw new IOException("Cannot find partitionColumn:" + partitionColumn + " in " + columnNames);
            }
            List<TypeInfo> hiveColumnTypesList = TypeInfoUtils.getTypeInfosFromTypeString(job.get(serdeConstants.LIST_COLUMN_TYPES));
            TypeInfo typeInfo = hiveColumnTypesList.get(columnNames.indexOf(partitionColumn));
            if (!(typeInfo instanceof PrimitiveTypeInfo)) {
              throw new IOException(partitionColumn + " is a complex type, only primitive type can be a partition column");
            }
            if (lowerBound == null || upperBound == null) {
              Pair<String, String> boundary = dbAccessor.getBounds(job, partitionColumn, lowerBound == null,
                      upperBound == null);
              if (lowerBound == null) {
                lowerBound = boundary.getLeft();
              }
              if (upperBound == null) {
                upperBound = boundary.getRight();
              }
            }
            if (lowerBound == null) {
              throw new IOException("lowerBound of " + partitionColumn + " cannot be null");
            }
            if (upperBound == null) {
              throw new IOException("upperBound of " + partitionColumn + " cannot be null");
            }
            IntervalSplitter intervalSplitter = IntervalSplitterFactory.newIntervalSpitter(typeInfo);
            List<MutablePair<String, String>> intervals = intervalSplitter.getIntervals(lowerBound, upperBound, numPartitions,
                    typeInfo);
            if (intervals.size()<=1) {
              LOGGER.debug("Creating 1 input splits");
              splits = new InputSplit[1];
              splits[0] = new JdbcInputSplit(FileInputFormat.getInputPaths(job)[0]);
              return splits;
            }
            intervals.get(0).setLeft(null);
            intervals.get(intervals.size()-1).setRight(null);
            splits = new InputSplit[intervals.size()];
            for (int i = 0; i < intervals.size(); i++) {
              splits[i] = new JdbcInputSplit(partitionColumn, intervals.get(i).getLeft(), intervals.get(i).getRight(), tablePaths[0]);
            }
          } else {
            int numRecords = dbAccessor.getTotalNumberOfRecords(job);

            if (numRecords < numPartitions) {
              numPartitions = numRecords;
            }

            int numRecordsPerSplit = numRecords / numPartitions;
            int numSplitsWithExtraRecords = numRecords % numPartitions;

            LOGGER.debug("Num records = {}", numRecords);
            splits = new InputSplit[numPartitions];

            int offset = 0;
            for (int i = 0; i < numPartitions; i++) {
              int numRecordsInThisSplit = numRecordsPerSplit;
              if (i < numSplitsWithExtraRecords) {
                numRecordsInThisSplit++;
              }

              splits[i] = new JdbcInputSplit(numRecordsInThisSplit, offset, tablePaths[0]);
              offset += numRecordsInThisSplit;
            }
          }

          dbAccessor = null;
          LOGGER.info("Num input splits created {}", splits.length);
          for (InputSplit split : splits) {
            LOGGER.info("split:" + split.toString());
          }
          return splits;
        }
        catch (Exception e) {
          LOGGER.error("Error while splitting input data.", e);
          throw new IOException(e);
        }
      }


      /**
       * For testing purposes only
       *
       * @param dbAccessor
       *            DatabaseAccessor object
       */
      public void setDbAccessor(DatabaseAccessor dbAccessor) {
        this.dbAccessor = dbAccessor;
      }
    }).getClass();
//    return JdbcInputFormat.class;
  }


  @SuppressWarnings("rawtypes")
  @Override
  public Class<? extends OutputFormat> getOutputFormatClass() {
    return JdbcOutputFormat.class;
  }


  @Override
  public Class<? extends AbstractSerDe> getSerDeClass() {
    return JdbcSerDe.class;
  }


  @Override
  public HiveMetaHook getMetaHook() {
    return null;
  }

  @Override
  public void configureInputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
    try {
      LOGGER.debug("Adding properties to input job conf");
      Properties properties = tableDesc.getProperties();
      JdbcStorageConfigManager.copyConfigurationToJob(properties, jobProperties);
    } catch (Exception e) {
      throw new IllegalArgumentException(e);
    }
  }

  @Override
  public void configureInputJobCredentials(TableDesc tableDesc, Map<String, String> jobSecrets) {
    try {
      LOGGER.debug("Adding secrets to input job conf");
      Properties properties = tableDesc.getProperties();
      JdbcStorageConfigManager.copySecretsToJob(properties, jobSecrets);
    } catch (Exception e) {
      throw new IllegalArgumentException(e);
    }
  }

  @Override
  public void configureTableJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
    try {
      LOGGER.debug("Adding properties to input job conf");
      Properties properties = tableDesc.getProperties();
      JdbcStorageConfigManager.copyConfigurationToJob(properties, jobProperties);
    } catch (Exception e) {
      throw new IllegalArgumentException(e);
    }
  }

  @Override
  public void configureOutputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
    try {
      LOGGER.debug("Adding secrets to input job conf");
      Properties properties = tableDesc.getProperties();
      JdbcStorageConfigManager.copySecretsToJob(properties, jobProperties);
    } catch (Exception e) {
      throw new IllegalArgumentException(e);
    }
  }

  @Override
  public HiveAuthorizationProvider getAuthorizationProvider() throws HiveException {
    return null;
  }

  @Override
  public void configureJobConf(TableDesc tableDesc, JobConf jobConf) {

    List<Class<?>> classesToLoad = new ArrayList<>();
    classesToLoad.add(org.apache.hive.storage.jdbc.JdbcInputSplit.class);
    classesToLoad.add(org.apache.commons.dbcp2.BasicDataSourceFactory.class);
    classesToLoad.add(org.apache.commons.pool2.impl.GenericObjectPool.class);
    // Adding mysql jdbc driver if exists
    try {
      classesToLoad.add(Class.forName("com.mysql.jdbc.Driver"));
    } catch (Exception e) {
    }
    // Adding postgres jdbc driver if exists
    try {
      classesToLoad.add(Class.forName("org.postgresql.Driver"));
    } catch (Exception e) {
    } // Adding oracle jdbc driver if exists
    try {
      classesToLoad.add(Class.forName("oracle.jdbc.OracleDriver"));
    } catch (Exception e) {
    } // Adding mssql jdbc driver if exists
    try {
      classesToLoad.add(Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver"));
    } catch (Exception e) {
    }
    try {
      classesToLoad.add(Class.forName("com.ibm.db2.jcc.DB2Driver"));
    } catch (Exception e) {
    } // Adding db2 jdbc driver if exists
    try {
      JarUtils.addDependencyJars(conf, classesToLoad);
    } catch (IOException e) {
      LOGGER.error("Could not add necessary JDBC storage handler dependencies to classpath", e);
    }
  }

  @Override
  public String toString() {
    return Constants.JDBC_HIVE_STORAGE_HANDLER_ID;
  }


  public class JdbcInputFormat extends HiveInputFormat<LongWritable, JdbcWritable> {

    private DatabaseAccessor dbAccessor = null;


    /**
     * {@inheritDoc}
     */
    @Override
    public RecordReader<LongWritable, JdbcWritable>
    getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException {

      if (!(split instanceof JdbcInputSplit)) {
        throw new RuntimeException("Incompatible split type " + split.getClass().getName() + ".");
      }

      return new JdbcRecordReader(job, (JdbcInputSplit) split);
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
      try {

        String partitionColumn = job.get(org.apache.hive.storage.jdbc.conf.Constants.JDBC_PARTITION_COLUMN);
        int numPartitions = job.getInt(org.apache.hive.storage.jdbc.conf.Constants.JDBC_NUM_PARTITIONS, -1);
        String lowerBound = job.get(org.apache.hive.storage.jdbc.conf.Constants.JDBC_LOW_BOUND);
        String upperBound = job.get(org.apache.hive.storage.jdbc.conf.Constants.JDBC_UPPER_BOUND);

        InputSplit[] splits;

        if (!job.getBoolean(org.apache.hive.storage.jdbc.conf.Constants.JDBC_SPLIT_QUERY, true) || numPartitions <= 1) {
          // We will not split this query if:
          // 1. hive.sql.query.split is set to false (either manually or automatically by calcite
          // 2. numPartitions == 1
          splits = new InputSplit[1];
          splits[0] = new JdbcInputSplit(FileInputFormat.getInputPaths(job)[0]);
          LOGGER.info("Creating 1 input split " + splits[0]);
          return splits;
        }

        dbAccessor = DatabaseAccessorFactory.getAccessor(job);
        Path[] tablePaths = FileInputFormat.getInputPaths(job);

        // We will split this query into n splits
        LOGGER.debug("Creating {} input splits", numPartitions);

        if (partitionColumn != null) {
          List<String> columnNames = dbAccessor.getColumnNames(job);
          if (!columnNames.contains(partitionColumn)) {
            throw new IOException("Cannot find partitionColumn:" + partitionColumn + " in " + columnNames);
          }
          List<TypeInfo> hiveColumnTypesList = TypeInfoUtils.getTypeInfosFromTypeString(job.get(serdeConstants.LIST_COLUMN_TYPES));
          TypeInfo typeInfo = hiveColumnTypesList.get(columnNames.indexOf(partitionColumn));
          if (!(typeInfo instanceof PrimitiveTypeInfo)) {
            throw new IOException(partitionColumn + " is a complex type, only primitive type can be a partition column");
          }
          if (lowerBound == null || upperBound == null) {
            Pair<String, String> boundary = dbAccessor.getBounds(job, partitionColumn, lowerBound == null,
                    upperBound == null);
            if (lowerBound == null) {
              lowerBound = boundary.getLeft();
            }
            if (upperBound == null) {
              upperBound = boundary.getRight();
            }
          }
          if (lowerBound == null) {
            throw new IOException("lowerBound of " + partitionColumn + " cannot be null");
          }
          if (upperBound == null) {
            throw new IOException("upperBound of " + partitionColumn + " cannot be null");
          }
          IntervalSplitter intervalSplitter = IntervalSplitterFactory.newIntervalSpitter(typeInfo);
          List<MutablePair<String, String>> intervals = intervalSplitter.getIntervals(lowerBound, upperBound, numPartitions,
                  typeInfo);
          if (intervals.size()<=1) {
            LOGGER.debug("Creating 1 input splits");
            splits = new InputSplit[1];
            splits[0] = new JdbcInputSplit(FileInputFormat.getInputPaths(job)[0]);
            return splits;
          }
          intervals.get(0).setLeft(null);
          intervals.get(intervals.size()-1).setRight(null);
          splits = new InputSplit[intervals.size()];
          for (int i = 0; i < intervals.size(); i++) {
            splits[i] = new JdbcInputSplit(partitionColumn, intervals.get(i).getLeft(), intervals.get(i).getRight(), tablePaths[0]);
          }
        } else {
          int numRecords = dbAccessor.getTotalNumberOfRecords(job);

          if (numRecords < numPartitions) {
            numPartitions = numRecords;
          }

          int numRecordsPerSplit = numRecords / numPartitions;
          int numSplitsWithExtraRecords = numRecords % numPartitions;

          LOGGER.debug("Num records = {}", numRecords);
          splits = new InputSplit[numPartitions];

          int offset = 0;
          for (int i = 0; i < numPartitions; i++) {
            int numRecordsInThisSplit = numRecordsPerSplit;
            if (i < numSplitsWithExtraRecords) {
              numRecordsInThisSplit++;
            }

            splits[i] = new JdbcInputSplit(numRecordsInThisSplit, offset, tablePaths[0]);
            offset += numRecordsInThisSplit;
          }
        }

        dbAccessor = null;
        LOGGER.info("Num input splits created {}", splits.length);
        for (InputSplit split : splits) {
          LOGGER.info("split:" + split.toString());
        }
        return splits;
      }
      catch (Exception e) {
        LOGGER.error("Error while splitting input data.", e);
        throw new IOException(e);
      }
    }


    /**
     * For testing purposes only
     *
     * @param dbAccessor
     *            DatabaseAccessor object
     */
    public void setDbAccessor(DatabaseAccessor dbAccessor) {
      this.dbAccessor = dbAccessor;
    }

  }

  public class JdbcOutputFormat extends FileOutputFormat<NullWritable, JdbcWritable> implements OutputFormat<NullWritable, JdbcWritable>,
          HiveOutputFormat<NullWritable, JdbcWritable> {

    /**
     * {@inheritDoc}
     */
    // TODO: Implement giving hdfs file sink.

    @Override
    public FileSinkOperator.RecordWriter getHiveRecordWriter(JobConf jc,
                                                             Path finalOutPath,
                                                             Class<? extends Writable> valueClass,
                                                             boolean isCompressed,
                                                             Properties tableProperties,
                                                             Progressable progress) throws IOException {
      FileSystem fs = finalOutPath.getFileSystem(jc);
      String dbTable = jc.get(JDBC_TABLE);
      return new JdbcWriter(jc,fs, dbTable, finalOutPath, progress);
//    throw new UnsupportedOperationException("Write operations are not allowed.");
    }


    /**
     * {@inheritDoc}
     * @return
     */


    @Override
    public org.apache.hadoop.mapred.RecordWriter<NullWritable, JdbcWritable> getRecordWriter(FileSystem fileSystem, JobConf jc, String s, Progressable progressable) throws IOException {
      String dbTable = jc.get(JDBC_TABLE);
      Path file = FileOutputFormat.getTaskOutputPath(jc, s);
      return new JdbcWriter(jc, fileSystem,dbTable,file , progressable );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void checkOutputSpecs(FileSystem ignored, JobConf job) throws IOException {
      // do nothing
    }

  }


}
