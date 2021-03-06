package org.apache.hive.storage.jdbc.format;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapred.*;
import org.apache.hive.storage.jdbc.records.JdbcWritable;
import org.apache.hive.storage.jdbc.split.JdbcInputSplit;
import org.apache.hive.storage.jdbc.records.JdbcRecordReader;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;

import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hive.storage.jdbc.conf.config.Constants;
import org.apache.hive.storage.jdbc.spitter.type.IntervalSplitter;
import org.apache.hive.storage.jdbc.spitter.type.IntervalSplitterFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hive.storage.jdbc.dao.dataBase.DatabaseAccessor;
import org.apache.hive.storage.jdbc.dao.dataBase.DatabaseAccessorFactory;

import java.io.IOException;
import java.util.List;

public class JdbcInputFormat extends HiveInputFormat<LongWritable, JdbcWritable> implements InputFormat<LongWritable, JdbcWritable> {

  private static final Logger LOGGER = LoggerFactory.getLogger(JdbcInputFormat.class);
  private DatabaseAccessor dbAccessor = null;


  /**
   * {@inheritDoc}
   */
  @Override
  public RecordReader<LongWritable, JdbcWritable>
    getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException {
    LOGGER.warn("get RecordReader");
    if (!(split instanceof JdbcInputSplit)) {
      throw new RuntimeException("Incompatible split type " + split.getClass().getName() + ".");
    }
    LOGGER.warn("get return RecordReader");
    return new JdbcRecordReader(job, (JdbcInputSplit) split);
  }


  /**
   * {@inheritDoc}
   */
  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    try {

      String partitionColumn = job.get(Constants.JDBC_PARTITION_COLUMN);
      int numPartitions = job.getInt(Constants.JDBC_NUM_PARTITIONS, -1);
      String lowerBound = job.get(Constants.JDBC_LOW_BOUND);
      String upperBound = job.get(Constants.JDBC_UPPER_BOUND);

      InputSplit[] splits;

      if (!job.getBoolean(Constants.JDBC_SPLIT_QUERY, true) || numPartitions <= 1) {
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
