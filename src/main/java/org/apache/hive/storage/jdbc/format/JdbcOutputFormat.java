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
package org.apache.hive.storage.jdbc.format;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.util.Progressable;
import org.apache.hive.storage.jdbc.records.JdbcWritable;
import org.apache.hive.storage.jdbc.records.JdbcWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

import static org.apache.hive.storage.jdbc.conf.config.Constants.*;

public class JdbcOutputFormat extends FileOutputFormat<NullWritable, JdbcWritable> implements OutputFormat<NullWritable, JdbcWritable>,
                                         HiveOutputFormat<NullWritable, JdbcWritable> {
  private static final Logger LOGGER = LoggerFactory.getLogger(JdbcOutputFormat.class);
  /**
   * {@inheritDoc}
   */
  // TODO: Implement giving hdfs file sink.

  @Override
  public RecordWriter getHiveRecordWriter(JobConf jc,
      Path finalOutPath,
      Class<? extends Writable> valueClass,
      boolean isCompressed,
      Properties tableProperties,
      Progressable progress) throws IOException {
      LOGGER.warn("get HIVE Record Writer");
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
    LOGGER.warn("get HIVE Record Writer Returning mapred RecordWriter");

    return new JdbcWriter(jc, fileSystem,dbTable,file , progressable );
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void checkOutputSpecs(FileSystem ignored, JobConf job) throws IOException {
    // do nothing
    LOGGER.warn("checkOutputSpecs");
  }

}
