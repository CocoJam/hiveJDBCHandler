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
package org.apache.hive.storage.jdbc.records;

import org.apache.hadoop.io.*;
//import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hive.storage.jdbc.split.JdbcInputSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hive.storage.jdbc.dao.dataBase.DatabaseAccessor;
import org.apache.hive.storage.jdbc.dao.dataBase.DatabaseAccessorFactory;
import org.apache.hive.storage.jdbc.dao.dataBase.JdbcRecordIterator;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

public class JdbcRecordReader implements RecordReader<LongWritable, JdbcWritable> {

  private static final Logger LOGGER = LoggerFactory.getLogger(JdbcRecordReader.class);
  private DatabaseAccessor dbAccessor = null;
  private JdbcRecordIterator iterator = null;
  private JdbcInputSplit split = null;
  private JobConf conf = null;
  private int pos = 0;


  public JdbcRecordReader(JobConf conf, JdbcInputSplit split) {
    LOGGER.trace("Initializing JdbcRecordReader");
    this.split = split;
    this.conf = conf;
  }


  @Override
  public boolean next(LongWritable key, JdbcWritable value) throws IOException {
    try {
      LOGGER.warn("JdbcRecordReader.next called");
      if (dbAccessor == null) {
        dbAccessor = DatabaseAccessorFactory.getAccessor(conf);
        iterator = dbAccessor.getRecordIterator(conf, split.getPartitionColumn(), split.getLowerBound(), split
                        .getUpperBound(), split.getLimit(), split.getOffset());
      }

      if (iterator.hasNext()) {
        LOGGER.warn("JdbcRecordReader has more records to read.");
        key.set(pos);
        pos++;
        Map<String, Object> record = iterator.next();
        if ((record != null) && (!record.isEmpty())) {
          for (Entry<String, Object> entry : record.entrySet()) {
            value.put(new Text(entry.getKey()),
                entry.getValue() == null ? NullWritable.get() : new ObjectWritable(entry.getValue()));
          }
          return true;
        }
        else {
          LOGGER.warn("JdbcRecordReader got null record.");
          return false;
        }
      }
      else {
        LOGGER.warn("JdbcRecordReader has no more records to read.");
        return false;
      }
    }
    catch (Exception e) {
      throw new IOException(e);
    }
  }


  @Override
  public LongWritable createKey() {
    return new LongWritable();
  }


  @Override
  public JdbcWritable createValue() {
    return new JdbcWritable();
  }


  @Override
  public long getPos() throws IOException {
    return pos;
  }


  @Override
  public void close() throws IOException {
    if (iterator != null) {
      iterator.close();
    }
    if (dbAccessor != null) {
      dbAccessor = null;
    }
  }


  @Override
  public float getProgress() throws IOException {
    if (split == null) {
      return 0;
    }
    else {
      return split.getLength() > 0 ? pos / (float) split.getLength() : 1.0f;
    }
  }
}
