package org.apache.hive.storage.jdbc;

import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;


public class JdbcWriter implements FileSinkOperator.RecordWriter, RecordWriter<NullWritable, MapWritable> {
    String dBTable;

    @Override
    public void write(Writable w) throws IOException {

    }

    @Override
    public void close(boolean abort) throws IOException {

    }

    @Override
    public void write(NullWritable nullWritable, MapWritable mapWritable) throws IOException {

    }

    @Override
    public void close(Reporter reporter) throws IOException {

    }
}
