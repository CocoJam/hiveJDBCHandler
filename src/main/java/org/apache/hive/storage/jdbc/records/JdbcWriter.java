package org.apache.hive.storage.jdbc.records;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;


public class JdbcWriter implements org.apache.hadoop.mapred.lib.db.DBWritable,
        org.apache.hadoop.mapreduce.lib.db.DBWritable
//        FileSinkOperator.RecordWriter, RecordWriter<NullWritable, MapWritable>
{
    private String dBTable;
    private FileSystem fs;
    private JobConf jc;
    private Path finalPath;
    private Progressable progress;
    private final FSDataOutputStream outputStream;
    private final Map<Object, Object> offsets = new HashMap<>();
    protected DataOutputStream out;

    public JdbcWriter(JobConf jc, FileSystem fs, String dBTable, Path path, Progressable progress) throws IOException {
        System.out.println("Create jdbc Writer");
        this.jc = jc;
        this.fs= fs;
        this.dBTable = dBTable;
        this.finalPath = path;
        this.progress = progress;
        this.outputStream = this.fs.create(this.finalPath, this.progress);
    }


//    @Override
//    public void write(Writable w) throws IOException {
//        if (w instanceof Text) {
//            Text tr = (Text)w;
//            this.outputStream.write(tr.getBytes(), 0, tr.getLength());
//        } else {
//            BytesWritable bw = (BytesWritable)w;
//            this.outputStream.write(w.toString().getBytes(StandardCharsets.UTF_8));
//        }
//    }
//
//    @Override
//    public void close(boolean abort) throws IOException {
//        return;
//    }
//
//    @Override
//    public synchronized void write(NullWritable nullWritable, MapWritable mapWritable) throws IOException {
//        write(mapWritable);
//    }
//
//    @Override
//    public void close(Reporter reporter) throws IOException {
//        close(false);
//    }

    @Override
    public void write(PreparedStatement preparedStatement) throws SQLException {

    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {

    }
}
