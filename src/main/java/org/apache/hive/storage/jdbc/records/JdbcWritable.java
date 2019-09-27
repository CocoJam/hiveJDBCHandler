package org.apache.hive.storage.jdbc.records;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hive.storage.jdbc.format.JdbcInputFormat;
import org.apache.hive.storage.jdbc.util.HiveJdbcBridgeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.*;

public class JdbcWritable extends MapWritable implements Writable, org.apache.hadoop.mapred.lib.db.DBWritable,
        org.apache.hadoop.mapreduce.lib.db.DBWritable{

    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcWritable.class);
    private Object[] columnValues;
    private int[] intTypes;
    public HashMap<Writable,Writable> map ;
    
	public void set(int i, Object javaObject) {
        LOGGER.warn("set values by writable: "+i+" : "+javaObject.toString());
		columnValues[i] = javaObject;
    }
    
    public JdbcWritable() {}

    public JdbcWritable(Object[] columnValues, int[] columnTypes) {
        this.columnValues = columnValues;
//        this.columnTypes = columnTypes;
        map = new HashMap<>();
    }
    @Override
    public void write(DataOutput out) throws IOException {
        if (columnValues == null) {
            out.writeInt(-1);
            LOGGER.warn("writable write int fail due to columnValues");
            return;
        }
        // if (hiveColumnTypes == null) {
        //     out.writeInt(-1);
        //     LOGGER.warn("writable write int fail due to hiveColumnTypes");
        //     return;
        // }
        final Object[] values = this.columnValues;
        final int[] types = this.intTypes;
        assert (values.length == types.length);
        out.writeInt(values.length);
        for (int i = 0; i < values.length; i++) {
            HiveJdbcBridgeUtils.writeObject(values[i], types[i], out);
            LOGGER.warn("writable write values "+ values[i]);
        }

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int size = in.readInt();
        if (size == -1) {
            LOGGER.warn("writable read int -1");
            return;
        }
        if (columnValues == null) {
            this.columnValues = new Object[size];
            this.intTypes = new int[size];
        } else {
            clear();
        }
        for (int i = 0; i < size; i++) {
            int sqlType = in.readInt();
            this.intTypes[i]= sqlType;
            Object v = HiveJdbcBridgeUtils.readObject(in, sqlType);
            columnValues[i] = v;
            LOGGER.warn("writable readField "+ columnValues[i].toString());
        }
    }

    @Override
    public void write(PreparedStatement preparedStatement) throws SQLException {
        assert (columnValues != null);
        final Object[] r = this.columnValues;
        final int cols = r.length;
        for (int i = 0; i < cols; i++) {
            final Object col = r[i];
            if (col == null) {
                preparedStatement.setNull(i + 1,  this.intTypes[i]);
            } else {
                preparedStatement.setObject(i + 1, col);
            }
        }
        LOGGER.warn("writable write prepraedStatement"+ preparedStatement.toString());
    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        final ResultSetMetaData meta = resultSet.getMetaData();
        final int cols = meta.getColumnCount();
        final Object[] columns = new Object[cols];
        final int[] types = new int[cols];
        for (int i = 0; i < cols; i++) {
            Object col = resultSet.getObject(i + 1);
            columns[i] = col;
            if (col == null) {
                types[i] = meta.getColumnType(i + 1);
            }
        }
        this.columnValues = columns;
        this.intTypes= types;
        LOGGER.warn("writable readField columnValues"+ Arrays.toString(this.columnValues));
        LOGGER.warn("writable readField intTypes"+ Arrays.toString(this.intTypes));
    }

    public void clear() {
        Arrays.fill(columnValues, null);
    }

}
