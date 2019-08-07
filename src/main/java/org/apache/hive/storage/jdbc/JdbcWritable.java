package org.apache.hive.storage.jdbc;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.*;

public class JdbcWritable implements Writable, org.apache.hadoop.mapred.lib.db.DBWritable,
        org.apache.hadoop.mapreduce.lib.db.DBWritable {
    private Object[] columnValues;
    private int[] columnTypes;

    public JdbcWritable() {
    }

    public JdbcWritable(Object[] columnValues, int[] columnTypes) {
        this.columnValues = columnValues;
        this.columnTypes = columnTypes;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        if (columnValues == null) {
            out.writeInt(-1);
            return;
        }
        if (columnTypes == null) {
            out.writeInt(-1);
            return;
        }
        final Object[] values = this.columnValues;
        final int[] types = this.columnTypes;
        assert (values.length == types.length);
        out.writeInt(values.length);
        for (int i = 0; i < values.length; i++) {
            HiveJdbcBridgeUtils.writeObject(values[i], types[i], out);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int size = in.readInt();
        if (size == -1) {
            return;
        }
        if (columnValues == null) {
            this.columnValues = new Object[size];
            this.columnTypes = new int[size];
        } else {
            clear();
        }
        for (int i = 0; i < size; i++) {
            int sqlType = in.readInt();
            columnTypes[i] = sqlType;
            Object v = HiveJdbcBridgeUtils.readObject(in, sqlType);
            columnValues[i] = v;
        }
    }

    @Override
    public void write(PreparedStatement preparedStatement) throws SQLException {
        assert (columnValues != null);
        assert (columnTypes != null);
        final Object[] r = this.columnValues;
        final int cols = r.length;
        for (int i = 0; i < cols; i++) {
            final Object col = r[i];
            if (col == null) {
                preparedStatement.setNull(i + 1, columnTypes[i]);
            } else {
                preparedStatement.setObject(i + 1, col);
            }
        }
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
        this.columnTypes = types;
    }

    public void clear() {
        Arrays.fill(columnValues, null);
    }
//    @Override
//    public int size() {
//        return propertyMapping.size();
//    }
//
//    @Override
//    public boolean isEmpty() {
//        return propertyMapping.isEmpty();
//    }
//
//    @Override
//    public boolean containsKey(Object key) {
//        return ;
//    }
//
//    @Override
//    public boolean containsValue(Object value) {
//        return false;
//    }
//
//    @Override
//    public Object get(Object key) {
//        return null;
//    }
//
//    @Override
//    public Object put(Object key, Object value) {
//        return null;
//    }
//
//    @Override
//    public Object remove(Object key) {
//        return null;
//    }
//
//    @Override
//    public void putAll(Map m) {
//
//    }
//
//    @Override
//    public void clear() {
//
//    }
//
//    @Override
//    public Set keySet() {
//        return null;
//    }
//
//    @Override
//    public Collection values() {
//        return null;
//    }
//
//    @Override
//    public Set<Entry> entrySet() {
//        return null;
//    }
}
