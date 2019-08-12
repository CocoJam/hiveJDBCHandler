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

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hive.common.util.DateUtils;
import org.apache.hive.storage.jdbc.conf.config.Constants;
import org.apache.hive.storage.jdbc.conf.config.JdbcStorageConfigManager;
import org.apache.hive.storage.jdbc.dao.dataBase.DatabaseAccessor;
import org.apache.hive.storage.jdbc.dao.dataBase.DatabaseAccessorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hive.storage.jdbc.conf.config.JdbcStorageConfig;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class JdbcSerDe extends AbstractSerDe {

  private static final Logger LOGGER = LoggerFactory.getLogger(JdbcSerDe.class);

  private String[] hiveColumnNames;
  private PrimitiveTypeInfo[] hiveColumnTypes;
  private ObjectInspector inspector;
  private List<Object> row;
  private JdbcSerializer serializer;

  /*
   * This method gets called multiple times by Hive. On some invocations, the properties will be empty.
   * We need to detect when the properties are not empty to initialise the class variables.
   *
   * @see org.apache.hadoop.hive.serde2.Deserializer#initialize(org.apache.hadoop.conf.Configuration, java.util.Properties)
   */
  @Override
  public void initialize(Configuration conf, Properties properties) throws SerDeException {
    try {
      LOGGER.trace("Initializing the JdbcSerDe");

      if (properties.containsKey(JdbcStorageConfig.DATABASE_TYPE.getPropertyName())) {
        Configuration tableConfig = JdbcStorageConfigManager.convertPropertiesToConfiguration(properties);
        DatabaseAccessor dbAccessor = DatabaseAccessorFactory.getAccessor(tableConfig);
        // Extract column names and types from properties
        List<TypeInfo> hiveColumnTypesList;
        if (properties.containsKey(Constants.JDBC_TABLE) && properties.containsKey(Constants.JDBC_QUERY)) {
          // The query has been autogenerated by Hive, the column names are the
          // same in the query pushed and the list of hiveColumnNames
          String fieldNamesProperty =
                  Preconditions.checkNotNull(properties.getProperty(Constants.JDBC_QUERY_FIELD_NAMES, null));
          String fieldTypesProperty =
                  Preconditions.checkNotNull(properties.getProperty(Constants.JDBC_QUERY_FIELD_TYPES, null));
          hiveColumnNames = fieldNamesProperty.trim().split(",");
          hiveColumnTypesList = TypeInfoUtils.getTypeInfosFromTypeString(fieldTypesProperty);
        } else if (properties.containsKey(Constants.JDBC_QUERY)) {
          // The query has been specified by user, extract column names
          hiveColumnNames = properties.getProperty(serdeConstants.LIST_COLUMNS).split(",");
          hiveColumnTypesList = TypeInfoUtils.getTypeInfosFromTypeString(properties.getProperty(serdeConstants.LIST_COLUMN_TYPES));
        } else {
          // Table is specified, we need to get the column names from the accessor due to capitalization
          hiveColumnNames = dbAccessor.getColumnNames(tableConfig).toArray(new String[0]);
          // Number should be equal to list of columns
          if (hiveColumnNames.length != properties.getProperty(serdeConstants.LIST_COLUMNS).split(",").length) {
            throw new SerDeException("Column numbers do not match. " +
                "Remote table columns are " + Arrays.toString(hiveColumnNames) + " and declared table columns in Hive " +
                "external table are " + Arrays.toString(properties.getProperty(serdeConstants.LIST_COLUMNS).split(",")));
          }
          hiveColumnTypesList = TypeInfoUtils.getTypeInfosFromTypeString(properties.getProperty(serdeConstants.LIST_COLUMN_TYPES));
        }
        if (hiveColumnNames.length == 0) {
          throw new SerDeException("Received an empty Hive column name definition");
        }
        if (hiveColumnTypesList.size() == 0) {
          throw new SerDeException("Received an empty Hive column type definition");
        }

        // Populate column types and inspector
        hiveColumnTypes = new PrimitiveTypeInfo[hiveColumnTypesList.size()];
        List<ObjectInspector> fieldInspectors = new ArrayList<>(hiveColumnNames.length);
        for (int i = 0; i < hiveColumnNames.length; i++) {
          TypeInfo ti = hiveColumnTypesList.get(i);
          if (ti.getCategory() != Category.PRIMITIVE) {
            throw new SerDeException("Non primitive types not supported yet");
          }
          hiveColumnTypes[i] = (PrimitiveTypeInfo) ti;
          fieldInspectors.add(
              PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(hiveColumnTypes[i]));
        }
        inspector =
            ObjectInspectorFactory.getStandardStructObjectInspector(Arrays.asList(hiveColumnNames),
                fieldInspectors);
        row = new ArrayList<>(hiveColumnNames.length);
        serializer = new JdbcSerializer(hiveColumnNames,hiveColumnTypes,row);
      }
    }
    catch (Exception e) {
      LOGGER.error("Caught exception while initializing the SqlSerDe", e);
      throw new SerDeException(e);
    }

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("JdbcSerDe initialized with\n" + "\t columns: " + Arrays.toString(hiveColumnNames) + "\n\t types: " + Arrays
          .toString(hiveColumnTypes));
    }
  }

  @Override
  public Object deserialize(Writable blob) throws SerDeException {
    LOGGER.trace("Deserializing from SerDe");
    if (!(blob instanceof MapWritable)) {
      throw new SerDeException("Expected MapWritable. Got " + blob.getClass().getName());
    }

    if ((row == null) || (hiveColumnNames == null)) {
      throw new SerDeException("JDBC SerDe hasn't been initialized properly");
    }

    row.clear();
    MapWritable input = (MapWritable) blob;
    Text columnKey = new Text();

    for (int i = 0; i < hiveColumnNames.length; i++) {
      columnKey.set(hiveColumnNames[i]);
      Writable value = input.get(columnKey);
      Object rowVal;

      if(value instanceof NullWritable) {
        rowVal = null;
      } else {
        rowVal = ((ObjectWritable)value).get();

        switch (hiveColumnTypes[i].getPrimitiveCategory()) {
        case INT:
        case SHORT:
        case BYTE:
          if (rowVal instanceof Number) {
            rowVal = ((Number)rowVal).intValue(); 
          } else {
            rowVal = Integer.valueOf(rowVal.toString());
          }
          break;
        case LONG:
          if (rowVal instanceof Long) {
            rowVal = ((Number)rowVal).longValue(); 
          } else {
            rowVal = Long.valueOf(rowVal.toString());
          }
          break;
        case FLOAT:
          if (rowVal instanceof Number) {
            rowVal = ((Number)rowVal).floatValue(); 
          } else {
            rowVal = Float.valueOf(rowVal.toString());
          }
          break;
        case DOUBLE:
          if (rowVal instanceof Number) {
            rowVal = ((Number)rowVal).doubleValue(); 
          } else {
            rowVal = Double.valueOf(rowVal.toString());
          }
          break;
        case DECIMAL:
          int scale = ((DecimalTypeInfo)hiveColumnTypes[i]).getScale();
          rowVal = HiveDecimal.create(rowVal.toString());
          ((HiveDecimal)rowVal).setScale(scale, BigDecimal.ROUND_HALF_EVEN);
          break;
        case BOOLEAN:
          if (rowVal instanceof Number) {
            rowVal = ((Number) value).intValue() != 0;
          } else {
            rowVal = Boolean.valueOf(value.toString());
          }
          break;
        case CHAR:
        case VARCHAR:
        case STRING:
          if (rowVal instanceof java.sql.Date) {
            rowVal = DateUtils.getDateFormat().format((java.sql.Date)rowVal);
          } else {
            rowVal = rowVal.toString();
          }
          break;
        case DATE:
          if (rowVal instanceof java.sql.Date) {
            java.sql.Date dateRowVal = (java.sql.Date) rowVal;
            rowVal = Date.ofEpochMilli(dateRowVal.getTime());
          } else {
            rowVal = Date.valueOf (rowVal.toString());
          }
          break;
        case TIMESTAMP:
          if (rowVal instanceof java.sql.Timestamp) {
            java.sql.Timestamp timestampRowVal = (java.sql.Timestamp) rowVal;
            rowVal = Timestamp.ofEpochMilli(timestampRowVal.getTime(), timestampRowVal.getNanos());
          } else {
            rowVal = Timestamp.valueOf (rowVal.toString());
          }
          break;
        default:
          //do nothing
          break;
        }
      }
      row.add(rowVal);
    }
    return row;
  }


  @Override
  public ObjectInspector getObjectInspector() throws SerDeException {
    return inspector;
  }


  @Override
  public Class<? extends Writable> getSerializedClass() {
    return MapWritable.class;
  }


  @Override
  public Writable serialize(Object obj, ObjectInspector objInspector) throws SerDeException {
    try {
      return serializer.serialize(obj, objInspector);
    } catch (Exception e) {
      e.printStackTrace();
    }
    return null;
//    throw new UnsupportedOperationException("Writes are not allowed");
  }


  @Override
  public SerDeStats getSerDeStats() {
    return null;
  }

}
