package org.apache.hive.storage.jdbc.records;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.lazy.fast.LazySimpleDeserializeRead;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.io.Writable;
import org.apache.hive.storage.jdbc.records.*;
import org.apache.hive.storage.jdbc.util.HiveJdbcBridgeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class JdbcSerializer {
    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcSerializer.class);
    private String[] hiveColumnNames;
    private PrimitiveTypeInfo[] hiveColumnTypes;
    private List<Object> row;
    private JdbcWritable jdbcWritable;
//    private LazySimpleSerDe
    public JdbcSerializer(String[] hiveColumnNames, PrimitiveTypeInfo[] hiveColumnTypes,List<Object> row) {
        this.hiveColumnNames = hiveColumnNames;
        this.hiveColumnTypes = hiveColumnTypes;
        this.row = row;
    }

    public Writable serialize(Object obj, ObjectInspector objInspector) throws Exception {
        if (objInspector.getCategory() != ObjectInspector.Category.STRUCT) {
            throw new SerDeException(getClass().toString()
                    + " can only serialize struct types, but we got: "
                    + objInspector.getTypeName());
        }
        StructObjectInspector soi = (StructObjectInspector) objInspector;
        List<? extends StructField> fields = soi.getAllStructFieldRefs();
        List<Object> values = soi.getStructFieldsDataAsList(obj);
        jdbcWritable = new JdbcWritable(hiveColumnTypes);
        for (int i = 0; i < hiveColumnNames.length; i++) {
            StructField structField = fields.get(i);
            Object field = soi.getStructFieldData(row,
                    structField);
            ObjectInspector fieldOI = structField.getFieldObjectInspector();
            Object javaObject = HiveJdbcBridgeUtils.deparseObject(field,
                    fieldOI);
            jdbcWritable.set(i, javaObject);
        }
        LOGGER.warn("serializer serialize");
        return jdbcWritable;
    }
}
