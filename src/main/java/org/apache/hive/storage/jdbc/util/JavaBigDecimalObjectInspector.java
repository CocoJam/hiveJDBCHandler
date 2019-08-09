package org.apache.hive.storage.jdbc.util;

import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaHiveDecimalObjectInspector;

import java.lang.Object;

import org.apache.hadoop.hive.common.type.HiveDecimal;

import java.math.BigDecimal;

import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;

public class JavaBigDecimalObjectInspector
        extends JavaHiveDecimalObjectInspector {

    public JavaBigDecimalObjectInspector() {
    }

    public JavaBigDecimalObjectInspector(DecimalTypeInfo typeInfo) {
        super(typeInfo);
    }

    @Override
    public HiveDecimal getPrimitiveJavaObject(Object o) {
        Object obj = null;

        if (o != null && o instanceof BigDecimal) {
            obj = HiveDecimal.create((BigDecimal) o);
        } else {
            obj = o;
        }

        return super.getPrimitiveJavaObject(obj);
    }

}