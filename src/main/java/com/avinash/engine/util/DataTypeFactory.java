package com.avinash.engine.util;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

public class DataTypeFactory {
    public static DataType getType(String type){
        switch (type) {
            case "string":
                return DataTypes.StringType;
            case "double":
                return DataTypes.DoubleType;
            case "int":
                return DataTypes.IntegerType;
            case "date":
                return DataTypes.DateType;
            case "long":
                return DataTypes.LongType;
            case "float":
                return DataTypes.FloatType;
            case "double[]":
                return DataTypes.createArrayType(DataTypes.DoubleType);
            case "int[]":
                return DataTypes.createArrayType(DataTypes.IntegerType);
            case "string[]":
                return DataTypes.createArrayType(DataTypes.StringType);
            case "date[]":
                return DataTypes.createArrayType(DataTypes.DateType);
            case "long[]":
                return DataTypes.createArrayType(DataTypes.LongType);
            case "float[]":
                return DataTypes.createArrayType(DataTypes.FloatType);
        }
          return DataTypes.NullType;
    }

}
