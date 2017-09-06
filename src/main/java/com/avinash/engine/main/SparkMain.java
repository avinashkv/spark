package com.avinash.engine.main;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.springframework.beans.factory.annotation.Autowired;

public class SparkMain{

    @Autowired
    private static JavaSparkContext sparkContext;

    public static void main(String[] args){
        SQLContext sqlContext = new SQLContext(sparkContext);
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL Example")
                .getOrCreate();

        //Derive from Datastore
        StructType schema = new StructType()
                .add("department", "string")
                .add("designation", "string")
                .add("ctc", "long")
                .add("state", "string");

        spark.read().option("header",false).schema(schema).csv();
    }

}
