package com.avinash.engine.main;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.springframework.beans.factory.annotation.Autowired;

public class SparkMain{

    @Autowired
    private static JavaSparkContext sparkContext;

    public static void main(String[] args){
        SQLContext sqlContext = new SQLContext(sparkContext);
    }

}
