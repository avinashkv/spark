package com.avinash.engine.spark.context;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkContext {
    private static SparkConf conf = new SparkConf().setAppName("SparkEngine").setMaster("master");
    private static JavaSparkContext INSTANCE = new JavaSparkContext(conf);

    public static JavaSparkContext getInstance(){
        return INSTANCE;
    }
}
