package com.avinash.engine.config;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

@Configuration
@PropertySource("classpath:engine.properties")
public class SparkConfig {

    @Value("engine.spark.engine.name")
    private String engineName;

    @Value("engine.spark.engine.master.url")
    private String masterURL;

    @Bean
    public JavaSparkContext javaSparkContext(){
        SparkConf conf = new SparkConf().setAppName(engineName).setMaster(masterURL);
        return new JavaSparkContext(conf);
    }

    @Bean
    public SparkSession sparkSession(){
        return  SparkSession.builder()
                .appName(engineName)
                .getOrCreate();
    }

}
