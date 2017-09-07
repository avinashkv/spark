package com.avinash.engine.main;

import com.avinash.engine.config.DataStoreConfig;
import com.avinash.engine.config.SparkConfig;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import({DataStoreConfig.class,SparkConfig.class})
public class SparkMain{

    @Autowired
    private static SparkSession sparkSession;

    @Autowired
    private static StructType mainstoreType;

    @Value("csv.schema.file.loc")
    private static String filelocation;

    public static void testSpark(){
        long count = sparkSession.read().option("header","true")
                .schema(mainstoreType)
                .csv(filelocation)
                .count();
        System.out.println("Count:"+count);
    }

    public static void main(String[] args){
        testSpark();
    }

}
