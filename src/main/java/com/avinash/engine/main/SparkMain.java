package com.avinash.engine.main;

import com.avinash.engine.config.DataStoreConfig;
import com.avinash.engine.config.SparkConfig;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Import;

@SpringBootApplication
@Import({DataStoreConfig.class,SparkConfig.class})
public class SparkMain implements CommandLineRunner {

    @Autowired
    private SparkSession sparkSession;

    @Autowired
    private StructType mainstoreType;

    @Value("${csv.schema.file.loc}")
    private String filelocation;

    @Override
    public void run(String... strings) throws Exception {
        long count = sparkSession.read().option("header","true")
                .schema(mainstoreType)
                .csv(filelocation)
                .count();
        System.out.println("Count:"+count);
    }

    public static void main(String[] args) throws Exception {
        SpringApplication.run(SparkMain.class, args);
    }

}
