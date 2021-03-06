package com.avinash.engine.config;

import com.avinash.engine.store.DataStore;
import com.avinash.engine.store.builder.HeaderFileDataStoreBuilder;
import com.avinash.engine.store.builder.IDataStoreBuilder;
import com.avinash.engine.util.DataTypeFactory;
import org.apache.spark.sql.types.StructType;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

import java.util.Arrays;

@Configuration
@PropertySource("classpath:engine.properties")
public class DataStoreConfig {

    @Value("${csv.schema.header.path}")
    private String header;

    @Bean
    public IDataStoreBuilder dataStoreBuilder(){
        return new HeaderFileDataStoreBuilder(header);
    }

    @Bean
    public DataStore datastore(){
        return dataStoreBuilder().build();
    }

    @Bean
    public StructType mainstoreType(){
        StructType schema = new StructType();
        Arrays.stream(datastore().getColumns()).forEach(col -> {
            schema.add(col, DataTypeFactory.getType(datastore().getDataType(col)),datastore().isNullable(col));
        });
        return schema;
    }
}
