package com.avinash.engine.store;

import java.util.Collections;
import java.util.Map;

public class DataStore {
    private final Map<String,Integer> storeindex;
    private final String[] columnNames;
    private final Boolean[] isKey;
    private final String[] dataType;
    private final Boolean[] isNullable;

    public DataStore(Map<String,Integer> storeindex,String[] columnNames,Boolean[] isKey,String[] dataType,Boolean[] isNullable){
        this.storeindex = Collections.unmodifiableMap(storeindex);
        this.columnNames=columnNames;
        this.isKey = isKey;
        this.dataType=dataType;
        this.isNullable=isNullable;
    }

    public Boolean isKey(String column){
        if(storeindex.containsKey(column.trim().toLowerCase())){
            return isKey[storeindex.get(column.trim().toLowerCase())];
        }
        return null;
    }

    public String getDataType(String column){
        if(storeindex.containsKey(column.trim().toLowerCase())){
            return dataType[storeindex.get(column.trim().toLowerCase())];
        }
        return null;
    }

    public Integer getIndexOf(String column){
        if(storeindex.containsKey(column.trim().toLowerCase())){
            return storeindex.get(column.trim().toLowerCase());
        }
        return null;
    }

    public Boolean isNullable(String column){
        if(storeindex.containsKey(column.trim().toLowerCase())){
            return isNullable[storeindex.get(column.trim().toLowerCase())];
        }
        return null;
    }
    public String[] getColumns(){
        return columnNames.clone();
    }
}
