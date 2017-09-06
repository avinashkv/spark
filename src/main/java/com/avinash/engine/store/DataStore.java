package com.avinash.engine.store;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class DataStore {
    private final Map<String,Integer> storeindex;
    private final String[] columnNames;
    private final Boolean[] isKey;
    private final String[] dataType;

    public DataStore(Map<String,Integer> storeindex,String[] columnNames,Boolean[] isKey,String[] dataType){//String headerFile
        this.storeindex = Collections.unmodifiableMap(storeindex);
        this.columnNames=columnNames;
        this.isKey = isKey;
        this.dataType=dataType;
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

    public Set<String> getColumns(){
        return storeindex.keySet();
    }
}
