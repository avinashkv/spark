package com.avinash.engine.store.builder;

import com.avinash.engine.store.DataStore;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HeaderFileDataStoreBuilder implements IDataStoreBuilder {
    private String headerFile;

    public HeaderFileDataStoreBuilder(String headerFile) {
        this.headerFile = headerFile;
    }

    public DataStore build(){
        Map<String,Integer> storeindex = new HashMap<String, Integer>();
        List<String> columnNames = new ArrayList<String>();
        List<Boolean> isKey= new ArrayList<Boolean>();
        List<String> dataType= new ArrayList<>();
        List<Boolean> isNullable= new ArrayList<>();
        try {
            File file = new File(headerFile);
            if (file.exists()) {
                List<String> contents = FileUtils.readLines(file);
                int lineNumber=0;
                for(String content:contents){
                    String[] contentStr = content.split(",");
                    for(int i=0;i<contentStr.length;i++){
                        String contentTrim= contentStr[i].trim().toLowerCase();
                        if(lineNumber==0){
                            storeindex.put(contentTrim,i);
                            columnNames.add(contentTrim);
                        }else if(lineNumber==1){
                            isKey.add(Boolean.parseBoolean(contentTrim));
                        }else if(lineNumber==2){
                            dataType.add(contentTrim);
                        }else if(lineNumber==3){
                            isNullable.add(Boolean.parseBoolean(contentTrim));
                        }
                    }
                }

            } else {
                throw new IllegalArgumentException("File:" + headerFile+" not found");
            }
        }catch (IOException e){
            throw new IllegalArgumentException("File:"+headerFile+" has a problem. Exception:"+e);
        }

        return new DataStore(storeindex, (String[])columnNames.toArray(), (Boolean[])isKey.toArray(),(String[])dataType.toArray(),(Boolean[])isNullable.toArray());
    }

}
