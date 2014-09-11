package com.ldbc.snb.interactive.neo4j.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

public class Utils {
    public static String[] copyArrayAndAddElement(String[] oldArray, String newElement) {
        if (null == oldArray) {
            return new String[]{newElement};
        } else {
            String[] newArray = new String[oldArray.length + 1];
            System.arraycopy(oldArray, 0, newArray, 0, oldArray.length);
            newArray[newArray.length - 1] = newElement;
            return newArray;
        }
    }

    public static Map loadConfig(String dbConfigFilePath) throws IOException {
        Map dbConfig = new Properties();
        ((Properties) dbConfig).load(new FileInputStream(new File(dbConfigFilePath)));
        return dbConfig;
    }
}
