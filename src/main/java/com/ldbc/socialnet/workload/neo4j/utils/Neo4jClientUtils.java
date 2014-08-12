package com.ldbc.socialnet.workload.neo4j.utils;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class Neo4jClientUtils {
    public static String toCypherPropertiesString(Map<String, Iterator<Byte>> values, String nodeName) {
        StringBuilder sb = new StringBuilder();
        for (String key : values.keySet()) {
            sb.append(nodeName);
            sb.append(".");
            sb.append(key);
            sb.append("={");
            sb.append(key);
            sb.append("},");
        }
        return sb.toString().substring(0, sb.toString().length() - 1);

    }

    public static Map<String, Object> toStringObjectMap(Map<String, Iterator<Byte>> values) {
        Map<String, Object> cypherMap = new HashMap<>();
        for (String key : values.keySet()) {
            cypherMap.put(key, values.get(key).toString());
        }
        return cypherMap;
    }

}
