package com.ldbc.socialnet.workload.neo4j.utils;

import com.ldbc.driver.util.Function0;
import com.ldbc.driver.util.Function1;
import com.ldbc.driver.util.Tuple;

import java.util.*;

public class StepsUtilsTemp {
    public static <GROUP_KEY, GROUPED_VALUE, ORIGINAL> Map<GROUP_KEY, Collection<GROUPED_VALUE>> groupBy(
            Iterator<ORIGINAL> originalValues,
            Function1<ORIGINAL, Tuple.Tuple2<GROUP_KEY, GROUPED_VALUE>> extractFun,
            final boolean distinct) {
        Function0<Collection<GROUPED_VALUE>> newGroupFun = new Function0<Collection<GROUPED_VALUE>>() {
            @Override
            public Collection<GROUPED_VALUE> apply() {
                if (distinct) return new HashSet<>();
                else return new ArrayList<>();
            }
        };
        Map<GROUP_KEY, Collection<GROUPED_VALUE>> groups = new HashMap<>();
        while (originalValues.hasNext()) {
            ORIGINAL originalValue = originalValues.next();
            Tuple.Tuple2<GROUP_KEY, GROUPED_VALUE> pathValues = extractFun.apply(originalValue);
            GROUP_KEY groupKey = pathValues._1();
            GROUPED_VALUE groupValue = pathValues._2();
            Collection<GROUPED_VALUE> group = groups.get(groupKey);
            if (null == group) group = newGroupFun.apply();
            group.add(groupValue);
            groups.put(groupKey, group);
        }
        return groups;
    }
}
