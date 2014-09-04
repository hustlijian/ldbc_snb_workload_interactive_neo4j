package com.ldbc.socialnet.neo4j;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.ldbc.driver.util.Function1;
import com.ldbc.driver.util.Tuple;
import com.ldbc.socialnet.workload.neo4j.utils.StepsUtilsTemp;
import org.junit.Test;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

// TODO move to StepUtils
public class StepsUtilsTempTest {
    @Test
    public void groupByNotDistinct() throws Exception {
        // Given
        List<String> original = Lists.newArrayList("1", "1", "2", "3", "11", "12", "123");
        Function1<String, Tuple.Tuple2<Integer, String>> extractFun = new Function1<String, Tuple.Tuple2<Integer, String>>() {
            @Override
            public Tuple.Tuple2<Integer, String> apply(String s) {
                return Tuple.tuple2(s.length(), s);
            }
        };
        boolean distinct = false;

        // When
        Map<Integer, Collection<String>> groups = StepsUtilsTemp.groupBy(original.iterator(), extractFun, distinct);

        // Then
        assertThat(groups.size(), is(3));
        assertThat(Lists.newArrayList(groups.get(1)), equalTo(Lists.newArrayList("1", "1", "2", "3")));
        assertThat(Lists.newArrayList(groups.get(2)), equalTo(Lists.newArrayList("11", "12")));
        assertThat(Lists.newArrayList(groups.get(3)), equalTo(Lists.newArrayList("123")));
    }

    @Test
    public void groupByDistinct() throws Exception {
        // Given
        List<String> original = Lists.newArrayList("1", "1", "2", "3", "11", "12", "123");
        Function1<String, Tuple.Tuple2<Integer, String>> extractFun = new Function1<String, Tuple.Tuple2<Integer, String>>() {
            @Override
            public Tuple.Tuple2<Integer, String> apply(String s) {
                return Tuple.tuple2(s.length(), s);
            }
        };
        boolean distinct = true;

        // When
        Map<Integer, Collection<String>> groups = StepsUtilsTemp.groupBy(original.iterator(), extractFun, distinct);

        // Then
        assertThat(groups.size(), is(3));
        assertThat(Sets.newHashSet(groups.get(1)), equalTo(Sets.newHashSet("1", "2", "3")));
        assertThat(Sets.newHashSet(groups.get(2)), equalTo(Sets.newHashSet("11", "12")));
        assertThat(Sets.newHashSet(groups.get(3)), equalTo(Sets.newHashSet("123")));
    }
}
