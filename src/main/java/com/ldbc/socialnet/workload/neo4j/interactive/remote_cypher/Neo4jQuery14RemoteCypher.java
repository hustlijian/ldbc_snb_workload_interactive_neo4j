package com.ldbc.socialnet.workload.neo4j.interactive.remote_cypher;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.ldbc.driver.DbException;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery14;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery14Result;
import com.ldbc.socialnet.workload.neo4j.interactive.Neo4jQuery14;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class Neo4jQuery14RemoteCypher extends Neo4jQuery14<Connection> {
    @Override
    public String description() {
        return QUERY_STRING;
    }

    @Override
    public Iterator<LdbcQuery14Result> execute(Connection connection, LdbcQuery14 operation) throws DbException {
        try (PreparedStatement preparedStatement = connection.prepareStatement(QUERY_STRING)) {
            preparedStatement.setLong(PERSON_ID_1, operation.personId1());
            preparedStatement.setLong(PERSON_ID_2, operation.personId2());
            preparedStatement.setInt(LIMIT, operation.limit());
            ResultSet resultSet = preparedStatement.executeQuery();
            return new ResultSetIterator(resultSet);
        } catch (SQLException e) {
            throw new DbException("Error while executing query", e);
        }
    }

    private class ResultSetIterator implements Iterator<LdbcQuery14Result> {
        private final ResultSet resultSet;

        private ResultSetIterator(ResultSet resultSet) {
            this.resultSet = resultSet;
        }

        @Override
        public boolean hasNext() {
            try {
                return resultSet.next();
            } catch (SQLException e) {
                e.printStackTrace();
                return false;
            }
        }

        @Override
        public LdbcQuery14Result next() {
            try {
                Collection<LdbcQuery14Result.PathNode> pathNodes = Collections2.transform((Collection<Collection<Object>>) resultSet.getObject("pathNodes"), new Function<Collection<Object>, LdbcQuery14Result.PathNode>() {
                    @Override
                    public LdbcQuery14Result.PathNode apply(Collection<Object> pathNode) {
                        List<Object> pathNodeList = Lists.newArrayList(pathNode);
                        return new LdbcQuery14Result.PathNode((String) pathNodeList.get(0), (long) pathNodeList.get(1));
                    }
                });
                return new LdbcQuery14Result(
                        pathNodes,
                        resultSet.getDouble("weight"));
            } catch (SQLException e) {
                throw new RuntimeException("Error while retrieving next row", e);
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("remove() not supported by " + getClass().getName());
        }
    }
}
