package com.ldbc.socialnet.workload.neo4j.interactive.remote_cypher;

import com.ldbc.driver.DbException;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery14;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery14Result;
import com.ldbc.socialnet.workload.neo4j.interactive.Neo4jQuery14;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;

public class Neo4jQuery14RemoteCypher extends Neo4jQuery14<Connection> {
    @Override
    public String description() {
        return QUERY_STRING;
    }

    @Override
    public Iterator<LdbcQuery14Result> execute(Connection connection, LdbcQuery14 operation) throws DbException {
        try (PreparedStatement preparedStatement = connection.prepareStatement(QUERY_STRING)) {
            preparedStatement.setLong(PERSON_ID_1, operation.person1Id());
            preparedStatement.setLong(PERSON_ID_2, operation.person2Id());
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

        // TODO implement
        @Override
        public LdbcQuery14Result next() {
            // TODO old, remove
//            try {
//                Collection<LdbcQuery14Result.PathNode> pathNodes = Collections2.transform((Collection<Collection<Object>>) resultSet.getObject("pathNodes"), new Function<Collection<Object>, LdbcQuery14Result.PathNode>() {
//                    @Override
//                    public LdbcQuery14Result.PathNode apply(Collection<Object> pathNode) {
//                        List<Object> pathNodeList = Lists.newArrayList(pathNode);
//                        return new LdbcQuery14Result.PathNode((String) pathNodeList.get(0), (long) pathNodeList.get(1));
//                    }
//                });
//                return new LdbcQuery14Result(
//                        pathNodes,
//                        resultSet.getDouble("weight"));
//            } catch (SQLException e) {
//                throw new RuntimeException("Error while retrieving next row", e);
//            }
            // TODO temp, remove
            Iterable<Long> personIdsInPath = null;
            double pathWeight = 0;
            return new LdbcQuery14Result(personIdsInPath, pathWeight);
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("remove() not supported by " + getClass().getName());
        }
    }
}
