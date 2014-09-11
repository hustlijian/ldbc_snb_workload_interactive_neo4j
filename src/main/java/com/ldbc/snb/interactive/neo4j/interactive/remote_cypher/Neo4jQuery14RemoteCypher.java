package com.ldbc.snb.interactive.neo4j.interactive.remote_cypher;

import com.ldbc.driver.DbException;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery14;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery14Result;
import com.ldbc.snb.interactive.neo4j.interactive.Neo4jQuery14;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.NoSuchElementException;

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
        private boolean hasNext;

        private ResultSetIterator(ResultSet resultSet) throws SQLException {
            this.resultSet = resultSet;
            this.hasNext = resultSet.next();
        }

        @Override
        public boolean hasNext() {
            return hasNext;
        }

        @Override
        public LdbcQuery14Result next() {
            try {
                if (false == hasNext) throw new NoSuchElementException();
                LdbcQuery14Result result = new LdbcQuery14Result(
                        (Iterable<Long>) resultSet.getObject("pathNodeIds"),
                        resultSet.getDouble("weight"));
                hasNext = resultSet.next();
                return result;
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
