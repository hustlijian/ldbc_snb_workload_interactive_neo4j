package com.ldbc.snb.interactive.neo4j.interactive.remote_cypher;

import com.ldbc.driver.DbException;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery7;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery7Result;
import com.ldbc.snb.interactive.neo4j.interactive.Neo4jQuery7;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class Neo4jQuery7RemoteCypher extends Neo4jQuery7<Connection> {
    @Override
    public String description() {
        return QUERY_STRING;
    }

    @Override
    public Iterator<LdbcQuery7Result> execute(Connection connection, LdbcQuery7 operation) throws DbException {
        try (PreparedStatement preparedStatement = connection.prepareStatement(QUERY_STRING)) {
            preparedStatement.setLong(PERSON_ID, operation.personId());
            preparedStatement.setInt(LIMIT, operation.limit());
            ResultSet resultSet = preparedStatement.executeQuery();
            return new ResultSetIterator(resultSet);
        } catch (SQLException e) {
            throw new DbException("Error while executing query", e);
        }
    }

    private class ResultSetIterator implements Iterator<LdbcQuery7Result> {
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
        public LdbcQuery7Result next() {
            try {
                if (false == hasNext) throw new NoSuchElementException();
                long latencyAsMilli = resultSet.getLong("latencyAsMilli");
                Long latencyAsMinutes = (latencyAsMilli / 1000) / 60;
                LdbcQuery7Result result = new LdbcQuery7Result(
                        resultSet.getLong("personId"),
                        resultSet.getString("personFirstName"),
                        resultSet.getString("personLastName"),
                        resultSet.getLong("likeTime"),
                        resultSet.getLong("messageId"),
                        resultSet.getString("messageContent"),
                        latencyAsMinutes.intValue(),
                        resultSet.getBoolean("isNew"));
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
