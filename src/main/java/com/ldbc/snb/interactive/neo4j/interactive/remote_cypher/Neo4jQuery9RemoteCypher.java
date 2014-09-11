package com.ldbc.snb.interactive.neo4j.interactive.remote_cypher;

import com.ldbc.driver.DbException;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery9;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery9Result;
import com.ldbc.snb.interactive.neo4j.interactive.Neo4jQuery9;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class Neo4jQuery9RemoteCypher extends Neo4jQuery9<Connection> {
    @Override
    public String description() {
        return QUERY_STRING;
    }

    @Override
    public Iterator<LdbcQuery9Result> execute(Connection connection, LdbcQuery9 operation) throws DbException {
        try (PreparedStatement preparedStatement = connection.prepareStatement(QUERY_STRING)) {
            preparedStatement.setLong(PERSON_ID, operation.personId());
            preparedStatement.setLong(LATEST_DATE, operation.maxDate().getTime());
            preparedStatement.setInt(LIMIT, operation.limit());
            ResultSet resultSet = preparedStatement.executeQuery();
            return new ResultSetIterator(resultSet);
        } catch (SQLException e) {
            throw new DbException("Error while executing query", e);
        }
    }

    private class ResultSetIterator implements Iterator<LdbcQuery9Result> {
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
        public LdbcQuery9Result next() {
            try {
                if (false == hasNext) throw new NoSuchElementException();
                LdbcQuery9Result result = new LdbcQuery9Result(
                        resultSet.getLong("personId"),
                        resultSet.getString("personFirstName"),
                        resultSet.getString("personLastName"),
                        resultSet.getLong("messageId"),
                        resultSet.getString("messageContent"),
                        resultSet.getLong("messageCreationDate"));
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