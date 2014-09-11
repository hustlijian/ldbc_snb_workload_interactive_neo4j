package com.ldbc.snb.interactive.neo4j.interactive.remote_cypher;

import com.ldbc.driver.DbException;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery6;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery6Result;
import com.ldbc.snb.interactive.neo4j.interactive.Neo4jQuery6;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class Neo4jQuery6RemoteCypher extends Neo4jQuery6<Connection> {
    @Override
    public String description() {
        return QUERY_STRING;
    }

    @Override
    public Iterator<LdbcQuery6Result> execute(Connection connection, LdbcQuery6 operation) throws DbException {
        try (PreparedStatement preparedStatement = connection.prepareStatement(QUERY_STRING)) {
            preparedStatement.setLong(PERSON_ID, operation.personId());
            preparedStatement.setString(TAG_NAME, operation.tagName());
            preparedStatement.setInt(LIMIT, operation.limit());
            ResultSet resultSet = preparedStatement.executeQuery();
            return new ResultSetIterator(resultSet);
        } catch (SQLException e) {
            throw new DbException("Error while executing query", e);
        }
    }

    private class ResultSetIterator implements Iterator<LdbcQuery6Result> {
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
        public LdbcQuery6Result next() {
            try {
                if (false == hasNext) throw new NoSuchElementException();
                LdbcQuery6Result result = new LdbcQuery6Result(
                        resultSet.getString("tagName"),
                        ((Long) resultSet.getLong("postCount")).intValue());
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
