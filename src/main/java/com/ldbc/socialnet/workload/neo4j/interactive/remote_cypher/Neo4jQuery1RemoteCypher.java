package com.ldbc.socialnet.workload.neo4j.interactive.remote_cypher;

import com.google.common.collect.Lists;
import com.ldbc.driver.DbException;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery1;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery1Result;
import com.ldbc.socialnet.workload.neo4j.interactive.Neo4jQuery1;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class Neo4jQuery1RemoteCypher extends Neo4jQuery1<Connection> {
    @Override
    public String description() {
        return QUERY_STRING;
    }

    @Override
    public Iterator<LdbcQuery1Result> execute(Connection connection, LdbcQuery1 operation) throws DbException {
        try (PreparedStatement preparedStatement = connection.prepareStatement(QUERY_STRING)) {
            preparedStatement.setLong(PERSON_ID, operation.personId());
            preparedStatement.setString(FRIEND_FIRST_NAME, operation.firstName());
            preparedStatement.setInt(LIMIT, operation.limit());
            ResultSet resultSet = preparedStatement.executeQuery();
            return new ResultSetIterator(resultSet);
        } catch (SQLException e) {
            throw new DbException("Error while executing query", e);
        }
    }

    private class ResultSetIterator implements Iterator<LdbcQuery1Result> {
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
        public LdbcQuery1Result next() {
            try {
                if (false == hasNext) throw new NoSuchElementException();
                Object emailsObject = resultSet.getObject("emails");
                Iterable<String> emails = (Iterable.class.isAssignableFrom(emailsObject.getClass()))
                        ? (Iterable) emailsObject
                        : Lists.newArrayList((String[]) emailsObject);

                Object languagesObject = resultSet.getObject("languages");
                Iterable<String> languages = (Iterable.class.isAssignableFrom(languagesObject.getClass()))
                        ? (Iterable) languagesObject
                        : Lists.newArrayList((String[]) languagesObject);

                LdbcQuery1Result result = new LdbcQuery1Result(
                        resultSet.getLong("id"),
                        resultSet.getString("lastName"),
                        resultSet.getInt("distance"),
                        resultSet.getLong("birthday"),
                        resultSet.getLong("creationDate"),
                        resultSet.getString("gender"),
                        resultSet.getString("browser"),
                        resultSet.getString("locationIp"),
                        emails,
                        languages,
                        resultSet.getString("cityName"),
                        (Collection) resultSet.getObject("unis"),
                        (Collection) resultSet.getObject("companies"));
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
