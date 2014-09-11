package com.ldbc.snb.interactive.neo4j.interactive.remote_cypher;

import com.ldbc.driver.DbException;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery5;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery5Result;
import com.ldbc.snb.interactive.neo4j.interactive.Neo4jQuery5;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Neo4jQuery5RemoteCypher extends Neo4jQuery5<Connection> {
    @Override
    public String description() {
        return QUERY_STRING;
    }

    @Override
    public Iterator<LdbcQuery5Result> execute(Connection connection, LdbcQuery5 operation) throws DbException {
        try (PreparedStatement preparedStatement = connection.prepareStatement(QUERY_STRING)) {
            preparedStatement.setLong(PERSON_ID, operation.personId());
            preparedStatement.setLong(JOIN_DATE, operation.minDate().getTime());
            preparedStatement.setInt(LIMIT, operation.limit());
            ResultSet resultSet = preparedStatement.executeQuery();
            List<LdbcQuery5Result> results = new ArrayList<>();
            while (resultSet.next()) {
                results.add(resultSetToLdbcQuery5Result(resultSet));
            }
            return results.iterator();
        } catch (SQLException e) {
            throw new DbException("Error while executing query", e);
        }
    }

    private LdbcQuery5Result resultSetToLdbcQuery5Result(ResultSet resultSet) throws SQLException {
        return new LdbcQuery5Result(
                resultSet.getString("forumName"),
                ((Long) resultSet.getLong("postCount")).intValue());
    }
}
