package com.ldbc.snb.interactive.neo4j.interactive.remote_cypher;

import com.ldbc.driver.DbException;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery12;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery12Result;
import com.ldbc.snb.interactive.neo4j.interactive.Neo4jQuery12;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Neo4jQuery12RemoteCypher extends Neo4jQuery12<Connection> {
    @Override
    public String description() {
        return QUERY_STRING;
    }

    @Override
    public Iterator<LdbcQuery12Result> execute(Connection connection, LdbcQuery12 operation) throws DbException {
        try (PreparedStatement preparedStatement = connection.prepareStatement(QUERY_STRING)) {
            preparedStatement.setLong(PERSON_ID, operation.personId());
            preparedStatement.setString(TAG_CLASS_NAME, operation.tagClassName());
            preparedStatement.setInt(LIMIT, operation.limit());
            ResultSet resultSet = preparedStatement.executeQuery();
            List<LdbcQuery12Result> results = new ArrayList<>();
            while (resultSet.next()) {
                results.add(resultSetToLdbcQuery12Result(resultSet));
            }
            return results.iterator();
        } catch (SQLException e) {
            throw new DbException("Error while executing query", e);
        }
    }

    private LdbcQuery12Result resultSetToLdbcQuery12Result(ResultSet resultSet) throws SQLException {
        return new LdbcQuery12Result(
                resultSet.getLong("friendId"),
                resultSet.getString("friendFirstName"),
                resultSet.getString("friendLastName"),
                (Iterable<String>) resultSet.getObject("tagNames"),
                ((Long) resultSet.getLong("count")).intValue());
    }
}
