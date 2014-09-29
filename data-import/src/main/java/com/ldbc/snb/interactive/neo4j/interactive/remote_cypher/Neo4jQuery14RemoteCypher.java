package com.ldbc.snb.interactive.neo4j.interactive.remote_cypher;

import com.ldbc.driver.DbException;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery14;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery14Result;
import com.ldbc.snb.interactive.neo4j.interactive.Neo4jQuery14;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
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
            preparedStatement.setLong(PERSON_ID_1, operation.person1Id());
            preparedStatement.setLong(PERSON_ID_2, operation.person2Id());
            ResultSet resultSet = preparedStatement.executeQuery();
            List<LdbcQuery14Result> results = new ArrayList<>();
            while (resultSet.next()) {
                results.add(resultSetToLdbcQuery14Result(resultSet));
            }
            return results.iterator();
        } catch (SQLException e) {
            throw new DbException("Error while executing query", e);
        }
    }

    private LdbcQuery14Result resultSetToLdbcQuery14Result(ResultSet resultSet) throws SQLException {
        return new LdbcQuery14Result(
                (Iterable<Long>) resultSet.getObject("pathNodeIds"),
                resultSet.getDouble("weight"));
    }
}
