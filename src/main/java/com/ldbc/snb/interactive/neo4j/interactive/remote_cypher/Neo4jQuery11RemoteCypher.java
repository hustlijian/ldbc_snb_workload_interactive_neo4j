package com.ldbc.snb.interactive.neo4j.interactive.remote_cypher;

import com.ldbc.driver.DbException;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery11;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery11Result;
import com.ldbc.snb.interactive.neo4j.interactive.Neo4jQuery11;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Neo4jQuery11RemoteCypher extends Neo4jQuery11<Connection> {
    @Override
    public String description() {
        return QUERY_STRING;
    }

    @Override
    public Iterator<LdbcQuery11Result> execute(Connection connection, LdbcQuery11 operation) throws DbException {
        try (PreparedStatement preparedStatement = connection.prepareStatement(QUERY_STRING)) {
            preparedStatement.setLong(PERSON_ID, operation.personId());
            preparedStatement.setString(COUNTRY_NAME, operation.countryName());
            preparedStatement.setInt(WORK_FROM_YEAR, operation.workFromYear());
            preparedStatement.setInt(LIMIT, operation.limit());
            ResultSet resultSet = preparedStatement.executeQuery();
            List<LdbcQuery11Result> results = new ArrayList<>();
            while (resultSet.next()) {
                results.add(resultSetToLdbcQuery11Result(resultSet));
            }
            return results.iterator();
        } catch (SQLException e) {
            throw new DbException("Error while executing query", e);
        }
    }

    private LdbcQuery11Result resultSetToLdbcQuery11Result(ResultSet resultSet) throws SQLException {
        return new LdbcQuery11Result(
                resultSet.getLong("friendId"),
                resultSet.getString("friendFirstName"),
                resultSet.getString("friendLastName"),
                resultSet.getString("companyName"),
                resultSet.getInt("workFromYear"));
    }
}
