package com.ldbc.snb.interactive.neo4j.interactive;

import com.ldbc.snb.interactive.neo4j.interactive.embedded_cypher.*;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class QueryPrinter {
    public static void main(String[] args) throws IOException {
        writeQueriesToFile("queries.txt");
    }

    public static void writeQueriesToFile(String filePath) throws IOException {
        File file = new File(filePath);
        BufferedWriter writer = new BufferedWriter(new FileWriter(file));
        writer.write("QUERY 1");
        writer.newLine();
        writer.newLine();
        writer.write(new Neo4jQuery1EmbeddedCypher().description());
        writer.newLine();
        writer.newLine();
        writer.write("QUERY 2");
        writer.newLine();
        writer.newLine();
        writer.write(new Neo4jQuery2EmbeddedCypher().description());
        writer.newLine();
        writer.newLine();
        writer.write("QUERY 3");
        writer.newLine();
        writer.newLine();
        writer.write(new Neo4jQuery3EmbeddedCypher().description());
        writer.newLine();
        writer.newLine();
        writer.write("QUERY 4");
        writer.newLine();
        writer.newLine();
        writer.write(new Neo4jQuery4EmbeddedCypher().description());
        writer.newLine();
        writer.newLine();
        writer.write("QUERY 5");
        writer.newLine();
        writer.newLine();
        writer.write(new Neo4jQuery5EmbeddedCypher().description());
        writer.newLine();
        writer.newLine();
        writer.write("QUERY 6");
        writer.newLine();
        writer.newLine();
        writer.write(new Neo4jQuery6EmbeddedCypher().description());
        writer.newLine();
        writer.newLine();
        writer.write("QUERY 7");
        writer.newLine();
        writer.newLine();
        writer.write(new Neo4jQuery7EmbeddedCypher().description());
        writer.newLine();
        writer.newLine();
        writer.write("QUERY 8");
        writer.newLine();
        writer.newLine();
        writer.write(new Neo4jQuery8EmbeddedCypher().description());
        writer.newLine();
        writer.newLine();
        writer.write("QUERY 9");
        writer.newLine();
        writer.newLine();
        writer.write(new Neo4jQuery9EmbeddedCypher().description());
        writer.newLine();
        writer.newLine();
        writer.write("QUERY 10");
        writer.newLine();
        writer.newLine();
        writer.write(new Neo4jQuery10EmbeddedCypher().description());
        writer.newLine();
        writer.newLine();
        writer.write("QUERY 11");
        writer.newLine();
        writer.newLine();
        writer.write(new Neo4jQuery11EmbeddedCypher().description());
        writer.newLine();
        writer.newLine();
        writer.write("QUERY 12");
        writer.newLine();
        writer.newLine();
        writer.write(new Neo4jQuery12EmbeddedCypher().description());
        writer.newLine();
        writer.newLine();
        writer.write("QUERY 13");
        writer.newLine();
        writer.newLine();
        writer.write(new Neo4jQuery13EmbeddedCypher().description());
        writer.newLine();
        writer.newLine();
        writer.write("QUERY 14");
        writer.newLine();
        writer.newLine();
        writer.write(new Neo4jQuery14EmbeddedCypher().description());
        writer.flush();
        writer.close();
    }
}
