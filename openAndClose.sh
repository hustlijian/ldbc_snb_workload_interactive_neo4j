mvn clean package -Dmaven.compiler.source=1.7 -Dmaven.compiler.target=1.7
java -cp target/neo4j_importer-0.1-SNAPSHOT.jar com.ldbc.socialnet.neo4j.OpenAndClose -Xmx100G
