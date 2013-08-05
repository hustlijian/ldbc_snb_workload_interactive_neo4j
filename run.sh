mvn clean package -Dmaven.compiler.source=1.6 -Dmaven.compiler.target=1.6
java -cp target/neo4j_importer-0.1-SNAPSHOT.jar com.ldbc.socialnet.neo4j.LdbcSocialNeworkNeo4jImporter -Xmx28G