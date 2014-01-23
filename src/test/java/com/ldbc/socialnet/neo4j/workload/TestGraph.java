package com.ldbc.socialnet.neo4j.workload;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.Label;
import org.neo4j.helpers.collection.MapUtil;

import com.ldbc.driver.util.Tuple.Tuple2;
import com.ldbc.socialnet.neo4j.workload.TestGraph.TestNodes.TestCities;
import com.ldbc.socialnet.neo4j.workload.TestGraph.TestNodes.TestComments;
import com.ldbc.socialnet.neo4j.workload.TestGraph.TestNodes.TestCompanies;
import com.ldbc.socialnet.neo4j.workload.TestGraph.TestNodes.TestCountries;
import com.ldbc.socialnet.neo4j.workload.TestGraph.TestNodes.TestForums;
import com.ldbc.socialnet.neo4j.workload.TestGraph.TestNodes.TestPersons;
import com.ldbc.socialnet.neo4j.workload.TestGraph.TestNodes.TestPosts;
import com.ldbc.socialnet.neo4j.workload.TestGraph.TestNodes.TestTags;
import com.ldbc.socialnet.neo4j.workload.TestGraph.TestNodes.TestUniversities;
import com.ldbc.socialnet.neo4j.workload.TestGraph.TestRels.TestHasMember;
import com.ldbc.socialnet.neo4j.workload.TestGraph.TestRels.TestStudyAt;
import com.ldbc.socialnet.neo4j.workload.TestGraph.TestRels.TestWorksAt;
import com.ldbc.socialnet.workload.neo4j.Domain;

import static com.ldbc.socialnet.workload.neo4j.Domain.*;

/*
    MATCH (person1:Person), (person2:Person), (person:Person{id:1})
    WHERE NOT((person1)-[:KNOWS]-(person)) AND NOT((person2)-[:KNOWS]-(person)) AND NOT(person1=person) AND NOT(person2=person)
    OPTIONAL MATCH (person1)-[knows:KNOWS]-(person2)
    OPTIONAL MATCH (post1:Post)-[hc1:HAS_CREATOR]->(person1)
    OPTIONAL MATCH (post2:Post)-[hc2:HAS_CREATOR]->(person2)
    OPTIONAL MATCH (person)-[ili:IS_LOCATED_IN]->(personCity:City)
    OPTIONAL MATCH (person1)-[ili1:IS_LOCATED_IN]->(person1City:City)
    OPTIONAL MATCH (person2)-[ili2:IS_LOCATED_IN]->(person2City:City)
    RETURN person1,knows,person2,post1,post2,hc1,hc2,ili1,person1City,ili2,person2City,person,ili,personCity
 */

public class TestGraph
{
    public static class Creator
    {
        public static String createGraphQuery()
        {
            return "CREATE\n"

            + "\n// --- NODES ---\n\n"

            /*
             * Forums
             */

            + " (cakesAndPiesForum:"
                   + Nodes.Forum
                   + " {cakesAndPiesForum}),"

                   + " (redditAddictsForum:"
                   + Nodes.Forum
                   + " {redditAddictsForum}),\n"

                   + " (floatingBoatsForum:"
                   + Nodes.Forum
                   + " {floatingBoatsForum}),"

                   + " (kiwisSheepAndBungyJumpingForum:"
                   + Nodes.Forum
                   + " {kiwisSheepAndBungyJumpingForum}),\n"

                   /*
                    * Tags
                    */

                   + " (cake:"
                   + Nodes.Tag
                   + " {cake}), "

                   + "(pie:"
                   + Nodes.Tag
                   + " {pie}), "

                   + "(lol:"
                   + Nodes.Tag
                   + " {lol}), "

                   + "(yolo:"
                   + Nodes.Tag
                   + " {yolo}), "

                   + "(wtf:"
                   + Nodes.Tag
                   + " {wtf}),\n"

                   /*
                    * Persons
                    */

                   + " (alex:"
                   + Nodes.Person
                   + " {alex}), "

                   + "(aiya:"
                   + Nodes.Person
                   + " {aiya}), "

                   + "(jake:"
                   + Nodes.Person
                   + " {jake}), "

                   + "(peter:"
                   + Nodes.Person
                   + " {peter}),\n"

                   + "(stranger:"
                   + Nodes.Person
                   + " {stranger}), "

                   + "(nicky:"
                   + Nodes.Person
                   + " {nicky}),"

                   + "(unknown:"
                   + Nodes.Person
                   + " {unknown}),\n"

                   /*
                   * Cities
                   */

                   + " (auckland:"
                   + Nodes.Place
                   + ":"
                   + Place.Type.City
                   + " {auckland}), (stockholm:"
                   + Nodes.Place
                   + ":"
                   + Place.Type.City
                   + " {stockholm}),"

                   + " (munich:"
                   + Nodes.Place
                   + ":"
                   + Place.Type.City
                   + " {munich}),\n"

                   + " (london:"
                   + Nodes.Place
                   + ":"
                   + Place.Type.City
                   + " {london}),"

                   + " (melbourne:"
                   + Nodes.Place
                   + ":"
                   + Place.Type.City
                   + " {melbourne}),\n"

                   /*
                   * Countries
                   */

                   + " (se:"
                   + Nodes.Place
                   + ":"
                   + Place.Type.Country
                   + " {sweden}),"

                   + " (nz:"
                   + Nodes.Place
                   + ":"
                   + Place.Type.Country
                   + " {new_zealand}),\n"

                   + " (de:"
                   + Nodes.Place
                   + ":"
                   + Place.Type.Country
                   + " {germany}),"

                   + " (au:"
                   + Nodes.Place
                   + ":"
                   + Place.Type.Country
                   + " {australia}),\n"

                   + " (uk:"
                   + Nodes.Place
                   + ":"
                   + Place.Type.Country
                   + " {england}),\n"

                   /*
                   * Universities
                   */

                   + " (aut:"
                   + Nodes.Organisation
                   + ":"
                   + Organisation.Type.University
                   + " {aut}),"

                   + " (kth:"
                   + Nodes.Organisation
                   + ":"
                   + Organisation.Type.University
                   + " {kth}),\n"

                   /*
                   * Companies
                   */

                   + " (sics:"
                   + Nodes.Organisation
                   + ":"
                   + Organisation.Type.Company
                   + " {sics}),"

                   + " (neo:"
                   + Nodes.Organisation
                   + ":"
                   + Organisation.Type.Company
                   + " {neo}),"

                   + " (hot:"
                   + Nodes.Organisation
                   + ":"
                   + Organisation.Type.Company
                   + " {hot}),\n"

                   /*
                   * Posts
                   */

                   + " (jakePost1:"
                   + Nodes.Post
                   + " {jakePost1}), (jakePost2:"
                   + Nodes.Post
                   + " {jakePost2}),"

                   + " (jakePost3:"
                   + Nodes.Post
                   + " {jakePost3}),\n"

                   + " (peterPost1:"
                   + Nodes.Post
                   + " {peterPost1}), (aiyaPost1:"
                   + Nodes.Post
                   + " {aiyaPost1}),"

                   + " (aiyaPost2:"
                   + Nodes.Post
                   + " {aiyaPost2}), (aiyaPost3:"
                   + Nodes.Post
                   + " {aiyaPost3}),\n"

                   + " (strangerPost1:"
                   + Nodes.Post
                   + " {strangerPost1}),"

                   + " (strangerPost2:"
                   + Nodes.Post
                   + " {strangerPost2}),"

                   + " (nickyPost1:"
                   + Nodes.Post
                   + " {nickyPost1}),\n"

                   + " (unknownPost1:"
                   + Nodes.Post
                   + " {unknownPost1}),"

                   + " (unknownPost2:"
                   + Nodes.Post
                   + " {unknownPost2}),\n"

                   /*
                   * Comments
                   */
                   + " (aiyaComment1:"
                   + Nodes.Comment
                   + " {aiyaComment1}),"

                   + " (aiyaComment2:"
                   + Nodes.Comment
                   + " {aiyaComment2}),\n"

                   + " (strangerComment1:"
                   + Nodes.Comment
                   + " {strangerComment1}),"

                   + " (jakeComment1:"
                   + Nodes.Comment
                   + " {jakeComment1}),"

                   + " (alexComment1:"
                   + Nodes.Comment
                   + " {alexComment1}),\n"

                   + "\n// --- RELATIONSHIPS ---\n\n"

                   /*
                    * City-Country
                    */

                   + " (stockholm)-[:"
                   + Rels.IS_LOCATED_IN
                   + "]->(se),"

                   + " (auckland)-[:"
                   + Rels.IS_LOCATED_IN
                   + "]->(nz),\n"

                   + " (munich)-[:"
                   + Rels.IS_LOCATED_IN
                   + "]->(de),"

                   + " (london)-[:"
                   + Rels.IS_LOCATED_IN
                   + "]->(uk),"

                   + " (melbourne)-[:"
                   + Rels.IS_LOCATED_IN
                   + "]->(au),\n"

                   /*
                    * Organization-Country
                    */

                   + " (neo)<-[:"
                   + Rels.WORKS_AT
                   + " {alexWorkAtNeo}]-(alex),"

                   + " (stockholm)<-[:"
                   + Rels.IS_LOCATED_IN
                   + "]-(kth),\n"

                   + " (auckland)<-[:"
                   + Rels.IS_LOCATED_IN
                   + "]-(aut),"

                   + " (se)<-[:"
                   + Rels.IS_LOCATED_IN
                   + "]-(sics),\n"

                   + " (nz)<-[:"
                   + Rels.IS_LOCATED_IN
                   + "]-(hot),\n"

                   /*
                    * Nicky
                    */

                   + " (nicky)-[:"
                   + Rels.IS_LOCATED_IN
                   + "]->(london),\n"

                   /*
                    * Alex
                    */

                   + " (alex)-[:"
                   + Rels.IS_LOCATED_IN
                   + "]->(stockholm),\n"

                   + " (se)<-[:"
                   + Rels.IS_LOCATED_IN
                   + "]-(neo),\n"

                   + " (kth)<-[:"
                   + Rels.STUDY_AT
                   + " {alexStudyAtKth}]-(alex),\n"

                   + " (aut)<-[:"
                   + Rels.STUDY_AT
                   + " {alexStudyAtAut}]-(alex),\n"

                   + " (sics)<-[:"
                   + Rels.WORKS_AT
                   + " {alexWorkAtSics}]-(alex),\n"

                   /*
                    * Aiya
                    */

                   + " (aiya)-[:"
                   + Rels.IS_LOCATED_IN
                   + "]->(auckland),\n"

                   + " (hot)<-[:"
                   + Rels.WORKS_AT
                   + " {aiyaWorkAtHot}]-(aiya),\n"

                   /*
                    * Jake
                    */

                   + " (jake)-[:"
                   + Rels.IS_LOCATED_IN
                   + "]->(stockholm),\n"

                   /*
                   * Peter
                   */

                   + " (peter)-[:"
                   + Rels.IS_LOCATED_IN
                   + "]->(munich),\n"

                   /*
                    * Stranger
                    */

                   + " (stranger)-[:"
                   + Rels.IS_LOCATED_IN
                   + "]->(stockholm),\n"

                   /*
                    * Unknown
                    */

                   + " (unknown)-[:"
                   + Rels.IS_LOCATED_IN
                   + "]->(stockholm),\n"

                   /*
                    * Forum-Person (moderator)
                    */

                   + " (cakesAndPiesForum)-[:"
                   + Rels.HAS_MODERATOR
                   + "]->(alex),"

                   + " (redditAddictsForum)-[:"
                   + Rels.HAS_MODERATOR
                   + "]->(jake),\n"

                   + " (floatingBoatsForum)-[:"
                   + Rels.HAS_MODERATOR
                   + "]->(jake),"

                   + " (kiwisSheepAndBungyJumpingForum)-[:"
                   + Rels.HAS_MODERATOR
                   + "]->(aiya),\n"

                   /*
                    * Forum-Person (member)
                    */

                   + " (cakesAndPiesForum)-[:"
                   + Rels.HAS_MEMBER
                   + " {cakesAndPiesHasMemberAlex}]->(alex),"

                   + " (cakesAndPiesForum)-[:"
                   + Rels.HAS_MEMBER
                   + " {cakesAndPiesHasMemberAiya}]->(aiya),\n"

                   + " (cakesAndPiesForum)-[:"
                   + Rels.HAS_MEMBER
                   + " {cakesAndPiesHasMemberStranger}]->(stranger),"

                   + " (cakesAndPiesForum)-[:"
                   + Rels.HAS_MEMBER
                   + " {cakesAndPiesHasMemberJake}]->(jake),\n"

                   + " (cakesAndPiesForum)-[:"
                   + Rels.HAS_MEMBER
                   + " {cakesAndPiesHasMemberNicky}]->(nicky),\n"

                   + " (redditAddictsForum)-[:"
                   + Rels.HAS_MEMBER
                   + " {redditAddictsHasMemberJake}]->(jake),"

                   + " (floatingBoatsForum)-[:"
                   + Rels.HAS_MEMBER
                   + " {floatingBoatsHasMemberAlex}]->(alex),\n"

                   + " (floatingBoatsForum)-[:"
                   + Rels.HAS_MEMBER
                   + " {floatingBoatsHasMemberJake}]->(jake),"

                   + " (floatingBoatsForum)-[:"
                   + Rels.HAS_MEMBER
                   + " {floatingBoatsHasMemberPeter}]->(peter),\n"

                   + " (kiwisSheepAndBungyJumpingForum)-[:"
                   + Rels.HAS_MEMBER
                   + " {kiwisSheepAndBungyJumpingHasMemberAiya}]->(aiya),\n"

                   + " (kiwisSheepAndBungyJumpingForum)-[:"
                   + Rels.HAS_MEMBER
                   + " {kiwisSheepAndBungyJumpingHasMemberAlex}]->(alex)\n"

                   /*
                   * Person-Person
                   */

                   + "FOREACH (n IN [jake, aiya, peter] | CREATE (alex)-[:"
                   + Rels.KNOWS
                   + "]->(n) )\n"

                   + "FOREACH (n IN [nicky] | CREATE (aiya)-[:"
                   + Rels.KNOWS
                   + "]->(n) )\n"

                   /*
                   * Post-Country
                   */

                   + "FOREACH (n IN [jakePost1,aiyaPost2, aiyaPost3] | CREATE (n)-[:"
                   + Rels.IS_LOCATED_IN
                   + "]->(nz) )\n"

                   + "FOREACH (n IN [nickyPost1,jakePost2,jakePost3,aiyaPost1,unknownPost1,unknownPost2,strangerPost2] | CREATE (n)-[:"
                   + Rels.IS_LOCATED_IN
                   + "]->(se) )\n"

                   + "FOREACH (n IN [peterPost1] | CREATE (n)-[:"
                   + Rels.IS_LOCATED_IN
                   + "]->(de) )\n"

                   + "FOREACH (n IN [strangerPost1] | CREATE (n)-[:"
                   + Rels.IS_LOCATED_IN
                   + "]->(au) )\n"

//                   + "FOREACH (n IN [] | CREATE (n)-[:"
//                   + Rels.IS_LOCATED_IN
//                   + "]->(uk) )\n"

                   /*
                   * Post-Person
                   */

                   + "FOREACH (n IN [jakePost1, jakePost2, jakePost3] | CREATE (n)-[:"
                   + Rels.HAS_CREATOR
                   + "]->(jake) )\n"

                   + "FOREACH (n IN [aiyaPost1, aiyaPost2, aiyaPost3] | CREATE (n)-[:"
                   + Rels.HAS_CREATOR
                   + "]->(aiya) )\n"

                   + "FOREACH (n IN [peterPost1] | CREATE (n)-[:"
                   + Rels.HAS_CREATOR
                   + "]->(peter) )\n"

                   + "FOREACH (n IN [strangerPost1, strangerPost2] | CREATE (n)-[:"
                   + Rels.HAS_CREATOR
                   + "]->(stranger) )\n"

                   + "FOREACH (n IN [unknownPost1, unknownPost2] | CREATE (n)-[:"
                   + Rels.HAS_CREATOR
                   + "]->(unknown) )\n"

                   + "FOREACH (n IN [nickyPost1] | CREATE (n)-[:"
                   + Rels.HAS_CREATOR
                   + "]->(nicky) )\n"

                   /*
                   * Post-Tag
                   */

                   + "FOREACH (n IN [jakePost1,jakePost2,aiyaPost1] | CREATE (n)-[:"
                   + Rels.HAS_TAG
                   + "]->(yolo) )\n"

                   + "FOREACH (n IN [jakePost3,nickyPost1,unknownPost2] | CREATE (n)-[:"
                   + Rels.HAS_TAG
                   + "]->(wtf) )\n"

                   + "FOREACH (n IN [jakePost3,peterPost1,aiyaPost2,strangerPost2,nickyPost1,unknownPost1] | CREATE (n)-[:"
                   + Rels.HAS_TAG
                   + "]->(lol) )\n"

                   + "FOREACH (n IN [jakePost3,peterPost1,aiyaPost1,aiyaPost3,strangerPost1] | CREATE (n)-[:"
                   + Rels.HAS_TAG
                   + "]->(pie) )\n"

                   + "FOREACH (n IN [jakePost1,aiyaPost1,aiyaPost3,strangerPost1,nickyPost1] | CREATE (n)-[:"
                   + Rels.HAS_TAG
                   + "]->(cake) )\n"

                   /*
                    * Post-Forum
                    */

                   + "FOREACH (n IN [jakePost1, jakePost2, aiyaPost1, aiyaPost2, strangerPost1, strangerPost2, nickyPost1]|"
                   + " CREATE (cakesAndPiesForum)-[:" + Rels.CONTAINER_OF + "]->(n) )\n"

                   + "FOREACH (n IN [jakePost3, peterPost1] | CREATE (floatingBoatsForum)-[:" + Rels.CONTAINER_OF
                   + "]->(n) )\n"

                   + "FOREACH (n IN [aiyaPost3] | CREATE (kiwisSheepAndBungyJumpingForum)-[:" + Rels.CONTAINER_OF
                   + "]->(n) )\n"

                   /*
                    * Comment-Person
                    */

                   + "FOREACH (n IN [aiyaComment1, aiyaComment2] | CREATE (n)-[:" + Rels.HAS_CREATOR + "]->(aiya) )\n"

                   + "FOREACH (n IN [alexComment1] | CREATE (n)-[:" + Rels.HAS_CREATOR + "]->(alex) )\n"

                   + "FOREACH (n IN [jakeComment1] | CREATE (n)-[:" + Rels.HAS_CREATOR + "]->(jake) )\n"

                   + "FOREACH (n IN [strangerComment1] | CREATE (n)-[:" + Rels.HAS_CREATOR + "]->(stranger) )\n"

                   /*
                    * Comment-Post
                    */

                   + "FOREACH (n IN [aiyaComment1, strangerComment1] | CREATE (n)-[:" + Rels.REPLY_OF
                   + "]->(jakePost2) )\n"

                   + "FOREACH (n IN [aiyaComment2] | CREATE (n)-[:" + Rels.REPLY_OF + "]->(strangerComment1) )\n"

                   + "FOREACH (n IN [jakeComment1] | CREATE (n)-[:" + Rels.REPLY_OF + "]->(peterPost1) )\n"

                   + "FOREACH (n IN [alexComment1] | CREATE (n)-[:" + Rels.REPLY_OF + "]->(aiyaPost2) )\n";
        }

        public static Map<String, Object> createGraphQueryParams()
        {
            return MapUtil.map( "cakesAndPiesForum", TestForums.cakesAndPies(), "redditAddictsForum",
                    TestForums.redditAddicts(), "floatingBoatsForum", TestForums.floatingBoats(),
                    "kiwisSheepAndBungyJumpingForum", TestForums.kiwisSheepAndBungyJumping(), "cake", TestTags.cake(),
                    "pie", TestTags.pie(), "lol", TestTags.lol(), "yolo", TestTags.yolo(), "wtf", TestTags.wtf(),
                    "alex", TestPersons.alex(), "aiya", TestPersons.aiya(), "jake", TestPersons.jake(), "peter",
                    TestPersons.peter(), "stranger", TestPersons.stranger(), "nicky", TestPersons.nicky(), "unknown",
                    TestPersons.unknownGuy(), "auckland", TestCities.auckland(), "stockholm", TestCities.stockholm(),
                    "munich", TestCities.munich(), "london", TestCities.london(), "melbourne", TestCities.melbourne(),
                    "sweden", TestCountries.sweden(), "new_zealand", TestCountries.newZealand(), "germany",
                    TestCountries.germany(), "australia", TestCountries.australia(), "england",
                    TestCountries.england(), "aut", TestUniversities.aut(), "kth", TestUniversities.kth(), "sics",
                    TestCompanies.sics(), "neo", TestCompanies.neo(), "hot", TestCompanies.hot(), "alexWorkAtSics",
                    TestWorksAt.alexWorkAtSics(), "alexWorkAtNeo", TestWorksAt.alexWorkAtNeo(), "aiyaWorkAtHot",
                    TestWorksAt.aiyaWorkAtHot(), "alexStudyAtAut", TestStudyAt.alexStudyAtAut(), "alexStudyAtKth",
                    TestStudyAt.alexStudyAtKth(), "jakePost1", TestPosts.jake1(), "jakePost2", TestPosts.jake2(),
                    "jakePost3", TestPosts.jake3(), "peterPost1", TestPosts.peter1(), "aiyaPost1", TestPosts.aiya1(),
                    "aiyaPost2", TestPosts.aiya2(), "aiyaPost3", TestPosts.aiya3(), "strangerPost1",
                    TestPosts.stranger1(), "strangerPost2", TestPosts.stranger2(), "unknownPost1",
                    TestPosts.unknown1(), "unknownPost2", TestPosts.unknown2(), "nickyPost1", TestPosts.nicky1(),
                    "cakesAndPiesHasMemberAlex", TestHasMember.cakesAndPiesHasMemberAlex(),
                    "cakesAndPiesHasMemberAiya", TestHasMember.cakesAndPiesHasMemberAiya(),
                    "cakesAndPiesHasMemberStranger", TestHasMember.cakesAndPiesHasMemberStranger(),
                    "cakesAndPiesHasMemberJake", TestHasMember.cakesAndPiesHasMemberJake(),
                    "cakesAndPiesHasMemberNicky", TestHasMember.cakesAndPiesHasMemberNicky(),
                    "redditAddictsHasMemberJake", TestHasMember.redditAddictsHasMemberJake(),
                    "floatingBoatsHasMemberJake", TestHasMember.floatingBoatsHasMemberJake(),
                    "floatingBoatsHasMemberAlex", TestHasMember.floatingBoatsHasMemberAlex(),
                    "floatingBoatsHasMemberPeter", TestHasMember.floatingBoatsHasMemberPeter(),
                    "kiwisSheepAndBungyJumpingHasMemberAiya", TestHasMember.kiwisSheepAndBungyJumpingHasMemberAiya(),
                    "kiwisSheepAndBungyJumpingHasMemberAlex", TestHasMember.kiwisSheepAndBungyJumpingHasMemberAlex(),
                    "aiyaComment1", TestComments.aiya1(), "aiyaComment2", TestComments.aiya2(), "strangerComment1",
                    TestComments.stranger1(), "jakeComment1", TestComments.jake1(), "alexComment1",
                    TestComments.alex1() );
        }

        public static Iterable<String> createIndexQueries()
        {
            List<String> createIndexQueries = new ArrayList<String>();
            for ( Tuple2<Label, String> labelAndProperty : labelPropertyPairsToIndex() )
            {
                createIndexQueries.add( "CREATE INDEX ON :" + labelAndProperty._1() + "(" + labelAndProperty._2() + ")" );
            }
            return createIndexQueries;
        }
    }

    protected static class TestRels
    {
        protected static class TestHasMember
        {

            // cakesAndPies - 2013, Calendar.OCTOBER, 2
            protected static Map<String, Object> cakesAndPiesHasMemberAlex()
            {
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set( 2013, Calendar.OCTOBER, 2 );
                long joinDate = c.getTimeInMillis();
                return MapUtil.map( HasMember.JOIN_DATE, joinDate );
            }

            protected static Map<String, Object> cakesAndPiesHasMemberAiya()
            {
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set( 2013, Calendar.OCTOBER, 3 );
                long joinDate = c.getTimeInMillis();
                return MapUtil.map( HasMember.JOIN_DATE, joinDate );
            }

            protected static Map<String, Object> cakesAndPiesHasMemberStranger()
            {
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set( 2013, Calendar.OCTOBER, 4 );
                long joinDate = c.getTimeInMillis();
                return MapUtil.map( HasMember.JOIN_DATE, joinDate );
            }

            protected static Map<String, Object> cakesAndPiesHasMemberJake()
            {
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set( 2013, Calendar.OCTOBER, 8 );
                long joinDate = c.getTimeInMillis();
                return MapUtil.map( HasMember.JOIN_DATE, joinDate );
            }

            protected static Map<String, Object> cakesAndPiesHasMemberNicky()
            {
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set( 2013, Calendar.OCTOBER, 9 );
                long joinDate = c.getTimeInMillis();
                return MapUtil.map( HasMember.JOIN_DATE, joinDate );
            }

            // redditAddicts - 2013, Calendar.OCTOBER, 22
            protected static Map<String, Object> redditAddictsHasMemberJake()
            {
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set( 2013, Calendar.OCTOBER, 22 );
                long joinDate = c.getTimeInMillis();
                return MapUtil.map( HasMember.JOIN_DATE, joinDate );
            }

            // floatingBoats - 2013, Calendar.NOVEMBER, 13
            protected static Map<String, Object> floatingBoatsHasMemberJake()
            {
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set( 2013, Calendar.NOVEMBER, 13 );
                long joinDate = c.getTimeInMillis();
                return MapUtil.map( HasMember.JOIN_DATE, joinDate );
            }

            protected static Map<String, Object> floatingBoatsHasMemberAlex()
            {
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set( 2013, Calendar.NOVEMBER, 14 );
                long joinDate = c.getTimeInMillis();
                return MapUtil.map( HasMember.JOIN_DATE, joinDate );
            }

            protected static Map<String, Object> floatingBoatsHasMemberPeter()
            {
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set( 2013, Calendar.NOVEMBER, 16 );
                long joinDate = c.getTimeInMillis();
                return MapUtil.map( HasMember.JOIN_DATE, joinDate );
            }

            // kiwisSheepAndBungyJumping - 2013, Calendar.NOVEMBER, 1
            protected static Map<String, Object> kiwisSheepAndBungyJumpingHasMemberAiya()
            {
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set( 2013, Calendar.NOVEMBER, 1 );
                long joinDate = c.getTimeInMillis();
                return MapUtil.map( HasMember.JOIN_DATE, joinDate );
            }

            protected static Map<String, Object> kiwisSheepAndBungyJumpingHasMemberAlex()
            {
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set( 2013, Calendar.NOVEMBER, 4 );
                long joinDate = c.getTimeInMillis();
                return MapUtil.map( HasMember.JOIN_DATE, joinDate );
            }
        }

        protected static class TestWorksAt
        {
            protected static Map<String, Object> alexWorkAtSics()
            {
                return MapUtil.map( WorksAt.WORK_FROM, 2010 );
            }

            protected static Map<String, Object> alexWorkAtNeo()
            {
                return MapUtil.map( WorksAt.WORK_FROM, 2012 );
            }

            protected static Map<String, Object> aiyaWorkAtHot()
            {
                return MapUtil.map( WorksAt.WORK_FROM, 2005 );
            }
        }

        protected static class TestStudyAt
        {
            protected static Map<String, Object> alexStudyAtAut()
            {
                return MapUtil.map( StudiesAt.CLASS_YEAR, 2006 );
            }

            protected static Map<String, Object> alexStudyAtKth()
            {
                return MapUtil.map( StudiesAt.CLASS_YEAR, 2008 );
            }
        }
    }

    protected static class TestNodes
    {
        protected static class TestCompanies
        {
            protected static Map<String, Object> sics()
            {
                return MapUtil.map( Organisation.NAME, "swedish institute of computer science" );
            }

            protected static Map<String, Object> neo()
            {
                return MapUtil.map( Place.NAME, "neo technology" );
            }

            protected static Map<String, Object> hot()
            {
                return MapUtil.map( Place.NAME, "house of travel" );
            }
        }

        protected static class TestUniversities
        {
            protected static Map<String, Object> aut()
            {
                return MapUtil.map( Organisation.NAME, "auckland university of technology" );
            }

            protected static Map<String, Object> kth()
            {
                return MapUtil.map( Place.NAME, "royal institute of technology" );
            }
        }

        protected static class TestCountries
        {
            protected static Map<String, Object> sweden()
            {
                return MapUtil.map( Place.NAME, "sweden" );
            }

            protected static Map<String, Object> newZealand()
            {
                return MapUtil.map( Place.NAME, "new zealand" );
            }

            protected static Map<String, Object> germany()
            {
                return MapUtil.map( Place.NAME, "germany" );
            }

            protected static Map<String, Object> australia()
            {
                return MapUtil.map( Place.NAME, "australia" );
            }

            protected static Map<String, Object> england()
            {
                return MapUtil.map( Place.NAME, "england" );
            }
        }

        protected static class TestCities
        {

            protected static Map<String, Object> munich()
            {
                return MapUtil.map( Place.NAME, "munich" );
            }

            protected static Map<String, Object> melbourne()
            {
                return MapUtil.map( Place.NAME, "melbourne" );
            }

            protected static Map<String, Object> malmo()
            {
                return MapUtil.map( Place.NAME, "malmö" );
            }

            protected static Map<String, Object> stockholm()
            {
                return MapUtil.map( Place.NAME, "stockholm" );
            }

            protected static Map<String, Object> auckland()
            {
                return MapUtil.map( Place.NAME, "auckland" );
            }

            protected static Map<String, Object> whangarei()
            {
                return MapUtil.map( Place.NAME, "whangarei" );
            }

            protected static Map<String, Object> london()
            {
                return MapUtil.map( Place.NAME, "london" );
            }
        }

        protected static class TestPersons
        {
            protected static Map<String, Object> alex()
            {
                Map<String, Object> params = new HashMap<String, Object>();
                params.put( Person.ID, 1L );
                params.put( Person.FIRST_NAME, "alex" );
                params.put( Person.LAST_NAME, "averbuch" );
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set( 2012, Calendar.JUNE, 6 );
                long creationDate = c.getTimeInMillis();
                params.put( Person.CREATION_DATE, creationDate );
                c.set( 1982, Calendar.JANUARY, 23 );
                long birthday = c.getTimeInMillis();
                params.put( Person.BIRTHDAY, birthday );
                params.put( Person.BROWSER_USED, "chrome" );
                params.put( Person.EMAIL_ADDRESSES, new String[] { "alex.averbuch@gmail.com",
                        "alex.averbuch@neotechnology.com" } );
                params.put( Person.GENDER, "male" );
                params.put( Person.LANGUAGES, new String[] { "english", "swedish" } );
                params.put( Person.LOCATION_IP, "192.168.42.24" );
                return params;
            }

            protected static Map<String, Object> aiya()
            {
                Map<String, Object> params = new HashMap<String, Object>();
                params.put( Person.ID, 2L );
                params.put( Person.FIRST_NAME, "aiya" );
                params.put( Person.LAST_NAME, "thorpe" );
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set( 2013, Calendar.MAY, 19 );
                long creationDate = c.getTimeInMillis();
                params.put( Person.CREATION_DATE, creationDate );
                c.set( 1983, Calendar.SEPTEMBER, 8 );
                long birthday = c.getTimeInMillis();
                params.put( Person.BIRTHDAY, birthday );
                params.put( Person.BROWSER_USED, "safari" );
                params.put( Person.EMAIL_ADDRESSES, new String[] { "aiya.thorpe@gmail.com" } );
                params.put( Person.GENDER, "female" );
                params.put( Person.LANGUAGES, new String[] { "english" } );
                params.put( Person.LOCATION_IP, "192.161.48.1" );
                return params;
            }

            protected static Map<String, Object> jake()
            {
                Map<String, Object> params = new HashMap<String, Object>();
                params.put( Person.ID, 3L );
                params.put( Person.FIRST_NAME, "jacob" );
                params.put( Person.LAST_NAME, "hansson" );
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set( 2013, Calendar.SEPTEMBER, 10 );
                long creationDate = c.getTimeInMillis();
                params.put( Person.CREATION_DATE, creationDate );
                c.set( 1987, Calendar.JULY, 21 );
                long birthday = c.getTimeInMillis();
                params.put( Person.BIRTHDAY, birthday );
                params.put( Person.BROWSER_USED, "safari" );
                params.put( Person.EMAIL_ADDRESSES, new String[] { "jakewins@gmail.com", "jake@neotechnology.com" } );
                params.put( Person.GENDER, "male" );
                params.put( Person.LANGUAGES, new String[] { "english", "swedish" } );
                params.put( Person.LOCATION_IP, "172.124.98.31" );
                return params;
            }

            protected static Map<String, Object> peter()
            {
                Map<String, Object> params = new HashMap<String, Object>();
                params.put( Person.ID, 4L );
                params.put( Person.FIRST_NAME, "peter" );
                params.put( Person.LAST_NAME, "rentschler" );
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set( 2013, Calendar.JANUARY, 5 );
                long creationDate = c.getTimeInMillis();
                params.put( Person.CREATION_DATE, creationDate );
                c.set( 1982, Calendar.JUNE, 5 );
                long birthday = c.getTimeInMillis();
                params.put( Person.BIRTHDAY, birthday );
                params.put( Person.BROWSER_USED, "firefox" );
                params.put( Person.EMAIL_ADDRESSES, new String[] { "peter.rentschler@gmx.de" } );
                params.put( Person.GENDER, "male" );
                params.put( Person.LANGUAGES, new String[] { "english", "german" } );
                params.put( Person.LOCATION_IP, "12.24.158.11" );
                return params;
            }

            protected static Map<String, Object> stranger()
            {
                Map<String, Object> params = new HashMap<String, Object>();
                params.put( Person.ID, 5L );
                params.put( Person.FIRST_NAME, "stranger" );
                params.put( Person.LAST_NAME, "dude" );
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set( 2012, Calendar.OCTOBER, 15 );
                long creationDate = c.getTimeInMillis();
                params.put( Person.CREATION_DATE, creationDate );
                c.set( 1985, Calendar.FEBRUARY, 11 );
                long birthday = c.getTimeInMillis();
                params.put( Person.BIRTHDAY, birthday );
                params.put( Person.BROWSER_USED, "internet explorer" );
                params.put( Person.EMAIL_ADDRESSES, new String[] { "dr.strange@love.com" } );
                params.put( Person.GENDER, "male" );
                params.put( Person.LANGUAGES, new String[] { "english" } );
                params.put( Person.LOCATION_IP, "12.24.158.11" );
                return params;
            }

            protected static Map<String, Object> nicky()
            {
                Map<String, Object> params = new HashMap<String, Object>();
                params.put( Person.ID, 6L );
                params.put( Person.FIRST_NAME, "nicky" );
                params.put( Person.LAST_NAME, "toothill" );
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set( 2013, Calendar.JUNE, 8 );
                long creationDate = c.getTimeInMillis();
                params.put( Person.CREATION_DATE, creationDate );
                c.set( 1982, Calendar.AUGUST, 11 );
                long birthday = c.getTimeInMillis();
                params.put( Person.BIRTHDAY, birthday );
                params.put( Person.BROWSER_USED, "safari" );
                params.put( Person.EMAIL_ADDRESSES, new String[] { "nicky@provider.com" } );
                params.put( Person.GENDER, "female" );
                params.put( Person.LANGUAGES, new String[] { "english", "spanish" } );
                params.put( Person.LOCATION_IP, "12.171.48.1" );
                return params;
            }

            protected static Map<String, Object> unknownGuy()
            {
                Map<String, Object> params = new HashMap<String, Object>();
                params.put( Person.ID, 7L );
                params.put( Person.FIRST_NAME, "unknown" );
                params.put( Person.LAST_NAME, "guy" );
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set( 2012, Calendar.OCTOBER, 18 );
                long creationDate = c.getTimeInMillis();
                params.put( Person.CREATION_DATE, creationDate );
                c.set( 1989, Calendar.MARCH, 21 );
                long birthday = c.getTimeInMillis();
                params.put( Person.BIRTHDAY, birthday );
                params.put( Person.BROWSER_USED, "firefox" );
                params.put( Person.EMAIL_ADDRESSES, new String[] { "unknown@email.com" } );
                params.put( Person.GENDER, "male" );
                params.put( Person.LANGUAGES, new String[] { "english" } );
                params.put( Person.LOCATION_IP, "112.216.53.199" );
                return params;
            }

        }

        protected static class TestPosts
        {
            protected static Map<String, Object> jake1()
            {
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set( 2013, Calendar.SEPTEMBER, 5, 0, 1, 0 );
                long creationDate = c.getTimeInMillis();

                Map<String, Object> params = new HashMap<String, Object>();
                params.put( Post.ID, 1L );
                params.put( Post.CONTENT, "[jake1] hello" );
                params.put( Post.LANGUAGE, new String[] { "english" } );
                params.put( Post.IMAGE_FILE, "some image file" );
                params.put( Post.CREATION_DATE, creationDate );
                params.put( Post.BROWSER_USED, "safari" );
                params.put( Post.LOCATION_IP, "31.55.91.141" );
                return params;
            }

            protected static Map<String, Object> jake2()
            {
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set( 2013, Calendar.SEPTEMBER, 5, 1, 0, 0 );
                long creationDate = c.getTimeInMillis();

                Map<String, Object> params = new HashMap<String, Object>();
                params.put( Post.ID, 2L );
                params.put( Post.CONTENT, "[jake2] hej" );
                params.put( Post.LANGUAGE, new String[] { "swedish" } );
                params.put( Post.IMAGE_FILE, "some image file" );
                params.put( Post.CREATION_DATE, creationDate );
                params.put( Post.BROWSER_USED, "safari" );
                params.put( Post.LOCATION_IP, "31.55.91.142" );
                return params;
            }

            protected static Map<String, Object> jake3()
            {
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set( 2013, Calendar.SEPTEMBER, 7, 0, 0, 0 );
                long creationDate = c.getTimeInMillis();

                Map<String, Object> params = new HashMap<String, Object>();
                params.put( Post.ID, 3L );
                params.put( Post.CONTENT, "[jake3] tjena" );
                params.put( Post.LANGUAGE, new String[] { "swedish" } );
                params.put( Post.IMAGE_FILE, "some image file" );
                params.put( Post.CREATION_DATE, creationDate );
                params.put( Post.BROWSER_USED, "safari" );
                params.put( Post.LOCATION_IP, "31.55.91.143" );
                return params;
            }

            protected static Map<String, Object> peter1()
            {
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set( 2013, Calendar.SEPTEMBER, 7, 1, 0, 0 );
                long creationDate = c.getTimeInMillis();

                Map<String, Object> params = new HashMap<String, Object>();
                params.put( Post.ID, 4L );
                params.put( Post.CONTENT, "[peter1] hallo" );
                params.put( Post.LANGUAGE, new String[] { "german" } );
                params.put( Post.IMAGE_FILE, "some image file" );
                params.put( Post.CREATION_DATE, creationDate );
                params.put( Post.BROWSER_USED, "firefox" );
                params.put( Post.LOCATION_IP, "31.55.91.241" );
                return params;
            }

            protected static Map<String, Object> aiya1()
            {
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set( 2013, Calendar.SEPTEMBER, 6, 0, 0, 0 );
                long creationDate = c.getTimeInMillis();

                Map<String, Object> params = new HashMap<String, Object>();
                params.put( Post.ID, 5L );
                params.put( Post.CONTENT, "[aiya1] kia ora" );
                params.put( Post.LANGUAGE, new String[] { "english" } );
                params.put( Post.IMAGE_FILE, "some image file" );
                params.put( Post.CREATION_DATE, creationDate );
                params.put( Post.BROWSER_USED, "safari" );
                params.put( Post.LOCATION_IP, "31.55.91.341" );
                return params;
            }

            protected static Map<String, Object> aiya2()
            {
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set( 2013, Calendar.SEPTEMBER, 9, 0, 0, 0 );
                long creationDate = c.getTimeInMillis();

                Map<String, Object> params = new HashMap<String, Object>();
                params.put( Post.ID, 6L );
                params.put( Post.CONTENT, "[aiya2] bro" );
                params.put( Post.LANGUAGE, new String[] { "english" } );
                params.put( Post.IMAGE_FILE, "some image file" );
                params.put( Post.CREATION_DATE, creationDate );
                params.put( Post.BROWSER_USED, "safari" );
                params.put( Post.LOCATION_IP, "31.55.91.342" );
                return params;
            }

            protected static Map<String, Object> aiya3()
            {
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set( 2013, Calendar.SEPTEMBER, 5, 0, 0, 0 );
                long creationDate = c.getTimeInMillis();

                Map<String, Object> params = new HashMap<String, Object>();
                params.put( Post.ID, 7L );
                params.put( Post.CONTENT, "[aiya3] chur" );
                params.put( Post.LANGUAGE, new String[] { "english" } );
                params.put( Post.IMAGE_FILE, "some image file" );
                params.put( Post.CREATION_DATE, creationDate );
                params.put( Post.BROWSER_USED, "safari" );
                params.put( Post.LOCATION_IP, "31.55.91.343" );
                return params;
            }

            protected static Map<String, Object> stranger1()
            {
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set( 2013, Calendar.SEPTEMBER, 5, 4, 23, 45 );
                long creationDate = c.getTimeInMillis();

                Map<String, Object> params = new HashMap<String, Object>();
                params.put( Post.ID, 8L );
                params.put( Post.CONTENT, "[stranger1] gidday" );
                params.put( Post.LANGUAGE, new String[] { "english" } );
                params.put( Post.IMAGE_FILE, "some image file" );
                params.put( Post.CREATION_DATE, creationDate );
                params.put( Post.BROWSER_USED, "internet explorer" );
                params.put( Post.LOCATION_IP, "31.55.91.441" );
                return params;
            }

            protected static Map<String, Object> stranger2()
            {
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set( 2013, Calendar.SEPTEMBER, 5, 22, 34, 54 );
                long creationDate = c.getTimeInMillis();

                Map<String, Object> params = new HashMap<String, Object>();
                params.put( Post.ID, 9L );
                params.put( Post.CONTENT, "[stranger2] i heart sheep" );
                params.put( Post.LANGUAGE, new String[] { "english" } );
                params.put( Post.IMAGE_FILE, "some image file" );
                params.put( Post.CREATION_DATE, creationDate );
                params.put( Post.BROWSER_USED, "internet explorer" );
                params.put( Post.LOCATION_IP, "31.55.91.442" );
                return params;
            }

            protected static Map<String, Object> unknown1()
            {
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set( 2013, Calendar.SEPTEMBER, 6, 12, 5, 0 );
                long creationDate = c.getTimeInMillis();

                Map<String, Object> params = new HashMap<String, Object>();
                params.put( Post.ID, 10L );
                params.put( Post.CONTENT, "[unknown1] I wish I was known" );
                params.put( Post.LANGUAGE, new String[] { "swedish", "english" } );
                params.put( Post.IMAGE_FILE, "some image file that noone cares about" );
                params.put( Post.CREATION_DATE, creationDate );
                params.put( Post.BROWSER_USED, "chrome" );
                params.put( Post.LOCATION_IP, "3.62.11.1" );
                return params;
            }

            protected static Map<String, Object> unknown2()
            {
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set( 2013, Calendar.SEPTEMBER, 6, 13, 21, 3 );
                long creationDate = c.getTimeInMillis();

                Map<String, Object> params = new HashMap<String, Object>();
                params.put( Post.ID, 11L );
                params.put( Post.CONTENT, "[unknown2] please know me somebody" );
                params.put( Post.LANGUAGE, new String[] { "english" } );
                params.put( Post.IMAGE_FILE, "some image file again" );
                params.put( Post.CREATION_DATE, creationDate );
                params.put( Post.BROWSER_USED, "opera" );
                params.put( Post.LOCATION_IP, "39.75.21.42" );
                return params;
            }

            protected static Map<String, Object> nicky1()
            {
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set( 2013, Calendar.SEPTEMBER, 5, 20, 0, 1 );
                long creationDate = c.getTimeInMillis();

                Map<String, Object> params = new HashMap<String, Object>();
                params.put( Post.ID, 12L );
                params.put( Post.CONTENT, "[nicky1] i live in england" );
                params.put( Post.LANGUAGE, new String[] { "english" } );
                params.put( Post.IMAGE_FILE, "some image file" );
                params.put( Post.CREATION_DATE, creationDate );
                params.put( Post.BROWSER_USED, "safari" );
                params.put( Post.LOCATION_IP, "33.125.1.451" );
                return params;
            }
        }

        protected static class TestTags
        {
            protected static Map<String, Object> cake()
            {
                Map<String, Object> params = new HashMap<String, Object>();
                params.put( Tag.NAME, "cake" );
                params.put( Tag.URI, new String[] { "www.cake.good" } );
                return params;
            }

            protected static Map<String, Object> pie()
            {
                Map<String, Object> params = new HashMap<String, Object>();
                params.put( Tag.NAME, "pie" );
                params.put( Tag.URI, new String[] { "www.is.better" } );
                return params;
            }

            protected static Map<String, Object> lol()
            {
                Map<String, Object> params = new HashMap<String, Object>();
                params.put( Tag.NAME, "lol" );
                params.put( Tag.URI, new String[] { "www.lol.ol" } );
                return params;
            }

            protected static Map<String, Object> yolo()
            {
                Map<String, Object> params = new HashMap<String, Object>();
                params.put( Tag.NAME, "yolo" );
                params.put( Tag.URI, new String[] { "www.yolo.nu" } );
                return params;
            }

            protected static Map<String, Object> wtf()
            {
                Map<String, Object> params = new HashMap<String, Object>();
                params.put( Tag.NAME, "wtf" );
                params.put( Tag.URI, new String[] { "www.wtf.com" } );
                return params;
            }
        }

        protected static class TestForums
        {
            protected static Map<String, Object> cakesAndPies()
            {
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set( 2013, Calendar.OCTOBER, 2 );
                long creationDate = c.getTimeInMillis();

                Map<String, Object> params = new HashMap<String, Object>();
                params.put( Forum.TITLE, "everything cakes and pies" );
                params.put( Forum.CREATION_DATE, creationDate );
                return params;
            }

            protected static Map<String, Object> redditAddicts()
            {
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set( 2013, Calendar.OCTOBER, 22 );
                long creationDate = c.getTimeInMillis();

                Map<String, Object> params = new HashMap<String, Object>();
                params.put( Forum.TITLE, "if it's not on reddit it's not on nothing" );
                params.put( Forum.CREATION_DATE, creationDate );
                return params;
            }

            protected static Map<String, Object> floatingBoats()
            {
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set( 2013, Calendar.NOVEMBER, 13 );
                long creationDate = c.getTimeInMillis();

                Map<String, Object> params = new HashMap<String, Object>();
                params.put( Forum.TITLE, "boats are not submarines" );
                params.put( Forum.CREATION_DATE, creationDate );
                return params;
            }

            protected static Map<String, Object> kiwisSheepAndBungyJumping()
            {
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set( 2013, Calendar.NOVEMBER, 1 );
                long creationDate = c.getTimeInMillis();

                Map<String, Object> params = new HashMap<String, Object>();
                params.put( Forum.TITLE, "kiwis sheep and bungy jumping" );
                params.put( Forum.CREATION_DATE, creationDate );
                return params;
            }
        }

        protected static class TestComments
        {
            protected static Map<String, Object> aiya1()
            {
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set( 2013, Calendar.SEPTEMBER, 6 );
                long creationDate = c.getTimeInMillis();

                Map<String, Object> params = new HashMap<String, Object>();
                params.put( Post.CONTENT, "[aiya1] hi back" );
                params.put( Post.CREATION_DATE, creationDate );
                params.put( Post.BROWSER_USED, "safari" );
                params.put( Post.LOCATION_IP, "3.15.76.11" );
                return params;
            }

            protected static Map<String, Object> aiya2()
            {
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set( 2013, Calendar.SEPTEMBER, 6 );
                long creationDate = c.getTimeInMillis();

                Map<String, Object> params = new HashMap<String, Object>();
                params.put( Post.CONTENT, "[aiya2] so?" );
                params.put( Post.CREATION_DATE, creationDate );
                params.put( Post.BROWSER_USED, "safari" );
                params.put( Post.LOCATION_IP, "3.15.76.11" );
                return params;
            }

            protected static Map<String, Object> stranger1()
            {
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set( 2013, Calendar.SEPTEMBER, 7 );
                long creationDate = c.getTimeInMillis();

                Map<String, Object> params = new HashMap<String, Object>();
                params.put( Post.CONTENT, "[stranger1] i don't know you" );
                params.put( Post.CREATION_DATE, creationDate );
                params.put( Post.BROWSER_USED, "internet explorer" );
                params.put( Post.LOCATION_IP, "31.41.93.5" );
                return params;
            }

            protected static Map<String, Object> jake1()
            {
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set( 2013, Calendar.SEPTEMBER, 7 );
                long creationDate = c.getTimeInMillis();

                Map<String, Object> params = new HashMap<String, Object>();
                params.put( Post.CONTENT, "[jake1] pity you couldn't come" );
                params.put( Post.CREATION_DATE, creationDate );
                params.put( Post.BROWSER_USED, "safari" );
                params.put( Post.LOCATION_IP, "44.49.9.15" );
                return params;
            }

            protected static Map<String, Object> alex1()
            {
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set( 2013, Calendar.SEPTEMBER, 7 );
                long creationDate = c.getTimeInMillis();

                Map<String, Object> params = new HashMap<String, Object>();
                params.put( Post.CONTENT, "[alex1] chur bro" );
                params.put( Post.CREATION_DATE, creationDate );
                params.put( Post.BROWSER_USED, "safari" );
                params.put( Post.LOCATION_IP, "112.9.1.27" );
                return params;
            }
        }
    }
}
