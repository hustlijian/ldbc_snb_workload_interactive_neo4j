package com.ldbc.socialnet.neo4j.workload;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.Label;
import org.neo4j.helpers.collection.MapUtil;

import com.ldbc.driver.util.Tuple.Tuple2;
import com.ldbc.socialnet.neo4j.workload.TestGraph.Nodes.Cities;
import com.ldbc.socialnet.neo4j.workload.TestGraph.Nodes.Comments;
import com.ldbc.socialnet.neo4j.workload.TestGraph.Nodes.Companies;
import com.ldbc.socialnet.neo4j.workload.TestGraph.Nodes.Countries;
import com.ldbc.socialnet.neo4j.workload.TestGraph.Nodes.Forums;
import com.ldbc.socialnet.neo4j.workload.TestGraph.Nodes.Persons;
import com.ldbc.socialnet.neo4j.workload.TestGraph.Nodes.Posts;
import com.ldbc.socialnet.neo4j.workload.TestGraph.Nodes.Tags;
import com.ldbc.socialnet.neo4j.workload.TestGraph.Nodes.Universities;
import com.ldbc.socialnet.neo4j.workload.TestGraph.Rels.HasMember;
import com.ldbc.socialnet.neo4j.workload.TestGraph.Rels.StudyAt;
import com.ldbc.socialnet.neo4j.workload.TestGraph.Rels.WorksAt;
import com.ldbc.socialnet.workload.Domain;

/*
 * what graphlab is
 * what audience
 * what we presented
 * what the reaction was
 * contacts made
 */

public class TestGraph
{
    public static class Creator
    {
        /*
        September 4-8 (4 days), Sweden, New Zealand, Alex
        
        September 6-8 (2/3 days), Alex
        pie     3
        lol     2
        yolo    1
        wtf     1
        cake    1
        
        cakesAndPies - 2013, Calendar.OCTOBER, 2
            Alex - 2013, Calendar.OCTOBER, 2 
            Aiya - 2013, Calendar.OCTOBER, 3
            Stranger - 2013, Calendar.OCTOBER, 4
            Jake - 2013, Calendar.OCTOBER, 8 

        redditAddicts - 2013, Calendar.OCTOBER, 22
            Jake - 2013, Calendar.OCTOBER, 22

        floatingBoats - 2013, Calendar.NOVEMBER, 13
            Jake - 2013, Calendar.NOVEMBER, 13 
            Alex - 2013, Calendar.NOVEMBER, 14
            Peter -  2013, Calendar.NOVEMBER, 16 

        kiwisSheepAndBungyJumping - 2013, Calendar.NOVEMBER, 1
            Aiya - 2013, Calendar.NOVEMBER, 1
            Alex - 2013, Calendar.NOVEMBER, 4

        POSTS

        Friend
            Jake       Sweden
                5 September, New Zealand,  hello            [cake,yolo]     jakePost1           cakesAndPies                
                5 September, Sweden,       hej              [yolo]          jakePost2           cakesAndPies
                
                    Aiya        6 September     aiyaComment1        hi back
                    Stranger    7 September     strangerComment1    i don't know you
                        Aiya 7 September aiyaComment2 so?
                    
                7 September, Sweden,       tjena            [wtf,lol,pie]   jakePost3       4   floatingBoats
            Peter      Germany
                7 September, Germany,      hallo            [pie,lol]       peterPost1      4   floatingBoats
                
                    Jake        7 September     jakeComment1        pity you couldn't come
                    
            Aiya       New Zealand
                6 September, Sweden,       kia ora          [pie,cake,yolo] aiyaPost1       4   cakesAndPies
                9 September, New Zealand,  bro              [lol]           aiyaPost2           cakesAndPies
                5 September, New Zealand,  chur             [cake, pie]     aiyaPost3           kiwisSheepAndBungyJumping
                
                    Alex        6 September     alexComment1        chur bro
                    
        Friend Of Friend             
            Nicky England
                5 September, England,    I live in england    [lol,cake,wtf]           nickyPost1       cakesAndPies
                                
        Not Friend             
            Stranger   Sweden
                2 September, Australia,    gidday           [pie, cake]     strangerPost1       cakesAndPies
                5 September, Australia,    I heart sheep    [lol]           strangerPost2       cakesAndPies                
        */

        public static String createGraphQuery()
        {
            return "CREATE\n"

            + "\n// --- NODES ---\n\n"

            /*
             * Forums
             */

            + " (cakesAndPiesForum:"
                   + Domain.Node.Forum
                   + " {cakesAndPiesForum}),"

                   + " (redditAddictsForum:"
                   + Domain.Node.Forum
                   + " {redditAddictsForum}),\n"

                   + " (floatingBoatsForum:"
                   + Domain.Node.Forum
                   + " {floatingBoatsForum}),"

                   + " (kiwisSheepAndBungyJumpingForum:"
                   + Domain.Node.Forum
                   + " {kiwisSheepAndBungyJumpingForum}),\n"

                   /*
                    * Tags
                    */

                   + " (cake:"
                   + Domain.Node.Tag
                   + " {cake}), (pie:"
                   + Domain.Node.Tag
                   + " {pie}),\n"

                   + " (lol:"
                   + Domain.Node.Tag
                   + " {lol}), (yolo:"
                   + Domain.Node.Tag
                   + " {yolo}),\n"

                   + " (wtf:"
                   + Domain.Node.Tag
                   + " {wtf}),\n"

                   /*
                    * Persons
                    */

                   + " (alex:"
                   + Domain.Node.Person
                   + " {alex}), (aiya:"
                   + Domain.Node.Person
                   + " {aiya}),\n"

                   + " (jake:"
                   + Domain.Node.Person
                   + " {jake}), (peter:"
                   + Domain.Node.Person
                   + " {peter}),\n"

                   + " (stranger:"
                   + Domain.Node.Person
                   + " {stranger}), (nicky:"
                   + Domain.Node.Person
                   + " {nicky}),\n"

                   /*
                   * Cities
                   */

                   + " (auckland:"
                   + Domain.Node.Place
                   + ":"
                   + Domain.Place.Type.City
                   + " {auckland}),"

                   + " (stockholm:"
                   + Domain.Node.Place
                   + ":"
                   + Domain.Place.Type.City
                   + " {stockholm}),\n"

                   + " (munich:"
                   + Domain.Node.Place
                   + ":"
                   + Domain.Place.Type.City
                   + " {munich}),"

                   + " (melbourne:"
                   + Domain.Node.Place
                   + ":"
                   + Domain.Place.Type.City
                   + " {melbourne}),\n"

                   /*
                   * Countries
                   */

                   + " (se:"
                   + Domain.Node.Place
                   + ":"
                   + Domain.Place.Type.Country
                   + " {sweden}),"

                   + " (nz:"
                   + Domain.Node.Place
                   + ":"
                   + Domain.Place.Type.Country
                   + " {new_zealand}),\n"

                   + " (de:"
                   + Domain.Node.Place
                   + ":"
                   + Domain.Place.Type.Country
                   + " {germany}),"

                   + " (au:"
                   + Domain.Node.Place
                   + ":"
                   + Domain.Place.Type.Country
                   + " {australia}),\n"

                   + " (uk:"
                   + Domain.Node.Place
                   + ":"
                   + Domain.Place.Type.Country
                   + " {england}),\n"

                   /*
                   * Universities
                   */

                   + " (aut:"
                   + Domain.Node.Organisation
                   + ":"
                   + Domain.Organisation.Type.University
                   + " {aut}),"

                   + " (kth:"
                   + Domain.Node.Organisation
                   + ":"
                   + Domain.Organisation.Type.University
                   + " {kth}),\n"

                   /*
                   * Companies
                   */

                   + " (sics:"
                   + Domain.Node.Organisation
                   + ":"
                   + Domain.Organisation.Type.Company
                   + " {sics}),"

                   + " (neo:"
                   + Domain.Node.Organisation
                   + ":"
                   + Domain.Organisation.Type.Company
                   + " {neo}),"

                   + " (hot:"
                   + Domain.Node.Organisation
                   + ":"
                   + Domain.Organisation.Type.Company
                   + " {hot}),\n"

                   /*
                   * Posts
                   */

                   + " (jakePost1:"
                   + Domain.Node.Post
                   + " {jakePost1}), (jakePost2:"
                   + Domain.Node.Post
                   + " {jakePost2}),"

                   + " (jakePost3:"
                   + Domain.Node.Post
                   + " {jakePost3}),\n"

                   + " (peterPost1:"
                   + Domain.Node.Post
                   + " {peterPost1}), (aiyaPost1:"
                   + Domain.Node.Post
                   + " {aiyaPost1}),"

                   + " (aiyaPost2:"
                   + Domain.Node.Post
                   + " {aiyaPost2}), (aiyaPost3:"
                   + Domain.Node.Post
                   + " {aiyaPost3}),\n"

                   + " (strangerPost1:"
                   + Domain.Node.Post
                   + " {strangerPost1}),"

                   + " (strangerPost2:"
                   + Domain.Node.Post
                   + " {strangerPost2}),\n"

                   + " (nickyPost1:"
                   + Domain.Node.Post
                   + " {nickyPost1}),\n"

                   /*
                   * Comments
                   */
                   + " (aiyaComment1:"
                   + Domain.Node.Comment
                   + " {aiyaComment1}),"

                   + " (aiyaComment2:"
                   + Domain.Node.Comment
                   + " {aiyaComment2}),\n"

                   + " (strangerComment1:"
                   + Domain.Node.Comment
                   + " {strangerComment1}),"

                   + " (jakeComment1:"
                   + Domain.Node.Comment
                   + " {jakeComment1}),"

                   + " (alexComment1:"
                   + Domain.Node.Comment
                   + " {alexComment1}),\n"

                   + "\n// --- RELATIONSHIPS ---\n\n"

                   /*
                    * City-Country
                    */

                   + " (stockholm)-[:"
                   + Domain.Rel.IS_LOCATED_IN
                   + "]->(se),"

                   + " (auckland)-[:"
                   + Domain.Rel.IS_LOCATED_IN
                   + "]->(nz),\n"

                   + " (munich)-[:"
                   + Domain.Rel.IS_LOCATED_IN
                   + "]->(de),"

                   + " (melbourne)-[:"
                   + Domain.Rel.IS_LOCATED_IN
                   + "]->(au),\n"

                   /*
                    * Organisation-Country
                    */

                   + " (neo)<-[:"
                   + Domain.Rel.WORKS_AT
                   + " {alexWorkAtNeo}]-(alex),"

                   + " (stockholm)<-[:"
                   + Domain.Rel.IS_LOCATED_IN
                   + "]-(kth),\n"

                   + " (auckland)<-[:"
                   + Domain.Rel.IS_LOCATED_IN
                   + "]-(aut),"

                   + " (se)<-[:"
                   + Domain.Rel.IS_LOCATED_IN
                   + "]-(sics),\n"

                   + " (nz)<-[:"
                   + Domain.Rel.IS_LOCATED_IN
                   + "]-(hot),\n"

                   /*
                    * Nicky
                    */

                   + " (nicky)-[:"
                   + Domain.Rel.IS_LOCATED_IN
                   + "]->(uk),\n"

                   /*
                    * Alex
                    */

                   + " (alex)-[:"
                   + Domain.Rel.IS_LOCATED_IN
                   + "]->(stockholm),\n"

                   + " (se)<-[:"
                   + Domain.Rel.IS_LOCATED_IN
                   + "]-(neo),\n"

                   + " (kth)<-[:"
                   + Domain.Rel.STUDY_AT
                   + " {alexStudyAtKth}]-(alex),\n"

                   + " (aut)<-[:"
                   + Domain.Rel.STUDY_AT
                   + " {alexStudyAtAut}]-(alex),\n"

                   + " (sics)<-[:"
                   + Domain.Rel.WORKS_AT
                   + " {alexWorkAtSics}]-(alex),\n"

                   /*
                    * Aiya
                    */

                   + " (aiya)-[:"
                   + Domain.Rel.IS_LOCATED_IN
                   + "]->(auckland),\n"

                   + " (hot)<-[:"
                   + Domain.Rel.WORKS_AT
                   + " {aiyaWorkAtHot}]-(aiya),\n"

                   /*
                    * Jake
                    */

                   + " (jake)-[:"
                   + Domain.Rel.IS_LOCATED_IN
                   + "]->(stockholm),\n"

                   /*
                   * Peter
                   */

                   + " (peter)-[:"
                   + Domain.Rel.IS_LOCATED_IN
                   + "]->(munich),\n"

                   /*
                    * Stranger
                    */

                   + " (stranger)-[:"
                   + Domain.Rel.IS_LOCATED_IN
                   + "]->(melbourne),\n"

                   /*
                    * Forum-Person (moderator)
                    */

                   + " (cakesAndPiesForum)-[:"
                   + Domain.Rel.HAS_MODERATOR
                   + "]->(alex),"

                   + " (redditAddictsForum)-[:"
                   + Domain.Rel.HAS_MODERATOR
                   + "]->(jake),\n"

                   + " (floatingBoatsForum)-[:"
                   + Domain.Rel.HAS_MODERATOR
                   + "]->(jake),"

                   + " (kiwisSheepAndBungyJumpingForum)-[:"
                   + Domain.Rel.HAS_MODERATOR
                   + "]->(aiya),\n"

                   /*
                    * Forum-Person (member)
                    */

                   + " (cakesAndPiesForum)-[:"
                   + Domain.Rel.HAS_MEMBER
                   + " {cakesAndPiesHasMemberAlex}]->(alex),"

                   + " (cakesAndPiesForum)-[:"
                   + Domain.Rel.HAS_MEMBER
                   + " {cakesAndPiesHasMemberAiya}]->(aiya),\n"

                   + " (cakesAndPiesForum)-[:"
                   + Domain.Rel.HAS_MEMBER
                   + " {cakesAndPiesHasMemberStranger}]->(stranger),"

                   + " (cakesAndPiesForum)-[:"
                   + Domain.Rel.HAS_MEMBER
                   + " {cakesAndPiesHasMemberJake}]->(jake),\n"

                   + " (cakesAndPiesForum)-[:"
                   + Domain.Rel.HAS_MEMBER
                   + " {cakesAndPiesHasMemberNicky}]->(nicky),\n"

                   + " (redditAddictsForum)-[:"
                   + Domain.Rel.HAS_MEMBER
                   + " {redditAddictsHasMemberJake}]->(jake),"

                   + " (floatingBoatsForum)-[:"
                   + Domain.Rel.HAS_MEMBER
                   + " {floatingBoatsHasMemberAlex}]->(alex),\n"

                   + " (floatingBoatsForum)-[:"
                   + Domain.Rel.HAS_MEMBER
                   + " {floatingBoatsHasMemberJake}]->(jake),"

                   + " (floatingBoatsForum)-[:"
                   + Domain.Rel.HAS_MEMBER
                   + " {floatingBoatsHasMemberPeter}]->(peter),\n"

                   + " (kiwisSheepAndBungyJumpingForum)-[:"
                   + Domain.Rel.HAS_MEMBER
                   + " {kiwisSheepAndBungyJumpingHasMemberAiya}]->(aiya),\n"

                   + " (kiwisSheepAndBungyJumpingForum)-[:"
                   + Domain.Rel.HAS_MEMBER
                   + " {kiwisSheepAndBungyJumpingHasMemberAlex}]->(alex)\n"

                   /*
                   * Person-Person
                   */

                   + "FOREACH (n IN [jake, aiya, peter] | CREATE (alex)-[:"
                   + Domain.Rel.KNOWS
                   + "]->(n) )\n"

                   + "FOREACH (n IN [nicky] | CREATE (aiya)-[:"
                   + Domain.Rel.KNOWS
                   + "]->(n) )\n"

                   /*
                   * Post-Country
                   */

                   + "FOREACH (n IN [jakePost1,aiyaPost2, aiyaPost3] | CREATE (n)-[:"
                   + Domain.Rel.IS_LOCATED_IN
                   + "]->(nz) )\n"

                   + "FOREACH (n IN [jakePost2, jakePost3, aiyaPost1] | CREATE (n)-[:"
                   + Domain.Rel.IS_LOCATED_IN
                   + "]->(se) )\n"

                   + "FOREACH (n IN [peterPost1] | CREATE (n)-[:"
                   + Domain.Rel.IS_LOCATED_IN
                   + "]->(de) )\n"

                   + "FOREACH (n IN [strangerPost1, strangerPost2] | CREATE (n)-[:"
                   + Domain.Rel.IS_LOCATED_IN
                   + "]->(au) )\n"

                   + "FOREACH (n IN [nickyPost1] | CREATE (n)-[:"
                   + Domain.Rel.IS_LOCATED_IN
                   + "]->(uk) )\n"

                   /*
                   * Post-Person
                   */

                   + "FOREACH (n IN [jakePost1, jakePost2, jakePost3] | CREATE (n)-[:"
                   + Domain.Rel.HAS_CREATOR
                   + "]->(jake) )\n"

                   + "FOREACH (n IN [aiyaPost1, aiyaPost2, aiyaPost3] | CREATE (n)-[:"
                   + Domain.Rel.HAS_CREATOR
                   + "]->(aiya) )\n"

                   + "FOREACH (n IN [peterPost1] | CREATE (n)-[:"
                   + Domain.Rel.HAS_CREATOR
                   + "]->(peter) )\n"

                   + "FOREACH (n IN [strangerPost1, strangerPost2] | CREATE (n)-[:"
                   + Domain.Rel.HAS_CREATOR
                   + "]->(stranger) )\n"

                   + "FOREACH (n IN [nickyPost1] | CREATE (n)-[:"
                   + Domain.Rel.HAS_CREATOR
                   + "]->(nicky) )\n"

                   /*
                   * Post-Tag
                   */

                   + "FOREACH (n IN [cake, yolo] | CREATE (jakePost1)-[:"
                   + Domain.Rel.HAS_TAG
                   + "]->(n) )\n"

                   + "FOREACH (n IN [yolo] | CREATE (jakePost2)-[:"
                   + Domain.Rel.HAS_TAG
                   + "]->(n) )\n"

                   + "FOREACH (n IN [wtf, lol, pie] | CREATE (jakePost3)-[:"
                   + Domain.Rel.HAS_TAG
                   + "]->(n) )\n"

                   + "FOREACH (n IN [pie, lol] | CREATE (peterPost1)-[:"
                   + Domain.Rel.HAS_TAG
                   + "]->(n) )\n"

                   + "FOREACH (n IN [pie, cake, yolo] | CREATE (aiyaPost1)-[:"
                   + Domain.Rel.HAS_TAG
                   + "]->(n) )\n"

                   + "FOREACH (n IN [lol] | CREATE (aiyaPost2)-[:"
                   + Domain.Rel.HAS_TAG
                   + "]->(n) )\n"

                   + "FOREACH (n IN [cake, pie] | CREATE (aiyaPost3)-[:"
                   + Domain.Rel.HAS_TAG
                   + "]->(n) )\n"

                   + "FOREACH (n IN [pie, cake] | CREATE (strangerPost1)-[:"
                   + Domain.Rel.HAS_TAG
                   + "]->(n) )\n"

                   + "FOREACH (n IN [lol] | CREATE (strangerPost2)-[:"
                   + Domain.Rel.HAS_TAG
                   + "]->(n) )\n"

                   + "FOREACH (n IN [lol,cake,wtf] | CREATE (nickyPost1)-[:"
                   + Domain.Rel.HAS_TAG
                   + "]->(n) )\n"

                   /*
                    * Post-Forum
                    */

                   + "FOREACH (n IN [jakePost1, jakePost2, aiyaPost1, aiyaPost2, strangerPost1, strangerPost2, nickyPost1]|"
                   + " CREATE (cakesAndPiesForum)-[:" + Domain.Rel.CONTAINER_OF + "]->(n) )\n"

                   + "FOREACH (n IN [jakePost3, peterPost1] | CREATE (floatingBoatsForum)-[:" + Domain.Rel.CONTAINER_OF
                   + "]->(n) )\n"

                   + "FOREACH (n IN [aiyaPost3] | CREATE (kiwisSheepAndBungyJumpingForum)-[:" + Domain.Rel.CONTAINER_OF
                   + "]->(n) )\n"

                   /*
                    * Comment-Person
                    */

                   + "FOREACH (n IN [aiyaComment1, aiyaComment2] | CREATE (n)-[:" + Domain.Rel.HAS_CREATOR
                   + "]->(aiya) )\n"

                   + "FOREACH (n IN [alexComment1] | CREATE (n)-[:" + Domain.Rel.HAS_CREATOR + "]->(alex) )\n"

                   + "FOREACH (n IN [jakeComment1] | CREATE (n)-[:" + Domain.Rel.HAS_CREATOR + "]->(jake) )\n"

                   + "FOREACH (n IN [strangerComment1] | CREATE (n)-[:" + Domain.Rel.HAS_CREATOR + "]->(stranger) )\n"

                   /*
                    * Comment-Post
                    */

                   + "FOREACH (n IN [aiyaComment1, strangerComment1] | CREATE (n)-[:" + Domain.Rel.REPLY_OF
                   + "]->(jakePost2) )\n"

                   + "FOREACH (n IN [aiyaComment2] | CREATE (n)-[:" + Domain.Rel.REPLY_OF + "]->(strangerComment1) )\n"

                   + "FOREACH (n IN [jakeComment1] | CREATE (n)-[:" + Domain.Rel.REPLY_OF + "]->(peterPost1) )\n"

                   + "FOREACH (n IN [alexComment1] | CREATE (n)-[:" + Domain.Rel.REPLY_OF + "]->(aiyaPost2) )\n";
        }

        public static Map<String, Object> createGraphQueryParams()
        {
            return MapUtil.map( "cakesAndPiesForum", Forums.cakesAndPies(), "redditAddictsForum",
                    Forums.redditAddicts(), "floatingBoatsForum", Forums.floatingBoats(),
                    "kiwisSheepAndBungyJumpingForum", Forums.kiwisSheepAndBungyJumping(), "cake", Tags.cake(), "pie",
                    Tags.pie(), "lol", Tags.lol(), "yolo", Tags.yolo(), "wtf", Tags.wtf(), "alex", Persons.alex(),
                    "aiya", Persons.aiya(), "jake", Persons.jake(), "peter", Persons.peter(), "stranger",
                    Persons.stranger(), "nicky", Persons.nicky(), "auckland", Cities.auckland(), "stockholm",
                    Cities.stockholm(), "munich", Cities.munich(), "melbourne", Cities.melbourne(), "sweden",
                    Countries.sweden(), "new_zealand", Countries.newZealand(), "germany", Countries.germany(),
                    "australia", Countries.australia(), "england", Countries.england(), "aut", Universities.aut(),
                    "kth", Universities.kth(), "sics", Companies.sics(), "neo", Companies.neo(), "hot",
                    Companies.hot(), "alexWorkAtSics", WorksAt.alexWorkAtSics(), "alexWorkAtNeo",
                    WorksAt.alexWorkAtNeo(), "aiyaWorkAtHot", WorksAt.aiyaWorkAtHot(), "alexStudyAtAut",
                    StudyAt.alexStudyAtAut(), "alexStudyAtKth", StudyAt.alexStudyAtKth(), "jakePost1", Posts.jake1(),
                    "jakePost2", Posts.jake2(), "jakePost3", Posts.jake3(), "peterPost1", Posts.peter1(), "aiyaPost1",
                    Posts.aiya1(), "aiyaPost2", Posts.aiya2(), "aiyaPost3", Posts.aiya3(), "strangerPost1",
                    Posts.stranger1(), "strangerPost2", Posts.stranger2(), "nickyPost1", Posts.nicky1(),
                    "cakesAndPiesHasMemberAlex", HasMember.cakesAndPiesHasMemberAlex(), "cakesAndPiesHasMemberAiya",
                    HasMember.cakesAndPiesHasMemberAiya(), "cakesAndPiesHasMemberStranger",
                    HasMember.cakesAndPiesHasMemberStranger(), "cakesAndPiesHasMemberJake",
                    HasMember.cakesAndPiesHasMemberJake(), "cakesAndPiesHasMemberNicky",
                    HasMember.cakesAndPiesHasMemberNicky(), "redditAddictsHasMemberJake",
                    HasMember.redditAddictsHasMemberJake(), "floatingBoatsHasMemberJake",
                    HasMember.floatingBoatsHasMemberJake(), "floatingBoatsHasMemberAlex",
                    HasMember.floatingBoatsHasMemberAlex(), "floatingBoatsHasMemberPeter",
                    HasMember.floatingBoatsHasMemberPeter(), "kiwisSheepAndBungyJumpingHasMemberAiya",
                    HasMember.kiwisSheepAndBungyJumpingHasMemberAiya(), "kiwisSheepAndBungyJumpingHasMemberAlex",
                    HasMember.kiwisSheepAndBungyJumpingHasMemberAlex(), "aiyaComment1", Comments.aiya1(),
                    "aiyaComment2", Comments.aiya2(), "strangerComment1", Comments.stranger1(), "jakeComment1",
                    Comments.jake1(), "alexComment1", Comments.alex1() );
        }

        public static Iterable<String> createIndexQueries()
        {
            List<String> createIndexQueries = new ArrayList<String>();
            for ( Tuple2<Label, String> labelAndProperty : Domain.labelPropertyPairsToIndex() )
            {
                createIndexQueries.add( "CREATE INDEX ON :" + labelAndProperty._1() + "(" + labelAndProperty._2() + ")" );
            }
            return createIndexQueries;
        }
    }

    protected static class Rels
    {
        protected static class HasMember
        {

            // cakesAndPies - 2013, Calendar.OCTOBER, 2
            protected static Map<String, Object> cakesAndPiesHasMemberAlex()
            {
                Calendar c = Calendar.getInstance();
                c.set( 2013, Calendar.OCTOBER, 2 );
                long joinDate = c.getTimeInMillis();
                return MapUtil.map( Domain.HasMember.JOIN_DATE, joinDate );
            }

            protected static Map<String, Object> cakesAndPiesHasMemberAiya()
            {
                Calendar c = Calendar.getInstance();
                c.set( 2013, Calendar.OCTOBER, 3 );
                long joinDate = c.getTimeInMillis();
                return MapUtil.map( Domain.HasMember.JOIN_DATE, joinDate );
            }

            protected static Map<String, Object> cakesAndPiesHasMemberStranger()
            {
                Calendar c = Calendar.getInstance();
                c.set( 2013, Calendar.OCTOBER, 4 );
                long joinDate = c.getTimeInMillis();
                return MapUtil.map( Domain.HasMember.JOIN_DATE, joinDate );
            }

            protected static Map<String, Object> cakesAndPiesHasMemberJake()
            {
                Calendar c = Calendar.getInstance();
                c.set( 2013, Calendar.OCTOBER, 8 );
                long joinDate = c.getTimeInMillis();
                return MapUtil.map( Domain.HasMember.JOIN_DATE, joinDate );
            }

            protected static Map<String, Object> cakesAndPiesHasMemberNicky()
            {
                Calendar c = Calendar.getInstance();
                c.set( 2013, Calendar.OCTOBER, 9 );
                long joinDate = c.getTimeInMillis();
                return MapUtil.map( Domain.HasMember.JOIN_DATE, joinDate );
            }

            // redditAddicts - 2013, Calendar.OCTOBER, 22
            protected static Map<String, Object> redditAddictsHasMemberJake()
            {
                Calendar c = Calendar.getInstance();
                c.set( 2013, Calendar.OCTOBER, 22 );
                long joinDate = c.getTimeInMillis();
                return MapUtil.map( Domain.HasMember.JOIN_DATE, joinDate );
            }

            // floatingBoats - 2013, Calendar.NOVEMBER, 13
            protected static Map<String, Object> floatingBoatsHasMemberJake()
            {
                Calendar c = Calendar.getInstance();
                c.set( 2013, Calendar.NOVEMBER, 13 );
                long joinDate = c.getTimeInMillis();
                return MapUtil.map( Domain.HasMember.JOIN_DATE, joinDate );
            }

            protected static Map<String, Object> floatingBoatsHasMemberAlex()
            {
                Calendar c = Calendar.getInstance();
                c.set( 2013, Calendar.NOVEMBER, 14 );
                long joinDate = c.getTimeInMillis();
                return MapUtil.map( Domain.HasMember.JOIN_DATE, joinDate );
            }

            protected static Map<String, Object> floatingBoatsHasMemberPeter()
            {
                Calendar c = Calendar.getInstance();
                c.set( 2013, Calendar.NOVEMBER, 16 );
                long joinDate = c.getTimeInMillis();
                return MapUtil.map( Domain.HasMember.JOIN_DATE, joinDate );
            }

            // kiwisSheepAndBungyJumping - 2013, Calendar.NOVEMBER, 1
            protected static Map<String, Object> kiwisSheepAndBungyJumpingHasMemberAiya()
            {
                Calendar c = Calendar.getInstance();
                c.set( 2013, Calendar.NOVEMBER, 1 );
                long joinDate = c.getTimeInMillis();
                return MapUtil.map( Domain.HasMember.JOIN_DATE, joinDate );
            }

            protected static Map<String, Object> kiwisSheepAndBungyJumpingHasMemberAlex()
            {
                Calendar c = Calendar.getInstance();
                c.set( 2013, Calendar.NOVEMBER, 4 );
                long joinDate = c.getTimeInMillis();
                return MapUtil.map( Domain.HasMember.JOIN_DATE, joinDate );
            }
        }

        protected static class WorksAt
        {
            protected static Map<String, Object> alexWorkAtSics()
            {
                return MapUtil.map( Domain.WorksAt.WORK_FROM, 2010 );
            }

            protected static Map<String, Object> alexWorkAtNeo()
            {
                return MapUtil.map( Domain.WorksAt.WORK_FROM, 2012 );
            }

            protected static Map<String, Object> aiyaWorkAtHot()
            {
                return MapUtil.map( Domain.WorksAt.WORK_FROM, 2005 );
            }
        }

        protected static class StudyAt
        {
            protected static Map<String, Object> alexStudyAtAut()
            {
                return MapUtil.map( Domain.StudiesAt.CLASS_YEAR, 2006 );
            }

            protected static Map<String, Object> alexStudyAtKth()
            {
                return MapUtil.map( Domain.StudiesAt.CLASS_YEAR, 2008 );
            }
        }
    }

    protected static class Nodes
    {
        protected static class Companies
        {
            protected static Map<String, Object> sics()
            {
                return MapUtil.map( Domain.Organisation.NAME, "swedish institute of computer science" );
            }

            protected static Map<String, Object> neo()
            {
                return MapUtil.map( Domain.Place.NAME, "neo technology" );
            }

            protected static Map<String, Object> hot()
            {
                return MapUtil.map( Domain.Place.NAME, "house of travel" );
            }
        }

        protected static class Universities
        {
            protected static Map<String, Object> aut()
            {
                return MapUtil.map( Domain.Organisation.NAME, "auckland university of technology" );
            }

            protected static Map<String, Object> kth()
            {
                return MapUtil.map( Domain.Place.NAME, "royal institute of technology" );
            }
        }

        protected static class Countries
        {
            protected static Map<String, Object> sweden()
            {
                return MapUtil.map( Domain.Place.NAME, "sweden" );
            }

            protected static Map<String, Object> newZealand()
            {
                return MapUtil.map( Domain.Place.NAME, "new zealand" );
            }

            protected static Map<String, Object> germany()
            {
                return MapUtil.map( Domain.Place.NAME, "germany" );
            }

            protected static Map<String, Object> australia()
            {
                return MapUtil.map( Domain.Place.NAME, "australia" );
            }

            protected static Map<String, Object> england()
            {
                return MapUtil.map( Domain.Place.NAME, "england" );
            }
        }

        protected static class Cities
        {

            protected static Map<String, Object> munich()
            {
                return MapUtil.map( Domain.Place.NAME, "munich" );
            }

            protected static Map<String, Object> melbourne()
            {
                return MapUtil.map( Domain.Place.NAME, "melbourne" );
            }

            protected static Map<String, Object> malmo()
            {
                return MapUtil.map( Domain.Place.NAME, "malmö" );
            }

            protected static Map<String, Object> stockholm()
            {
                return MapUtil.map( Domain.Place.NAME, "stockholm" );
            }

            protected static Map<String, Object> auckland()
            {
                return MapUtil.map( Domain.Place.NAME, "auckland" );
            }

            protected static Map<String, Object> whangarei()
            {
                return MapUtil.map( Domain.Place.NAME, "whangarei" );
            }
        }

        protected static class Persons
        {
            protected static Map<String, Object> alex()
            {
                Map<String, Object> params = new HashMap<String, Object>();
                params.put( Domain.Person.ID, 1L );
                params.put( Domain.Person.FIRST_NAME, "alex" );
                params.put( Domain.Person.LAST_NAME, "averbuch" );
                Calendar c = Calendar.getInstance();
                c.set( 2012, Calendar.JUNE, 6 );
                long creationDate = c.getTimeInMillis();
                params.put( Domain.Person.CREATION_DATE, creationDate );
                c.set( 1982, Calendar.JANUARY, 23 );
                long birthday = c.getTimeInMillis();
                params.put( Domain.Person.BIRTHDAY, birthday );
                params.put( Domain.Person.BROWSER_USED, "chrome" );
                params.put( Domain.Person.EMAIL_ADDRESSES, new String[] { "alex.averbuch@gmail.com",
                        "alex.averbuch@neotechnology.com" } );
                params.put( Domain.Person.GENDER, "male" );
                params.put( Domain.Person.LANGUAGES, new String[] { "english", "swedish" } );
                params.put( Domain.Person.LOCATION_IP, "192.168.42.24" );
                return params;
            }

            protected static Map<String, Object> aiya()
            {
                Map<String, Object> params = new HashMap<String, Object>();
                params.put( Domain.Person.ID, 2L );
                params.put( Domain.Person.FIRST_NAME, "aiya" );
                params.put( Domain.Person.LAST_NAME, "thorpe" );
                Calendar c = Calendar.getInstance();
                c.set( 2013, Calendar.MAY, 19 );
                long creationDate = c.getTimeInMillis();
                params.put( Domain.Person.CREATION_DATE, creationDate );
                c.set( 1983, Calendar.SEPTEMBER, 8 );
                long birthday = c.getTimeInMillis();
                params.put( Domain.Person.BIRTHDAY, birthday );
                params.put( Domain.Person.BROWSER_USED, "safari" );
                params.put( Domain.Person.EMAIL_ADDRESSES, new String[] { "aiya.thorpe@gmail.com" } );
                params.put( Domain.Person.GENDER, "female" );
                params.put( Domain.Person.LANGUAGES, new String[] { "english" } );
                params.put( Domain.Person.LOCATION_IP, "192.161.48.1" );
                return params;
            }

            protected static Map<String, Object> jake()
            {
                Map<String, Object> params = new HashMap<String, Object>();
                params.put( Domain.Person.ID, 3L );
                params.put( Domain.Person.FIRST_NAME, "jacob" );
                params.put( Domain.Person.LAST_NAME, "hansson" );
                Calendar c = Calendar.getInstance();
                c.set( 2013, Calendar.SEPTEMBER, 10 );
                long creationDate = c.getTimeInMillis();
                params.put( Domain.Person.CREATION_DATE, creationDate );
                c.set( 1987, Calendar.JULY, 21 );
                long birthday = c.getTimeInMillis();
                params.put( Domain.Person.BIRTHDAY, birthday );
                params.put( Domain.Person.BROWSER_USED, "safari" );
                params.put( Domain.Person.EMAIL_ADDRESSES, new String[] { "jakewins@gmail.com",
                        "jake@neotechnology.com" } );
                params.put( Domain.Person.GENDER, "male" );
                params.put( Domain.Person.LANGUAGES, new String[] { "english", "swedish" } );
                params.put( Domain.Person.LOCATION_IP, "172.124.98.31" );
                return params;
            }

            protected static Map<String, Object> peter()
            {
                Map<String, Object> params = new HashMap<String, Object>();
                params.put( Domain.Person.ID, 4L );
                params.put( Domain.Person.FIRST_NAME, "peter" );
                params.put( Domain.Person.LAST_NAME, "rentschler" );
                Calendar c = Calendar.getInstance();
                c.set( 2013, Calendar.JANUARY, 5 );
                long creationDate = c.getTimeInMillis();
                params.put( Domain.Person.CREATION_DATE, creationDate );
                c.set( 1982, Calendar.JUNE, 5 );
                long birthday = c.getTimeInMillis();
                params.put( Domain.Person.BIRTHDAY, birthday );
                params.put( Domain.Person.BROWSER_USED, "firefox" );
                params.put( Domain.Person.EMAIL_ADDRESSES, new String[] { "peter.rentschler@gmx.de" } );
                params.put( Domain.Person.GENDER, "male" );
                params.put( Domain.Person.LANGUAGES, new String[] { "english", "german" } );
                params.put( Domain.Person.LOCATION_IP, "12.24.158.11" );
                return params;
            }

            protected static Map<String, Object> stranger()
            {
                Map<String, Object> params = new HashMap<String, Object>();
                params.put( Domain.Person.ID, 5L );
                params.put( Domain.Person.FIRST_NAME, "stranger" );
                params.put( Domain.Person.LAST_NAME, "dude" );
                Calendar c = Calendar.getInstance();
                c.set( 2012, Calendar.OCTOBER, 15 );
                long creationDate = c.getTimeInMillis();
                params.put( Domain.Person.CREATION_DATE, creationDate );
                c.set( 1985, Calendar.FEBRUARY, 11 );
                long birthday = c.getTimeInMillis();
                params.put( Domain.Person.BIRTHDAY, birthday );
                params.put( Domain.Person.BROWSER_USED, "internet explorer" );
                params.put( Domain.Person.EMAIL_ADDRESSES, new String[] { "dr.strange@love.come" } );
                params.put( Domain.Person.GENDER, "male" );
                params.put( Domain.Person.LANGUAGES, new String[] { "english" } );
                params.put( Domain.Person.LOCATION_IP, "12.24.158.11" );
                return params;
            }

            protected static Map<String, Object> nicky()
            {
                Map<String, Object> params = new HashMap<String, Object>();
                params.put( Domain.Person.ID, 6L );
                params.put( Domain.Person.FIRST_NAME, "nicky" );
                params.put( Domain.Person.LAST_NAME, "toothill" );
                Calendar c = Calendar.getInstance();
                c.set( 2013, Calendar.JUNE, 8 );
                long creationDate = c.getTimeInMillis();
                params.put( Domain.Person.CREATION_DATE, creationDate );
                c.set( 1982, Calendar.AUGUST, 11 );
                long birthday = c.getTimeInMillis();
                params.put( Domain.Person.BIRTHDAY, birthday );
                params.put( Domain.Person.BROWSER_USED, "safari" );
                params.put( Domain.Person.EMAIL_ADDRESSES, new String[] { "nicky@provider.com" } );
                params.put( Domain.Person.GENDER, "female" );
                params.put( Domain.Person.LANGUAGES, new String[] { "english", "spanish" } );
                params.put( Domain.Person.LOCATION_IP, "12.171.48.1" );
                return params;
            }

        }

        protected static class Posts
        {
            protected static Map<String, Object> jake1()
            {
                Calendar c = Calendar.getInstance();
                c.set( 2013, Calendar.SEPTEMBER, 5 );
                long creationDate = c.getTimeInMillis();

                Map<String, Object> params = new HashMap<String, Object>();
                params.put( Domain.Post.CONTENT, "[jake1] hello" );
                params.put( Domain.Post.LANGUAGE, new String[] { "english" } );
                params.put( Domain.Post.IMAGE_FILE, "some image file" );
                params.put( Domain.Post.CREATION_DATE, creationDate );
                params.put( Domain.Post.BROWSER_USED, "safari" );
                params.put( Domain.Post.LOCATION_IP, "31.55.91.141" );
                return params;
            }

            protected static Map<String, Object> jake2()
            {
                Calendar c = Calendar.getInstance();
                c.set( 2013, Calendar.SEPTEMBER, 5 );
                long creationDate = c.getTimeInMillis();

                Map<String, Object> params = new HashMap<String, Object>();
                params.put( Domain.Post.CONTENT, "[jake2] hej" );
                params.put( Domain.Post.LANGUAGE, new String[] { "swedish" } );
                params.put( Domain.Post.IMAGE_FILE, "some image file" );
                params.put( Domain.Post.CREATION_DATE, creationDate );
                params.put( Domain.Post.BROWSER_USED, "safari" );
                params.put( Domain.Post.LOCATION_IP, "31.55.91.142" );
                return params;
            }

            protected static Map<String, Object> jake3()
            {
                Calendar c = Calendar.getInstance();
                c.set( 2013, Calendar.SEPTEMBER, 7 );
                long creationDate = c.getTimeInMillis();

                Map<String, Object> params = new HashMap<String, Object>();
                params.put( Domain.Post.CONTENT, "[jake3] tjena" );
                params.put( Domain.Post.LANGUAGE, new String[] { "swedish" } );
                params.put( Domain.Post.IMAGE_FILE, "some image file" );
                params.put( Domain.Post.CREATION_DATE, creationDate );
                params.put( Domain.Post.BROWSER_USED, "safari" );
                params.put( Domain.Post.LOCATION_IP, "31.55.91.143" );
                return params;
            }

            protected static Map<String, Object> peter1()
            {
                Calendar c = Calendar.getInstance();
                c.set( 2013, Calendar.SEPTEMBER, 7 );
                long creationDate = c.getTimeInMillis();

                Map<String, Object> params = new HashMap<String, Object>();
                params.put( Domain.Post.CONTENT, "[peter1] hallo" );
                params.put( Domain.Post.LANGUAGE, new String[] { "german" } );
                params.put( Domain.Post.IMAGE_FILE, "some image file" );
                params.put( Domain.Post.CREATION_DATE, creationDate );
                params.put( Domain.Post.BROWSER_USED, "firefox" );
                params.put( Domain.Post.LOCATION_IP, "31.55.91.241" );
                return params;
            }

            protected static Map<String, Object> aiya1()
            {
                Calendar c = Calendar.getInstance();
                c.set( 2013, Calendar.SEPTEMBER, 6 );
                long creationDate = c.getTimeInMillis();

                Map<String, Object> params = new HashMap<String, Object>();
                params.put( Domain.Post.CONTENT, "[aiya1] kia ora" );
                params.put( Domain.Post.LANGUAGE, new String[] { "english" } );
                params.put( Domain.Post.IMAGE_FILE, "some image file" );
                params.put( Domain.Post.CREATION_DATE, creationDate );
                params.put( Domain.Post.BROWSER_USED, "safari" );
                params.put( Domain.Post.LOCATION_IP, "31.55.91.341" );
                return params;
            }

            protected static Map<String, Object> aiya2()
            {
                Calendar c = Calendar.getInstance();
                c.set( 2013, Calendar.SEPTEMBER, 9 );
                long creationDate = c.getTimeInMillis();

                Map<String, Object> params = new HashMap<String, Object>();
                params.put( Domain.Post.CONTENT, "[aiya2] bro" );
                params.put( Domain.Post.LANGUAGE, new String[] { "english" } );
                params.put( Domain.Post.IMAGE_FILE, "some image file" );
                params.put( Domain.Post.CREATION_DATE, creationDate );
                params.put( Domain.Post.BROWSER_USED, "safari" );
                params.put( Domain.Post.LOCATION_IP, "31.55.91.342" );
                return params;
            }

            protected static Map<String, Object> aiya3()
            {
                Calendar c = Calendar.getInstance();
                c.set( 2013, Calendar.SEPTEMBER, 5 );
                long creationDate = c.getTimeInMillis();

                Map<String, Object> params = new HashMap<String, Object>();
                params.put( Domain.Post.CONTENT, "[aiya3] chur" );
                params.put( Domain.Post.LANGUAGE, new String[] { "english" } );
                params.put( Domain.Post.IMAGE_FILE, "some image file" );
                params.put( Domain.Post.CREATION_DATE, creationDate );
                params.put( Domain.Post.BROWSER_USED, "safari" );
                params.put( Domain.Post.LOCATION_IP, "31.55.91.343" );
                return params;
            }

            protected static Map<String, Object> stranger1()
            {
                Calendar c = Calendar.getInstance();
                c.set( 2013, Calendar.SEPTEMBER, 2 );
                long creationDate = c.getTimeInMillis();

                Map<String, Object> params = new HashMap<String, Object>();
                params.put( Domain.Post.CONTENT, "[stranger1] gidday" );
                params.put( Domain.Post.LANGUAGE, new String[] { "english" } );
                params.put( Domain.Post.IMAGE_FILE, "some image file" );
                params.put( Domain.Post.CREATION_DATE, creationDate );
                params.put( Domain.Post.BROWSER_USED, "internet explorer" );
                params.put( Domain.Post.LOCATION_IP, "31.55.91.441" );
                return params;
            }

            protected static Map<String, Object> stranger2()
            {
                Calendar c = Calendar.getInstance();
                c.set( 2013, Calendar.SEPTEMBER, 5 );
                long creationDate = c.getTimeInMillis();

                Map<String, Object> params = new HashMap<String, Object>();
                params.put( Domain.Post.CONTENT, "[stranger2] i heart sheep" );
                params.put( Domain.Post.LANGUAGE, new String[] { "english" } );
                params.put( Domain.Post.IMAGE_FILE, "some image file" );
                params.put( Domain.Post.CREATION_DATE, creationDate );
                params.put( Domain.Post.BROWSER_USED, "internet explorer" );
                params.put( Domain.Post.LOCATION_IP, "31.55.91.442" );
                return params;
            }

            protected static Map<String, Object> nicky1()
            {
                Calendar c = Calendar.getInstance();
                c.set( 2013, Calendar.SEPTEMBER, 5 );
                long creationDate = c.getTimeInMillis();

                Map<String, Object> params = new HashMap<String, Object>();
                params.put( Domain.Post.CONTENT, "[nicky1] i live in england" );
                params.put( Domain.Post.LANGUAGE, new String[] { "english" } );
                params.put( Domain.Post.IMAGE_FILE, "some image file" );
                params.put( Domain.Post.CREATION_DATE, creationDate );
                params.put( Domain.Post.BROWSER_USED, "safari" );
                params.put( Domain.Post.LOCATION_IP, "33.125.1.451" );
                return params;
            }
        }

        protected static class Tags
        {
            protected static Map<String, Object> cake()
            {
                Map<String, Object> params = new HashMap<String, Object>();
                params.put( Domain.Tag.NAME, "cake" );
                params.put( Domain.Tag.URL, new String[] { "www.cake.good" } );
                return params;
            }

            protected static Map<String, Object> pie()
            {
                Map<String, Object> params = new HashMap<String, Object>();
                params.put( Domain.Tag.NAME, "pie" );
                params.put( Domain.Tag.URL, new String[] { "www.is.better" } );
                return params;
            }

            protected static Map<String, Object> lol()
            {
                Map<String, Object> params = new HashMap<String, Object>();
                params.put( Domain.Tag.NAME, "lol" );
                params.put( Domain.Tag.URL, new String[] { "www.lol.ol" } );
                return params;
            }

            protected static Map<String, Object> yolo()
            {
                Map<String, Object> params = new HashMap<String, Object>();
                params.put( Domain.Tag.NAME, "yolo" );
                params.put( Domain.Tag.URL, new String[] { "www.yolo.nu" } );
                return params;
            }

            protected static Map<String, Object> wtf()
            {
                Map<String, Object> params = new HashMap<String, Object>();
                params.put( Domain.Tag.NAME, "wtf" );
                params.put( Domain.Tag.URL, new String[] { "www.wtf.com" } );
                return params;
            }
        }

        protected static class Forums
        {
            protected static Map<String, Object> cakesAndPies()
            {
                Calendar c = Calendar.getInstance();
                c.set( 2013, Calendar.OCTOBER, 2 );
                long creationDate = c.getTimeInMillis();

                Map<String, Object> params = new HashMap<String, Object>();
                params.put( Domain.Forum.TITLE, "everything cakes and pies" );
                params.put( Domain.Forum.CREATION_DATE, creationDate );
                return params;
            }

            protected static Map<String, Object> redditAddicts()
            {
                Calendar c = Calendar.getInstance();
                c.set( 2013, Calendar.OCTOBER, 22 );
                long creationDate = c.getTimeInMillis();

                Map<String, Object> params = new HashMap<String, Object>();
                params.put( Domain.Forum.TITLE, "if it's not on reddit it's not on nothing" );
                params.put( Domain.Forum.CREATION_DATE, creationDate );
                return params;
            }

            protected static Map<String, Object> floatingBoats()
            {
                Calendar c = Calendar.getInstance();
                c.set( 2013, Calendar.NOVEMBER, 13 );
                long creationDate = c.getTimeInMillis();

                Map<String, Object> params = new HashMap<String, Object>();
                params.put( Domain.Forum.TITLE, "boats are not submarines" );
                params.put( Domain.Forum.CREATION_DATE, creationDate );
                return params;
            }

            protected static Map<String, Object> kiwisSheepAndBungyJumping()
            {
                Calendar c = Calendar.getInstance();
                c.set( 2013, Calendar.NOVEMBER, 1 );
                long creationDate = c.getTimeInMillis();

                Map<String, Object> params = new HashMap<String, Object>();
                params.put( Domain.Forum.TITLE, "kiwis sheep and bungy jumping" );
                params.put( Domain.Forum.CREATION_DATE, creationDate );
                return params;
            }
        }

        protected static class Comments
        {
            protected static Map<String, Object> aiya1()
            {
                Calendar c = Calendar.getInstance();
                c.set( 2013, Calendar.SEPTEMBER, 6 );
                long creationDate = c.getTimeInMillis();

                Map<String, Object> params = new HashMap<String, Object>();
                params.put( Domain.Post.CONTENT, "[aiya1] hi back" );
                params.put( Domain.Post.CREATION_DATE, creationDate );
                params.put( Domain.Post.BROWSER_USED, "safari" );
                params.put( Domain.Post.LOCATION_IP, "3.15.76.11" );
                return params;
            }

            protected static Map<String, Object> aiya2()
            {
                Calendar c = Calendar.getInstance();
                c.set( 2013, Calendar.SEPTEMBER, 6 );
                long creationDate = c.getTimeInMillis();

                Map<String, Object> params = new HashMap<String, Object>();
                params.put( Domain.Post.CONTENT, "[aiya2] so?" );
                params.put( Domain.Post.CREATION_DATE, creationDate );
                params.put( Domain.Post.BROWSER_USED, "safari" );
                params.put( Domain.Post.LOCATION_IP, "3.15.76.11" );
                return params;
            }

            protected static Map<String, Object> stranger1()
            {
                Calendar c = Calendar.getInstance();
                c.set( 2013, Calendar.SEPTEMBER, 7 );
                long creationDate = c.getTimeInMillis();

                Map<String, Object> params = new HashMap<String, Object>();
                params.put( Domain.Post.CONTENT, "[stranger1] i don't know you" );
                params.put( Domain.Post.CREATION_DATE, creationDate );
                params.put( Domain.Post.BROWSER_USED, "internet explorer" );
                params.put( Domain.Post.LOCATION_IP, "31.41.93.5" );
                return params;
            }

            protected static Map<String, Object> jake1()
            {
                Calendar c = Calendar.getInstance();
                c.set( 2013, Calendar.SEPTEMBER, 7 );
                long creationDate = c.getTimeInMillis();

                Map<String, Object> params = new HashMap<String, Object>();
                params.put( Domain.Post.CONTENT, "[jake1] pity you couldn't come" );
                params.put( Domain.Post.CREATION_DATE, creationDate );
                params.put( Domain.Post.BROWSER_USED, "safari" );
                params.put( Domain.Post.LOCATION_IP, "44.49.9.15" );
                return params;
            }

            protected static Map<String, Object> alex1()
            {
                Calendar c = Calendar.getInstance();
                c.set( 2013, Calendar.SEPTEMBER, 7 );
                long creationDate = c.getTimeInMillis();

                Map<String, Object> params = new HashMap<String, Object>();
                params.put( Domain.Post.CONTENT, "[alex1] chur bro" );
                params.put( Domain.Post.CREATION_DATE, creationDate );
                params.put( Domain.Post.BROWSER_USED, "safari" );
                params.put( Domain.Post.LOCATION_IP, "112.9.1.27" );
                return params;
            }
        }
    }
}
