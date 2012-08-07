/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */
package org.apache.cassandra.cql.jdbc;

import static org.junit.Assert.*;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import org.apache.cassandra.cql.ConnectionDetails;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *Test CQL Collections Data Types 
 * List
 * Map
 * Set
 *
 */
public class CollectionsTest
{
    private static final Logger LOG = LoggerFactory.getLogger(CollectionsTest.class);
    
    private static final String HOST = System.getProperty("host", ConnectionDetails.getHost());
    private static final int PORT = Integer.parseInt(System.getProperty("port", ConnectionDetails.getPort()+""));
    private static final String KEYSPACE = "testks";
    private static final String SYSTEM = "system";
    private static final String CQLV3 = "3.0.0";

    private static java.sql.Connection con = null;

    /**
     * @throws java.lang.Exception
     */
    @BeforeClass
    public static void setUpBeforeClass() throws Exception
    {
        Class.forName("org.apache.cassandra.cql.jdbc.CassandraDriver");
        String URL = String.format("jdbc:cassandra://%s:%d/%s?version=%s",HOST,PORT,SYSTEM,CQLV3);
        
        con = DriverManager.getConnection(URL);

        LOG.debug("URL = '{}'",URL);
        
        Statement stmt = con.createStatement();
        
        // Use Keyspace
        String useKS = String.format("USE %s;",KEYSPACE);
        
        // Drop Keyspace
        String dropKS = String.format("DROP KEYSPACE %s;",KEYSPACE);
        
        try { stmt.execute(dropKS);}
        catch (Exception e){/* Exception on DROP is OK */}

        // Create KeySpace
        String createKS = String.format("CREATE KEYSPACE %s WITH strategy_class = SimpleStrategy AND strategy_options:replication_factor = 1;",KEYSPACE);
        LOG.debug("createKS    = '{}'",createKS);
        
        stmt = con.createStatement();
        stmt.execute("USE " + SYSTEM);
        stmt.execute(createKS);
        stmt.execute(useKS);
        
       
        // Create the target Table (CF)
        String createTable = 
                        "CREATE TABLE testcollection ("
                        + " k int PRIMARY KEY," 
                        + " L list<float>,"
                        + " M map<double, boolean>,"
                        + " S set<text>"
                        + ") ;";
        LOG.debug("createTable = '{}'",createTable);

        stmt.execute(createTable);
        stmt.close();
        con.close();

        // open it up again to see the new TABLE
        URL = String.format("jdbc:cassandra://%s:%d/%s?version=%s",HOST,PORT,KEYSPACE,CQLV3); 
        con = DriverManager.getConnection(URL);
        LOG.debug("URL = '{}'",URL);
        LOG.debug("Test: 'CollectionTest' initialization complete.\n\n");
    }

    /**
     * Close down the connection when complete
     */
    @AfterClass
    public static void tearDownAfterClass() throws Exception
    {
        if (con!=null) con.close();
   }

    @Test
    public void test() throws Exception
    {
        Statement statement = con.createStatement();
        
        String insert = "INSERT INTO testcollection (k,L) VALUES( 1,[1.0, 3.0, 5.0]);";
        statement.executeUpdate(insert);
        
        String update1 = "UPDATE testcollection SET S = {'red', 'white', 'blue'} WHERE k = 1;";
        String update2 = "UPDATE testcollection SET M = {2.0: 'true', 4.0: 'true', 6.0 : 'true'} WHERE k = 1;";
        statement.executeUpdate(update1);
        statement.executeUpdate(update2);
        
        statement.close();
        
        statement = con.createStatement();
        ResultSet result = statement.executeQuery("SELECT * FROM testcollection WHERE k = 1;");
        result.next();
        
        assertEquals(1, result.getInt("k"));
    }

}
