package org.apache.cassandra.cql.jdbc;

import java.nio.ByteBuffer;
import java.util.LinkedList;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.transport.SimpleClient;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.junit.Test;

/**
 * @author John Sanda
 */
public class SimpleClientTest {

    @Test
    public void testClient() throws Exception {
        SimpleClient client = new SimpleClient("localhost", 9042);
        client.connect(true);

        String cql = "CREATE KEYSPACE rhqtest WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};";
//        client.execute(cql, ConsistencyLevel.ONE);
        client.execute("use rhqtest;", ConsistencyLevel.ONE);

        String rawMetricsCQL = "CREATE TABLE raw_metrics (\n" +
            "    schedule_id int,\n" +
            "    time timestamp,\n" +
            "    value double,\n" +
            "    PRIMARY KEY (schedule_id, time)\n" +
            ");";
        ResultMessage.Prepared result = client.prepare(rawMetricsCQL);
        client.executePrepared(result.statementId.bytes, new LinkedList<ByteBuffer>(), ConsistencyLevel.ONE);
    }

}
