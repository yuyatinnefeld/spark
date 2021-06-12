package com.learning.sparkstreaming.chapter6;

import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;
import redis.clients.jedis.Jedis;

public class RedisWriter extends ForeachWriter<Row> {


    private static Jedis jedis = null;
    private static final String lbKey= "topics-leaderboard";

    @Override public boolean open(long partitionId, long version) {
        // Open connection
        if ( jedis == null) {
            jedis = new Jedis("localhost");
        }
        return true;
    }
    @Override public void process(Row record) {
        // insert into DB
        String topic = record.getString(0);
        Double increment = Double.valueOf(record.getInt(1));
        jedis.zincrby(lbKey,increment,topic);

        System.out.println("Retrieved Redis Data " + record.toString() );
    }

    @Override public void close(Throwable errorOrNull) {
        // Close the connection
    }
}
