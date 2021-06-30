package com.learning.sparkstreaming.chapter4;

import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;
import redis.clients.jedis.Jedis;

public class RedisWriter extends ForeachWriter<Row> {


    private static Jedis jedis = null;
    private static final String lbKey= "player-leaderboard";

    @Override public boolean open(long partitionId, long version) {
        // Open connection
        if ( jedis == null) {
            jedis = new Jedis("localhost");
        }
        return true;
    }
    @Override public void process(Row record) {

        System.out.println("Retrieved Scores " + record.toString() );

        // Update Redis SortedSet with incremental scores
        String player = record.getString(0);
        Double increment = Double.valueOf(record.getString(1));

        jedis.zincrby(lbKey,increment,player);

    }

    @Override public void close(Throwable errorOrNull) {
        // Close the connection
    }
}
