package utils;

import nodes.FilterQ1Bolt;
import org.apache.storm.tuple.Tuple;
import redis.clients.jedis.Jedis;

import java.io.Serializable;

public class TumblingWindow implements Serializable {

    private int windowLenghtInHours;
    private long startTimestamp;
    private long endTimestamp;
    private Jedis jedis;
    int redisTimeout 	= 60000;

    public TumblingWindow(int windowLenghtInHours, long startTimestamp, String redisUrl, int redisPort) {
        this.windowLenghtInHours = windowLenghtInHours;
        this.jedis = new Jedis(redisUrl, redisPort, redisTimeout);
        if (jedis.get(Variable.REDIS_COMMENT_COUNTER) == null) {
            this.startTimestamp = startTimestamp;
            jedis.set(Variable.REDIS_COMMENT_COUNTER, Long.valueOf(startTimestamp).toString());
        } else {
            this.startTimestamp = Long.parseLong(jedis.get(Variable.REDIS_COMMENT_COUNTER));
        }
        this.endTimestamp = startTimestamp + windowLenghtInHours * 60 * 60;
    }

    public int getWindowLenghtInHours() {
        return windowLenghtInHours;
    }

    public long getStartTimestamp() {
        startTimestamp = Long.parseLong(jedis.get(Variable.REDIS_COMMENT_COUNTER));
        this.endTimestamp = startTimestamp + windowLenghtInHours * 60 * 60;
        return startTimestamp;
    }

    public long getEndTimestamp() {
        return endTimestamp;
    }

    public void moveForward() {
        this.startTimestamp = endTimestamp;
        this.endTimestamp += windowLenghtInHours * 60 * 60;
        jedis.set(Variable.REDIS_COMMENT_COUNTER, Long.valueOf(startTimestamp).toString());
    }

    public boolean isOutOfWindowTuple(Tuple tuple) {
        if (tuple.getLongByField(FilterQ1Bolt.F_CREATE_TIME) > endTimestamp)
            return true;
        return false;
    }
}
