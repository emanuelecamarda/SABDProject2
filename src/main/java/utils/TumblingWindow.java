package utils;

import redis.clients.jedis.Jedis;
import java.io.Serializable;

public class TumblingWindow implements Serializable {

    private int windowLengthInHours;
    private long startTimestamp;
    private long endTimestamp;
    private Jedis jedis;
    int redisTimeout 	= 60000;
    private String redisKey;

    public TumblingWindow(int windowLengthInHours, long startTimestamp, String redisUrl, int redisPort,
                          String redisKey) {
        this.windowLengthInHours = windowLengthInHours;
        this.jedis = new Jedis(redisUrl, redisPort, redisTimeout);
        this.redisKey = redisKey;
        if (jedis.get(redisKey) == null) {
            this.startTimestamp = startTimestamp;
            jedis.set(redisKey, Long.valueOf(startTimestamp).toString());
        } else {
            this.startTimestamp = Long.parseLong(jedis.get(redisKey));
        }
        this.endTimestamp = startTimestamp + windowLengthInHours * 60 * 60;
    }

    public int getWindowLengthInHours() {
        return windowLengthInHours;
    }

    public long getStartTimestamp() {
        startTimestamp = Long.parseLong(jedis.get(redisKey));
        this.endTimestamp = startTimestamp + windowLengthInHours * 60 * 60;
        return startTimestamp;
    }

    public long getEndTimestamp() {
        return endTimestamp;
    }

    public void moveForward() {
        this.startTimestamp = endTimestamp;
        this.endTimestamp += windowLengthInHours * 60 * 60;
        jedis.set(redisKey, Long.valueOf(startTimestamp).toString());
    }

}
