package nodes;

import com.google.gson.Gson;
import org.apache.log4j.Logger;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;
import utils.Variable;
import utils.LinesBatch;
import org.apache.storm.topology.base.BaseRichSpout;

import java.util.Map;

public class DataSourceSpout extends BaseRichSpout {

    public static final String F_DATA 		    =	"RowString";
    public static final String F_MSGID		    = 	"MSGID";
    public static final String F_TIMESTAMP_PROC = 	"execTimestamp";

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = Logger.getLogger(DataSourceSpout.class);

    String redisUrl;
    int redisPort;
    int redisTimeout 	= 60000;
    static long msgId 	= 0;
    Jedis jedis;
    SpoutOutputCollector _collector;
    Gson gson;

    public DataSourceSpout(String redisUrl, int redisPort) {
        this.redisUrl = redisUrl;
        this.redisPort = redisPort;
    }

    @Override
    public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, SpoutOutputCollector collector) {
        jedis = new Jedis(redisUrl, redisPort, redisTimeout);
        _collector = collector;
        gson = new Gson();
    }

    @Override
    public void nextTuple() {
        try {

            // try get data from redis
            String data = jedis.get(Variable.REDIS_DATA);
            while (data == null){
                try {
                    Thread.sleep(Variable.SHORT_SLEEP);
                } catch (InterruptedException e) { }
                data = jedis.get(Variable.REDIS_DATA);
            }

            /* Remove file from Redis */
            jedis.del(Variable.REDIS_DATA);

            /* Send data from spout */
            LinesBatch linesBatch = gson.fromJson(data, LinesBatch.class);
            String now = String.valueOf(System.currentTimeMillis());

            for (String row : linesBatch.getLines()) {
                msgId++;
                Values values = new Values();
                values.add(Long.toString(msgId));
                values.add(row);
                values.add(now);
                LOG.debug("Sending row = \"" + row + "\"");
                this._collector.emit(values, msgId);
            }

            jedis.set(Variable.REDIS_CONSUMED, "true");

        } catch (JedisConnectionException e) {
            e.printStackTrace();
            jedis = new Jedis(redisUrl, redisPort, redisTimeout);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(F_MSGID, F_DATA, F_TIMESTAMP_PROC));
    }

    @Override
    public void close() {
        super.close();
        this.jedis.close();
    }

}
