package nodes;

import org.apache.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import redis.clients.jedis.Jedis;
import utils.RankableObjectWithFields;
import utils.Rankings;
import utils.TupleHelper;
import java.util.HashMap;
import java.util.Map;

public class IntermediateRankingBolt extends BaseRichBolt {

    private static final long serialVersionUID = -1369800530256637409L;
    private static final Logger LOG = Logger.getLogger(IntermediateRankingBolt.class);

    private static final int DEFAULT_COUNT = 10;
    public static final String F_RANKINGS           = "rankings";
    public static final String F_START_TIMESTAMP    = "startTimestamp";
    private static final int DEFAULT_EMIT_FREQUENCY_IN_SECONDS = 2;

    private final int topN;
    private Rankings rankings;
    private long lastTimestamp = 0;
    private int emitFrequencyInSeconds;
    private Jedis jedis;
    int redisTimeout 	= 60000;
    private OutputCollector collector;
    private String redisUrl;
    private int redisPort;
    private String redisKey;

    public IntermediateRankingBolt(String redisUrl, int redisPort, String redisKey) {
        this(DEFAULT_COUNT, DEFAULT_EMIT_FREQUENCY_IN_SECONDS, redisUrl, redisPort, redisKey);
    }

    public IntermediateRankingBolt(int topN, String redisUrl, int redisPort, String redisKey) {
        this(topN, DEFAULT_EMIT_FREQUENCY_IN_SECONDS, redisUrl, redisPort, redisKey);
    }

    public IntermediateRankingBolt(int topN, int emitFrequencyInSeconds, String redisUrl, int redisPort,
                                   String redisKey) {

        if (topN < 1) {
            throw new IllegalArgumentException("topN must be >= 1 (you requested " + topN + ")");
        }

        if (emitFrequencyInSeconds < 1) {
            throw new IllegalArgumentException(
                    "The emit frequency must be >= 1 seconds (you requested " + emitFrequencyInSeconds + " seconds)");
        }

        this.topN = topN;
        this.emitFrequencyInSeconds = emitFrequencyInSeconds;
        rankings = new Rankings(this.topN);
        this.redisUrl = redisUrl;
        this.redisPort = redisPort;
        this.redisKey = redisKey;
    }

    /**
     * This method functions as a template method (design pattern).
     */
    @Override
    public final void execute(Tuple tuple) {

        if (TupleHelper.isTickTuple(tuple) && lastTimestamp != 0 && lastTimestamp < getLastGlobalTimestamp()) {
            LOG.debug("Move window, emitting current rankings");
            collector.emit(new Values(rankings.copy(), lastTimestamp));
            LOG.debug("Rankings: " + rankings);
            lastTimestamp = getLastGlobalTimestamp();
            this.rankings = new Rankings(topN);
        } else if (TupleHelper.isTickTuple(tuple)){ // lastTimestamp == lastGlobalTimestamp
            // Do nothing
        } else { // not tick tuple

            // initialize last timestamp
            if (getLastGlobalTimestamp() == 0) {
                long lastGlobalTimestamp = tuple.getLongByField(CommentCounterBolt.F_TIMESTAMP);
                jedis.set(redisKey, Long.valueOf(lastGlobalTimestamp).toString());
            }

            if (lastTimestamp == 0)
                lastTimestamp = getLastGlobalTimestamp();

            if (lastTimestamp < tuple.getLongByField(CommentCounterBolt.F_TIMESTAMP)) {
                LOG.debug("Move window, emitting current rankings");
                collector.emit(new Values(rankings.copy(), lastTimestamp));
                LOG.debug("Rankings: " + rankings);
                // case first tuple out of window
                if (lastTimestamp == getLastGlobalTimestamp()) {
                    long lastGlobalTimestamp = tuple.getLongByField(CommentCounterBolt.F_TIMESTAMP);
                    jedis.set(redisKey, Long.valueOf(lastGlobalTimestamp).toString());
                }
                lastTimestamp = getLastGlobalTimestamp();
                this.rankings = new Rankings(topN);
                RankableObjectWithFields rankable = RankableObjectWithFields.from(tuple);
                LOG.debug("rankable: " + rankable);
                rankings.updateWith(rankable);
            } else {
                RankableObjectWithFields rankable = RankableObjectWithFields.from(tuple);
                LOG.debug("rankable: " + rankable);
                rankings.updateWith(rankable);
            }

        }

    }

    private long getLastGlobalTimestamp() {
        if (jedis.get(redisKey) == null)
            return 0;
        else return Long.parseLong(jedis.get(redisKey));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(F_RANKINGS, F_START_TIMESTAMP));
    }

    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = new HashMap<>();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, emitFrequencyInSeconds);
        return conf;
    }

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector collector) {
        this.collector = collector;
        jedis = new Jedis(redisUrl, redisPort, redisTimeout);
    }

}
