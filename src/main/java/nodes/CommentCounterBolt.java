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
import utils.TumblingWindow;
import utils.TupleHelper;

import java.util.HashMap;
import java.util.Map;

public class CommentCounterBolt extends BaseRichBolt {

    public static final String F_ARTICLE_ID    = "articleID";
    public static final String F_COUNT         = "count";
    public static final String F_TIMESTAMP     = "timestamp";
    public static final long JAN_1_2018        = 1514764800;
    private static final Logger LOG = Logger.getLogger(CommentCounterBolt.class);
    private static final int DEFAULT_SLIDING_WINDOW_IN_HOUR = 24; // 24 hour window
    private final int windowLengthInHours;
    private static final int DEFAULT_EMIT_FREQUENCY_IN_SECONDS = 1;
    private OutputCollector _collector;
    private Map<String,Long> counter;
    private TumblingWindow window = null;
    private int emitFrequencyInSeconds;
    private long lastStartTimestamp = 0;
    private String redisUrl;
    private int redisPort;
    private String redisKey;

    public CommentCounterBolt(String redisUrl, int redisPort, String redisKey) {
        this(DEFAULT_SLIDING_WINDOW_IN_HOUR, DEFAULT_EMIT_FREQUENCY_IN_SECONDS, redisUrl, redisPort, redisKey);
    }

    public CommentCounterBolt(int windowLengthInHours, String redisUrl, int redisPort, String redisKey) {
        this(windowLengthInHours, DEFAULT_EMIT_FREQUENCY_IN_SECONDS, redisUrl, redisPort, redisKey);
    }

    public CommentCounterBolt(int windowLengthInHours, int emitFrequencyInSeconds, String redisUrl, int redisPort,
                              String redisKey) {

        if (emitFrequencyInSeconds < 1) {
            throw new IllegalArgumentException(
                    "The emit frequency must be >= 1 seconds (you requested " + emitFrequencyInSeconds + " seconds)");
        }

        if (windowLengthInHours < 0) {
            throw new IllegalArgumentException(
                    "The window's length in hours must be >= 0 hours (you requested " + windowLengthInHours
                            + " hours)");
        }
        
        this.windowLengthInHours = windowLengthInHours;
        this.emitFrequencyInSeconds = emitFrequencyInSeconds;
        counter = new HashMap<>();
        this.redisUrl = redisUrl;
        this.redisPort = redisPort;
        this.redisKey = redisKey;
    }

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector collector) {
        this._collector = collector;
        window = new TumblingWindow(windowLengthInHours, JAN_1_2018, redisUrl, redisPort, redisKey);
    }

    @Override
    public void execute(Tuple tuple) {

        if (TupleHelper.isTickTuple(tuple) && lastStartTimestamp != 0 &&
                window.getStartTimestamp() != lastStartTimestamp) {
            LOG.info("Triggering current window tuple.");
            emit();
            lastStartTimestamp = window.getStartTimestamp();
        } else if (TupleHelper.isTickTuple(tuple)){ // lastStartTimestamp == lastGlobalTimestamp
            // Do nothing
        } else { // not tick tuple

            if (lastStartTimestamp == 0) {
                lastStartTimestamp = window.getStartTimestamp();
                while (window.getEndTimestamp() < tuple.getLongByField(FilterQ1Bolt.F_CREATE_TIME)) {
                    window.moveForward();
                    lastStartTimestamp = window.getStartTimestamp();
                }
            }

            if (lastStartTimestamp + 3600 * windowLengthInHours < tuple.getLongByField(FilterQ1Bolt.F_CREATE_TIME)) {
                LOG.info("Triggering current window tuple.");
                emit();
                // case first tuple out of window, need to move window
                if (lastStartTimestamp == window.getStartTimestamp()) {
                    LOG.info("Moving window forward");
                    window.moveForward();
                    while (tuple.getLongByField(FilterQ1Bolt.F_CREATE_TIME) > window.getEndTimestamp()) {
                        window.moveForward();
                    }
                }
                lastStartTimestamp = window.getStartTimestamp();
                countAndAck(tuple);
            } else {
                countAndAck(tuple);
            }
        }
    }

    private void countAndAck(Tuple tuple) {
        String articleID = tuple.getStringByField(FilterQ1Bolt.F_ARTICLE_ID);
        if (counter.get(articleID) == null) {
            counter.put(articleID, Long.valueOf(1));
        } else {
            Long count = counter.get(articleID);
            counter.put(articleID, count + 1);
        }

        _collector.ack(tuple);
    }

    private void emit() {
        // emit all count in counter
        for (Map.Entry<String,Long> entry: counter.entrySet()) {
            String articleID = entry.getKey();
            Long count = entry.getValue();
            LOG.info("Emitting tuple ( articleID=" + articleID + ", count=" + count + ", timestamp="
                    + lastStartTimestamp + " )");
            System.out.println("Emitting tuple ( articleID=" + articleID + ", count=" + count + ", timestamp="
                    + lastStartTimestamp + " )");
            _collector.emit(new Values(articleID, count, lastStartTimestamp));
        }

        // reset counter
        this.counter = new HashMap<>();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(F_ARTICLE_ID, F_COUNT, F_TIMESTAMP));
    }

    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = new HashMap<>();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, emitFrequencyInSeconds);
        return conf;
    }
}
