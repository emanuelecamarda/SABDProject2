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
import utils.CircularTumblingWindow;
import utils.TumblingWindow;
import utils.TupleHelper;

import java.util.HashMap;
import java.util.Map;

public class IntermediateCounterBolt extends BaseRichBolt {

    public static final String F_COUNTS         = "counts";
    public static final String F_TIMESTAMP     = "startTimestamp";
    public static final long JAN_1_2018        = 1514764800;
    private static final Logger LOG = Logger.getLogger(IntermediateCounterBolt.class);
    private static final int DEFAULT_SLIDING_WINDOW_IN_HOUR = 24; // 24 hour window
    private final int windowLengthInHours;
    private static final int DEFAULT_EMIT_FREQUENCY_IN_SECONDS = 2;
    private OutputCollector _collector;
    private Long[] counter;
    private TumblingWindow outsideWindow = null;
    private CircularTumblingWindow insideWindow = null;
    private int emitFrequencyInSeconds;
    private long lastStartTimestamp = 0;
    private String redisUrl;
    private int redisPort;
    private String redisKey;

    public IntermediateCounterBolt(String redisUrl, int redisPort, String redisKey) {
        this(DEFAULT_SLIDING_WINDOW_IN_HOUR, DEFAULT_EMIT_FREQUENCY_IN_SECONDS, redisUrl, redisPort, redisKey);
    }

    public IntermediateCounterBolt(int windowLengthInHours, String redisUrl, int redisPort, String redisKey) {
        this(windowLengthInHours, DEFAULT_EMIT_FREQUENCY_IN_SECONDS, redisUrl, redisPort, redisKey);
    }

    public IntermediateCounterBolt(int windowLengthInHours, int emitFrequencyInSeconds, String redisUrl, int redisPort,
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
        initializeCounter();
        this.redisUrl = redisUrl;
        this.redisPort = redisPort;
        this.redisKey = redisKey;
    }

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this._collector = outputCollector;
        outsideWindow = new TumblingWindow(windowLengthInHours, JAN_1_2018, redisUrl, redisPort, redisKey);
        insideWindow = new CircularTumblingWindow(2, JAN_1_2018, 12);
    }

    @Override
    public void execute(Tuple tuple) {

        if (TupleHelper.isTickTuple(tuple) && lastStartTimestamp != 0 &&
                outsideWindow.getStartTimestamp() != lastStartTimestamp) {
            LOG.info("Triggering current window tuple.");
            _collector.emit(new Values(counter, lastStartTimestamp));
            initializeCounter();
            lastStartTimestamp = outsideWindow.getStartTimestamp();
        } else if (TupleHelper.isTickTuple(tuple)){ // lastStartTimestamp == lastGlobalTimestamp
            // Do nothing
        } else { // not tick tuple

            if (lastStartTimestamp == 0)
                lastStartTimestamp = outsideWindow.getStartTimestamp();

            if (tuple.getLongByField(FilterQ2Bolt.F_CREATE_TIME) > outsideWindow.getEndTimestamp()) {
                LOG.info("Triggering current window tuple.");
                _collector.emit(new Values(counter, lastStartTimestamp));
                initializeCounter();
                // case first tuple out of window, need to move window
                if (lastStartTimestamp == outsideWindow.getStartTimestamp()) {
                    LOG.info("Moving window forward");
                    outsideWindow.moveForward();
                    while (tuple.getLongByField(FilterQ2Bolt.F_CREATE_TIME) > outsideWindow.getEndTimestamp()) {
                        outsideWindow.moveForward();
                    }
                }
                lastStartTimestamp = outsideWindow.getStartTimestamp();
                countAndAck(tuple);
            } else {
                countAndAck(tuple);
            }
        }

    }

    private void countAndAck(Tuple tuple) {
        while (tuple.getLongByField(FilterQ2Bolt.F_CREATE_TIME) > insideWindow.getEndTimestamp())
            insideWindow.moveForward();
        counter[insideWindow.getSlot()]++;
        _collector.ack(tuple);
    }

    private void initializeCounter() {
        this.counter = new Long[12];
        for (int i = 0; i < counter.length; i++) {
            counter[i] = Long.valueOf(0);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(F_COUNTS, F_TIMESTAMP));
    }

    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = new HashMap<>();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, emitFrequencyInSeconds);
        return conf;
    }
}
