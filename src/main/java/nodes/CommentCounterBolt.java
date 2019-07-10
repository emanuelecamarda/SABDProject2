package nodes;

import org.apache.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import utils.TumblingWindowCounter;

import java.util.Map;

public class CommentCounterBolt extends BaseRichBolt {

    /*
     * This class implements a comment counter for incoming tuple. It has a tumbling windows base on event time. After
     * coming a tuple with event time over the window, this last are shifting and reset, and the bolt emit the
     * statistics.
     */

    public static final String F_ARTICLE_ID    = "articleID";
    public static final String F_COUNT         = "count";
    public static final String F_TIMESTAMP     = "timestamp";
    private static final Logger LOG = Logger.getLogger(CommentCounterBolt.class);
    private static final int DEFAULT_SLIDING_WINDOW_IN_HOUR = 24; // 24 hour window
    private final TumblingWindowCounter<Object> counter;
    private final int windowLengthInHours;
    private OutputCollector _collector;

    public CommentCounterBolt() {
        this(DEFAULT_SLIDING_WINDOW_IN_HOUR);
    }


    public CommentCounterBolt(int windowLengthInHours) {
        this.windowLengthInHours = windowLengthInHours;
        this.counter = new TumblingWindowCounter<>(this.windowLengthInHours);
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this._collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        if (counter.checkShiftWindow(tuple)) {
            LOG.debug("Received tick tuple, triggering emit of current window counts");
            Long timestampStart = counter.getBatchFinalDate().getTimeInMillis() / 1000 - windowLengthInHours * 60 * 60;
            Map<Object, Long> counts = counter.getCountThenAdvanceWindow();
            emit(counts, timestampStart);
            while (counter.checkShiftWindow(tuple)) {
                timestampStart = counter.getBatchFinalDate().getTimeInMillis() / 1000 - windowLengthInHours * 60 * 60;
                counts = counter.getCountThenAdvanceWindow();
                emit(counts, timestampStart);
            }
        } else {
            countObjAndAck(tuple);
        }
    }

    private void emit(Map<Object, Long> counts, Long timestampStart) {
        for (Map.Entry<Object, Long> entry : counts.entrySet()) {
            Object obj = entry.getKey();
            Long count = entry.getValue();
            LOG.debug("Emitting tuple: ( articleID=" + obj + ", count=" + count + " )");
            _collector.emit(new Values(obj, count, timestampStart));
        }
    }

    private void countObjAndAck(Tuple tuple) {
        Object obj = tuple.getStringByField(FilterQ1Bolt.F_ARTICLE_ID);
        counter.incrementCount(obj);
        _collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(F_ARTICLE_ID, F_COUNT, F_TIMESTAMP));
    }

}
