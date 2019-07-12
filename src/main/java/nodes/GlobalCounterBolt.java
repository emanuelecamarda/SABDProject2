package nodes;

import org.apache.log4j.Logger;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class GlobalCounterBolt extends BaseBasicBolt {

    private static final long serialVersionUID = -8447525895532302198L;
    private static final Logger LOG = Logger.getLogger(GlobalCounterBolt.class);
    public static final String F_COUNTS = "counts";
    public static final String F_TIMESTAMP = "startTimestamp";

    private static long lastTimestamp = 0;
    private Long[] counter;

    public GlobalCounterBolt() {
        initializeCounter();
    }

    private void initializeCounter() {
        this.counter = new Long[12];
        for (int i = 0; i < counter.length; i++) {
            counter[i] = Long.valueOf(0);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(F_TIMESTAMP, F_COUNTS));
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {

        Long[] countsToBeMerged = (Long[]) tuple.getValueByField(IntermediateCounterBolt.F_COUNTS);
        long currentTimestamp = tuple.getLongByField(IntermediateCounterBolt.F_TIMESTAMP);

        if (lastTimestamp == 0)
            lastTimestamp = currentTimestamp;

        if (lastTimestamp < currentTimestamp) {
            LOG.debug("Received tick tuple, triggering emit of current rankings");
            collector.emit(new Values(lastTimestamp, counter));
            LOG.debug("Counter: " + counter);
            lastTimestamp = currentTimestamp;
            initializeCounter();
            LOG.debug("Counts: " + countsToBeMerged);
            updateCounter(countsToBeMerged);
        } else {
            LOG.debug("Counts: " + countsToBeMerged);
            updateCounter(countsToBeMerged);
        }
    }

    private void updateCounter(Long[] countsToBeMerged) {
        for (int i = 0; i < counter.length; i++) {
            counter[i] += countsToBeMerged[i];
        }
    }
}
