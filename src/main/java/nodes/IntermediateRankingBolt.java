package nodes;

import org.apache.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import utils.Rankable;
import utils.RankableObjectWithFields;
import utils.Rankings;
import utils.TupleHelper;

import java.util.HashMap;
import java.util.Map;

public class IntermediateRankingBolt extends BaseBasicBolt {

    private static final long serialVersionUID = -1369800530256637409L;
    private static final Logger LOG = Logger.getLogger(IntermediateRankingBolt.class);

    private static final int DEFAULT_EMIT_FREQUENCY_IN_SECONDS = 2;
    private static final int DEFAULT_COUNT = 10;

    private final int emitFrequencyInSeconds;
    private final int count;
    private final Rankings rankings;

    public IntermediateRankingBolt() {

        this(DEFAULT_COUNT, DEFAULT_EMIT_FREQUENCY_IN_SECONDS);

    }

    public IntermediateRankingBolt(int topN) {

        this(topN, DEFAULT_EMIT_FREQUENCY_IN_SECONDS);

    }

    public IntermediateRankingBolt(int topN, int emitFrequencyInSeconds) {

        if (topN < 1) {
            throw new IllegalArgumentException("topN must be >= 1 (you requested " + topN + ")");
        }

        if (emitFrequencyInSeconds < 1) {
            throw new IllegalArgumentException(
                    "The emit frequency must be >= 1 seconds (you requested " + emitFrequencyInSeconds + " seconds)");
        }

        count = topN;
        this.emitFrequencyInSeconds = emitFrequencyInSeconds;
        rankings = new Rankings(count);

    }

    /**
     * This method functions as a template method (design pattern).
     *
     */
    public final void execute(Tuple tuple, BasicOutputCollector collector) {

        if (TupleHelper.isTickTuple(tuple)) {
            LOG.debug("Received tick tuple, triggering emit of current rankings");
            collector.emit(new Values(rankings.copy()));
            LOG.debug("Rankings: " + rankings);

        } else {

            RankableObjectWithFields rankable = RankableObjectWithFields.from(tuple);
            rankings.updateWith(rankable);

        }

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("rankings"));
    }

    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = new HashMap<>();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, emitFrequencyInSeconds);
        return conf;
    }

}
