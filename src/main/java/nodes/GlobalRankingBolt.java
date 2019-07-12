package nodes;

import org.apache.log4j.Logger;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import utils.Rankings;

public final class GlobalRankingBolt extends BaseBasicBolt {

    private static final long serialVersionUID = -8447525895532302198L;
    private static final Logger LOG = Logger.getLogger(GlobalRankingBolt.class);
    public static final String F_RANKINGS = "rankings";
    public static final String F_START_TIMESTAMP = "startTimestamp";
    public static final int DEFAULT_TOP_N = 3;

    private int topN;
    private static long lastTimestamp = 0;
    private Rankings rankings;

    public GlobalRankingBolt() { this(DEFAULT_TOP_N); }

    public GlobalRankingBolt(int topN) {

        if (topN < 1) {
            throw new IllegalArgumentException("topN must be >= 1 (you requested " + topN + ")");
        }

        this.topN = topN;
        rankings = new Rankings(topN);
    }

    /**
     * This method functions as a template method (design pattern).
     */
    @Override
    public final void execute(Tuple tuple, BasicOutputCollector collector) {

        Rankings rankingsToBeMerged = (Rankings) tuple.getValueByField(IntermediateRankingBolt.F_RANKINGS);
        long currentTimestamp = tuple.getLongByField(IntermediateRankingBolt.F_START_TIMESTAMP);

        if (lastTimestamp == 0)
            lastTimestamp = currentTimestamp;

        if (lastTimestamp < currentTimestamp) {
            LOG.debug("Received tick tuple, triggering emit of current rankings");
            collector.emit(new Values(rankings.copy(), lastTimestamp));
            LOG.debug("Rankings: " + rankings);
            lastTimestamp = currentTimestamp;
            this.rankings = new Rankings(topN);
            LOG.debug("rankings: " + rankingsToBeMerged);
            rankings.updateWith(rankingsToBeMerged);
            rankings.pruneZeroCounts();
        } else {
            LOG.debug("rankings: " + rankingsToBeMerged);
            rankings.updateWith(rankingsToBeMerged);
            rankings.pruneZeroCounts();
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(F_RANKINGS, F_START_TIMESTAMP));
    }

}
