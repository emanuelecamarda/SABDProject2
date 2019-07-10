package nodes;

import org.apache.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CommentCounterWindowedBolt extends BaseWindowedBolt {

    public static final String F_ARTICLE_ID         = "articleID";
    public static final String F_COUNT              = "count";
    public static final String F_START_TIMESTAMP    = "startTimestamp";
    private static final Logger LOG = Logger.getLogger(nodes.CommentCounterBolt.class);
    Map<String, Integer> counts = new HashMap<>();
    OutputCollector collector;

    public CommentCounterWindowedBolt(){
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(TupleWindow tuples) {
        List<Tuple> incoming = tuples.getNew();

        for (Tuple tuple : incoming) {
            String articleID = tuple.getString(0);
            Integer count = counts.get(articleID);
            if (count == null)
                count = 0;
            count++;
            counts.put(articleID, count);
        }

        List<Tuple> expired = tuples.getExpired();

        for (Tuple tuple : expired){
            String articleID = tuple.getString(0);
            Integer count = counts.get(articleID);
            if (count != null){
                count--;
                if (count > 0)
                    counts.put(articleID, count);
                else
                    counts.remove(articleID);
            }
        }

        Long startTimestamp = tuples.getStartTimestamp();

        for (String articleID : counts.keySet()){
            Integer count = counts.get(articleID);
            LOG.debug("Sending tuple (" + articleID + ", " + count + ", " + startTimestamp + ")");
            collector.emit(new Values(articleID, count, startTimestamp));
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(F_ARTICLE_ID, F_COUNT, F_START_TIMESTAMP));
    }

}
