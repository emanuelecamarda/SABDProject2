package nodes;

import org.apache.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import utils.Variable;
import java.util.Map;
import java.util.regex.Pattern;

public class FilterQ2Bolt extends BaseRichBolt {

    private static final Logger LOG = Logger.getLogger(FilterQ2Bolt.class);
    private OutputCollector _collector;
    private Pattern SEPARATOR;
    public static final String F_ARTICLE_ID	    = "articleID";
    public static final String F_CREATE_TIME    = "createTime";

    public FilterQ2Bolt() {}

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this._collector = outputCollector;
        this.SEPARATOR = Pattern.compile(",");
    }

    @Override
    public void execute(Tuple tuple) {
        String rawData 	= tuple.getStringByField(DataSourceSpout.F_DATA);

        /* Do NOT emit if the EOF has been reached */
        if (rawData == null || rawData.equals(Variable.REDIS_EOF)) {
            _collector.ack(tuple);
            return;
        }

        String[] data = SEPARATOR.split(rawData);
        if (!data[4].equals("comment"))
            return;
        Values values = new Values();
        values.add(data[1]);    // Article ID
        values.add(Long.parseLong(data[5]));    // Comment creation timestamp
        LOG.debug("Sending tuple: ( createTime=" + data[5] + " )");
        _collector.emit(values);
        _collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(F_ARTICLE_ID, F_CREATE_TIME));
    }
}
