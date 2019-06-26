package nodes;

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

public class FilterQ1Bolt extends BaseRichBolt {

    private OutputCollector _collector;
    private Pattern SEPARATOR;
    public static final String F_MSGID				= "MSGID";
    public static final String F_ARTICLE_ID	        = "articleID";
    public static final String F_CREATE_TIME 	    = "createTime";
    public static final String F_TIMESTAMP			= "timestamp";

    public FilterQ1Bolt() {
    }

    @Override
    public void prepare(@SuppressWarnings("rawtypes") Map map, TopologyContext topologyContext,
                        OutputCollector outputCollector) {
        this._collector = outputCollector;
        this.SEPARATOR = Pattern.compile(",");
    }

    @Override
    public void execute(Tuple tuple) {
        String rawData 	= tuple.getStringByField(DataSourceSpout.F_DATA);
        String msgId 	= tuple.getStringByField(DataSourceSpout.F_MSGID);
        String timestamp = tuple.getStringByField(DataSourceSpout.F_TIMESTAMP);


        /* Do NOT emit if the EOF has been reached */
        if (rawData == null || rawData.equals(Variable.REDIS_EOF)){
            _collector.ack(tuple);
            return;
        }

        String[] data = SEPARATOR.split(rawData);
        Values values = new Values();
        values.add(msgId);
        values.add(data[1]);    // Article ID
        values.add(data[5]);    // Comment creation timestamp
        values.add(timestamp);

        _collector.emit(values);
        _collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(F_MSGID, F_ARTICLE_ID, F_CREATE_TIME, F_TIMESTAMP));
    }

}
