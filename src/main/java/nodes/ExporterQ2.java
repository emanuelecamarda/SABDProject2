package nodes;

import org.apache.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import utils.RabbitMQManager;
import java.util.Map;

public class ExporterQ2 extends BaseRichBolt {

    private static final long serialVersionUID = 1L;
    private OutputCollector collector;
    private static final Logger LOG = Logger.getLogger(ExporterQ2.class);
    public static final String F_OUTPUT_RAW = "outputRaw";

    private RabbitMQManager rabbitmq;
    private String rabbitMqHost;
    private String rabbitMqUsername;
    private String rabbitMqPassword;
    private String defaultQueue;

    public ExporterQ2(String rabbitMqHost, String rabbitMqUsername, String rabbitMqPassword, String defaultQueue) {
        this.rabbitMqHost = rabbitMqHost;
        this.rabbitMqUsername = rabbitMqUsername;
        this.rabbitMqPassword = rabbitMqPassword;
        this.defaultQueue = defaultQueue;
    }

    @Override
    public void prepare(@SuppressWarnings("rawtypes") Map<String, Object> map, TopologyContext topologyContext,
                        OutputCollector outputCollector) {
        this.collector = outputCollector;
        this.rabbitmq = new RabbitMQManager(rabbitMqHost, rabbitMqUsername, rabbitMqPassword, defaultQueue);
    }

    @Override
    public void execute(Tuple tuple) {

        LOG.debug("Exp: rec1 = " + tuple.getValueByField(GlobalCounterBolt.F_COUNTS));
        Long[] counts = (Long[]) tuple.getValueByField(GlobalCounterBolt.F_COUNTS);

        LOG.debug("Exp: rec2 = " + counts);

        String raw = tuple.getLongByField(GlobalCounterBolt.F_TIMESTAMP).toString();
        for (int i = 0; i < counts.length; i++){
            raw += "," + counts[i];
        }

        LOG.debug("Exp: rec3 = " + raw);
        rabbitmq.send(raw);
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(F_OUTPUT_RAW));
    }
}
