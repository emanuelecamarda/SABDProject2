package nodes;

import org.apache.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import utils.RabbitMQManager;
import utils.Rankable;
import utils.Rankings;

import java.util.Map;

public class ExporterQ1 extends BaseRichBolt {

    private static final long serialVersionUID = 1L;
    private OutputCollector collector;
    private static final Logger LOG = Logger.getLogger(ExporterQ1.class);
    public static final String F_OUTPUT_RAW = "outputRaw";

    private RabbitMQManager rabbitmq;
    private String rabbitMqHost;
    private String rabbitMqUsername;
    private String rabbitMqPassword;
    private String defaultQueue;
    private int topN;

    public ExporterQ1(String rabbitMqHost, String rabbitMqUsername, String rabbitMqPassword,
                                String defaultQueue, int topN) {
        super();
        this.rabbitMqHost = rabbitMqHost;
        this.rabbitMqUsername = rabbitMqUsername;
        this.rabbitMqPassword = rabbitMqPassword;
        this.defaultQueue = defaultQueue;
        this.topN = topN;
    }

    @Override
    public void prepare(@SuppressWarnings("rawtypes") Map map, TopologyContext topologyContext,
                        OutputCollector outputCollector) {

        this.collector=outputCollector;
        this.rabbitmq = new RabbitMQManager(rabbitMqHost, rabbitMqUsername, rabbitMqPassword, defaultQueue);

    }

    @Override
    public void execute(Tuple tuple) {

        LOG.info("Exp: rec1 = " + tuple.getValueByField(GlobalRankingBolt.F_RANKINGS));
        Rankings rankings  = (Rankings) tuple.getValueByField(GlobalRankingBolt.F_RANKINGS);

        LOG.info("Exp: rec2 = " + rankings);
        LOG.info("Exp: rec3 = " + rankings.getRankings());

        int i = 0;
        String raw = tuple.getLongByField(GlobalRankingBolt.F_START_TIMESTAMP).toString();
        for (Rankable r : rankings.getRankings()){
            raw += "," + r.getObject() + "," + r.getCount();
            i++;
        }
        while (i != this.topN) {
            raw += ",,";
            i++;
        }
        raw+="\n";

        LOG.info("Exp: rec4 = " + raw);
        rabbitmq.send(raw);
        collector.ack(tuple);

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

        outputFieldsDeclarer.declare(new Fields(F_OUTPUT_RAW));

    }

}