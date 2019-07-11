import nodes.*;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;
import utils.TConf;
import utils.Variable;

public class TopologyQ2 {

    public static void main(String[] args) throws Exception {

        TConf config = new TConf();
        String redisUrl			= config.getString(TConf.REDIS_URL);
        int redisPort 			= config.getInteger(TConf.REDIS_PORT);
        String rabbitMqHost 	= config.getString(TConf.RABBITMQ_HOST);
        String rabbitMqUsername = config.getString(TConf.RABBITMQ_USERNAME);
        String rabbitMqPassword	= config.getString(TConf.RABBITMQ_PASSWORD);


        System.out.println("===================================================== ");
        System.out.println("Variable:");
        System.out.println("Redis: " + redisUrl + ":" + redisPort);
        System.out.println("RabbitMQ: " + rabbitMqHost + " (user: " + rabbitMqUsername + ", " + rabbitMqPassword + ")");
        System.out.println("===================================================== ");

        /*
         * Usage:
         * /apache-storm-1.1.0/bin/storm jar /data/SABDProject2-1.0-jar-with-dependencies.jar TopologyQ2 query2
         */

        /* Build topology */
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("datasource", new DataSourceSpout(redisUrl, redisPort), 5);

        builder.setBolt("filterQ2", new FilterQ2Bolt(), 5)
                .shuffleGrouping("datasource");


        // ------------------------------- Window 24 Hours -----------------------------------

        builder.setBolt("intermediateCounter24HourWindow",
                new IntermediateCounterBolt(24, redisUrl, redisPort, Variable.REDIS_INTER_COUNTER_24HOUR),
                8)
                .fieldsGrouping("filterQ2", new Fields(FilterQ2Bolt.F_ARTICLE_ID));

        builder.setBolt("globalCounter24HourWindow", new GlobalCounterBolt(), 1)
                .globalGrouping("intermediateCounter24HourWindow");

        builder.setBolt("exporter24HourWindowQ2",
                new ExporterQ2(rabbitMqHost, rabbitMqUsername, rabbitMqPassword, Variable.RABBITMQ_QUEUE_Q2_24HOUR),
                1)
                .shuffleGrouping("globalCounter24HourWindow");


        // ------------------------------- Window 7 Days -----------------------------------

        builder.setBolt("intermediateCounter7DayWindow",
                new IntermediateCounterBolt(24 * 7, redisUrl, redisPort, Variable.REDIS_INTER_COUNTER_7DAY),
                8)
                .fieldsGrouping("filterQ2", new Fields(FilterQ2Bolt.F_ARTICLE_ID));

        builder.setBolt("globalCounter7DayWindow", new GlobalCounterBolt(), 1)
                .globalGrouping("intermediateCounter7DayWindow");

        builder.setBolt("exporter7DayWindowQ2",
                new ExporterQ2(rabbitMqHost, rabbitMqUsername, rabbitMqPassword, Variable.RABBITMQ_QUEUE_Q2_7DAY),
                1)
                .shuffleGrouping("globalCounter7DayWindow");


        // ------------------------------- Window 1 Month -----------------------------------

        builder.setBolt("intermediateCounter1MonthWindow",
                new IntermediateCounterBolt(24 * 7 * 30, redisUrl, redisPort, Variable.REDIS_INTER_COUNTER_1MONTH),
                8)
                .fieldsGrouping("filterQ2", new Fields(FilterQ2Bolt.F_ARTICLE_ID));

        builder.setBolt("globalCounter1MonthWindow", new GlobalCounterBolt(), 1)
                .globalGrouping("intermediateCounter1MonthWindow");

        builder.setBolt("exporter1MonthWindowQ2",
                new ExporterQ2(rabbitMqHost, rabbitMqUsername, rabbitMqPassword, Variable.RABBITMQ_QUEUE_Q2_1MONTH),
                1)
                .shuffleGrouping("globalCounter1MonthWindow");


        /* Create configurations */
        Config conf = new Config();
        conf.setDebug(true);

        // local
        if (args.length == 0) {
            conf.setMaxTaskParallelism(3);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("query2", conf, builder.createTopology());
            Utils.sleep(100000);
            cluster.killTopology("query2");
            cluster.shutdown();
        } else {
            // cluster
            /* number of workers to create for current topology */
            conf.setNumWorkers(3);
            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        }

    }

}
