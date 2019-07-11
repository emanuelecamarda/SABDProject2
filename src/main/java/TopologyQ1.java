import nodes.*;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;
import utils.TConf;
import utils.Variable;

import static utils.Variable.TOP_Q1;

public class TopologyQ1 {

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
         * /apache-storm-1.1.0/bin/storm jar /data/SABDProject2-1.0-jar-with-dependencies.jar TopologyQ1 query1
         */

        /* Build topology */
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("datasource", new DataSourceSpout(redisUrl, redisPort), 5);

        builder.setBolt("filterQ1", new FilterQ1Bolt(), 5)
                .shuffleGrouping("datasource");


        // ------------------------------- Window 1 Hour -----------------------------------

        builder.setBolt("commentCounter1HourWindow",
                new CommentCounterBolt(1, redisUrl, redisPort, Variable.REDIS_COUNTER_1HOUR),
                6)
                .fieldsGrouping("filterQ1", new Fields(FilterQ1Bolt.F_ARTICLE_ID));

        builder.setBolt("intermediateRanking1HourWindow",
                new IntermediateRankingBolt(TOP_Q1, redisUrl, redisPort, Variable.REDIS_INTER_RANKER_1HOUR),
                4)
                .fieldsGrouping("commentCounter1HourWindow", new Fields(CommentCounterBolt.F_ARTICLE_ID));

        builder.setBolt("globalRanking1HourWindow", new GlobalRankingBolt(TOP_Q1), 1)
                .globalGrouping("intermediateRanking1HourWindow");

        builder.setBolt("exporter1HourWindowQ1",
                new ExporterQ1(rabbitMqHost, rabbitMqUsername, rabbitMqPassword, Variable.RABBITMQ_QUEUE_Q1_1HOUR, TOP_Q1),
                1)
                .shuffleGrouping("globalRanking1HourWindow");


        // ------------------------------- Window 24 Hours -----------------------------------

        builder.setBolt("commentCounter24HourWindow",
                new CommentCounterBolt(24,10,  redisUrl, redisPort, Variable.REDIS_COUNTER_24HOUR),
                6)
                .fieldsGrouping("filterQ1", new Fields(FilterQ1Bolt.F_ARTICLE_ID));

        builder.setBolt("intermediateRanking24HourWindow",
                new IntermediateRankingBolt(TOP_Q1, 10, redisUrl, redisPort, Variable.REDIS_INTER_RANKER_24HOUR),
                8)
                .fieldsGrouping("commentCounter24HourWindow", new Fields(CommentCounterBolt.F_ARTICLE_ID));

        builder.setBolt("globalRanking24HourWindow", new GlobalRankingBolt(TOP_Q1),
                1)
                .globalGrouping("intermediateRanking24HourWindow");

        builder.setBolt("exporter24HourWindowQ1",
                new ExporterQ1(rabbitMqHost, rabbitMqUsername, rabbitMqPassword, Variable.RABBITMQ_QUEUE_Q1_24HOUR, TOP_Q1),
                1)
                .shuffleGrouping("globalRanking24HourWindow");


        // ------------------------------- Window 7 Days -----------------------------------

        builder.setBolt("commentCounter7DayWindow",
                new CommentCounterBolt(24 * 7, 60, redisUrl, redisPort, Variable.REDIS_COUNTER_7DAY),
                12)
                .fieldsGrouping("filterQ1", new Fields(FilterQ1Bolt.F_ARTICLE_ID));

        builder.setBolt("intermediateRanking7DayWindow",
                new IntermediateRankingBolt(TOP_Q1, 60, redisUrl, redisPort, Variable.REDIS_INTER_RANKER_7DAY),
                16)
                .fieldsGrouping("commentCounter7DayWindow", new Fields(CommentCounterBolt.F_ARTICLE_ID));

        builder.setBolt("globalRanking7DayWindow", new GlobalRankingBolt(TOP_Q1),
                1)
                .globalGrouping("intermediateRanking7DayWindow");

        builder.setBolt("exporter7DayWindowQ1",
                new ExporterQ1(rabbitMqHost, rabbitMqUsername, rabbitMqPassword, Variable.RABBITMQ_QUEUE_Q1_7DAY, TOP_Q1),
                1)
                .shuffleGrouping("globalRanking7DayWindow");



        /* Create configurations */
        Config conf = new Config();
        conf.setDebug(true);

        // local
        if (args.length == 0) {
            conf.setMaxTaskParallelism(3);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("query1", conf, builder.createTopology());
            Utils.sleep(100000);
            cluster.killTopology("query1");
            cluster.shutdown();
        } else {
            // cluster
            /* number of workers to create for current topology */
            conf.setNumWorkers(3);
            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        }

    }

}
