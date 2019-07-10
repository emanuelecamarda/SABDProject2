import clojure.lang.RT;
import nodes.*;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;
import utils.TConf;
import utils.Variable;

import static utils.Variable.TOP_Q1;

public class Topology {

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
         * /apache-storm-1.1.0/bin/storm jar /data/SABDProject2-1.0-jar-with-dependencies.jar Topology [redis url]
         */

        if (args.length > 1) {
            redisUrl = args[1];
        }

        /* Build topology */
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("datasource", new DataSourceSpout(redisUrl, redisPort), 5);

        builder.setBolt("filterQ1", new FilterQ1Bolt(), 5)
                .shuffleGrouping("datasource");

//        builder.setBolt("commentCounter1HourWindow", new CommentCounterBolt(1), 12)
//                .fieldsGrouping("filterQ1", new Fields(FilterQ1Bolt.F_ARTICLE_ID));

//        builder.setBolt("commentCounter1HourWindow", new CommentCounterWindowedBolt()
//                .withTumblingWindow(BaseWindowedBolt.Duration.hours(1)).withTimestampField(FilterQ1Bolt.F_CREATE_TIME),
//                12)
//                .fieldsGrouping("filterQ1", new Fields(FilterQ1Bolt.F_ARTICLE_ID));

        builder.setBolt("commentCounter1HourWindow",
                new MyCommentCounterBolt(1, redisUrl, redisPort), 12)
                .fieldsGrouping("filterQ1", new Fields(FilterQ1Bolt.F_ARTICLE_ID));

        builder.setBolt("intermediateRanking1HourWindow", new IntermediateRankingBolt(TOP_Q1, redisUrl, redisPort),
                6)
                .fieldsGrouping("commentCounter1HourWindow", new Fields(CommentCounterBolt.F_COUNT));

        builder.setBolt("globalRanking1HourWindow", new GlobalRankingBolt(TOP_Q1), 1)
                .globalGrouping("intermediateRanking1HourWindow");

        builder.setBolt("exporter1HourWindowQ1", new ExporterQ1(rabbitMqHost, rabbitMqUsername, rabbitMqPassword,
                Variable.RABBITMQ_QUEUE_Q1_1HOUR, TOP_Q1), 1)
                .shuffleGrouping("globalRanking1HourWindow");

//        builder.setBolt("metronome", new Metronome())
//                .setNumTasks(numTasksMetronome)
//                .shuffleGrouping("filterByCoordinates");
//
//        builder.setBolt("computeCellID", new ComputeCellID())
//                .setNumTasks(numTasks)
//                .shuffleGrouping("filterByCoordinates");
//
//        builder.setBolt("countByWindow", new CountByWindow())
//                .setNumTasks(numTasks)
//                .fieldsGrouping("computeCellID", new Fields(ComputeCellID.F_ROUTE))
//                .allGrouping("metronome", Metronome.S_METRONOME);
//
//		/* Two operators that realize the top-10 ranking in two steps (typical design pattern):
//        PartialRank can be distributed and parallelized,
//        whereas TotalRank is centralized and computes the global ranking */
//
//        builder.setBolt("partialRank", new PartialRank(10))
//                .setNumTasks(numTasks)
//                .fieldsGrouping("countByWindow", new Fields(ComputeCellID.F_ROUTE));
//
//        builder.setBolt("globalRank", new GlobalRank(10, rabbitMqHost, rabbitMqUsername, rabbitMqPassword), 1)
//                .setNumTasks(numTasksGlobalRank)
//                .shuffleGrouping("partialRank");
//
//        StormTopology stormTopology = builder.createTopology();
//
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
