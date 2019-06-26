import nodes.CommentCounterBolt;
import nodes.DataSourceSpout;
import nodes.FilterQ1Bolt;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import utils.Variable;

public class Topology {

    public static void main(String[] args) throws Exception {

//        TConf config = new TConf();
//        String redisUrl			= config.getString(TConf.REDIS_URL);
//        int redisPort 			= config.getInteger(TConf.REDIS_PORT);
//        int numTasks 			= config.getInteger(TConf.NUM_TASKS);
//        int numTasksMetronome   = 1;  // each task of the metronome generate a flood of messages
//        int numTasksGlobalRank  = 1;
//        String rabbitMqHost 	= config.getString(TConf.RABBITMQ_HOST);
//        String rabbitMqUsername = config.getString(TConf.RABBITMQ_USERNAME);
//        String rabbitMqPassword	= config.getString(TConf.RABBITMQ_PASSWORD);
//
//
//        System.out.println("===================================================== ");
//        System.out.println("Variable:");
//        System.out.println("Redis: " + redisUrl + ":" + redisPort);
//        System.out.println("RabbitMQ: " + rabbitMqHost + " (user: " + rabbitMqUsername + ", " + rabbitMqPassword + ")");
//        System.out.println("Tasks:" + numTasks);
//        System.out.println("===================================================== ");
//
        /*
         * Usage:
         * /apache-storm-1.1.0/bin/storm jar /data/SABDProject2-1.0-jar-with-dependencies.jar Topology [redis url]
         */

        String redisUrl = Variable.REDIS_URL;
        int redisPort = Variable.REDIS_PORT;

        if (args.length > 0) {
            redisUrl = args[0];
        }

        /* Build topology */
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("datasource", new DataSourceSpout(redisUrl, redisPort), 5);

        builder.setBolt("filterQ1", new FilterQ1Bolt(), 5)
                .shuffleGrouping("datasource");

        builder.setBolt("CommentCounter1HourWindow", new CommentCounterBolt(1), 12)
                .fieldsGrouping("filterQ1", new Fields(FilterQ1Bolt.F_ARTICLE_ID));

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
//        /* Create configurations */
//        Config conf = new Config();
//        conf.setDebug(false);
//        /* number of workers to create for current topology */
//        conf.setNumWorkers(3);
//
//
//        /* Update numWorkers using command-line received parameters */
//        if (args.length == 2){
//            try{
//                if (args[1] != null){
//                    int numWorkers = Integer.parseInt(args[1]);
//                    conf.setNumWorkers(numWorkers);
//                    System.out.println("Number of workers to generate for current topology set to: " + numWorkers);
//                }
//            } catch (NumberFormatException nf){}
//        }
//
//        // local
////		LocalCluster cluster = new LocalCluster();
////        cluster.submitTopology("debs", conf, stormTopology);
////        Utils.sleep(100000);
////        cluster.killTopology("debs");
////        cluster.shutdown();
//
//        // cluster
//        StormSubmitter.submitTopology(args[0], conf, stormTopology);

    }

}
