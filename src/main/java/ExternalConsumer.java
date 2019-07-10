import utils.RabbitMQManager;
import utils.TConf;
import utils.Variable;

public class ExternalConsumer {

    public static void main(String[] args) throws InterruptedException {
        /*
         * Usage:
         * java -cp <jar class path> ExternalConsumer <query number> <window size>
         */

        int queryNumber = 0, windowSize = 0;
        String rabbitMQQueue = "", outputFile = "", header;
        TConf config = new TConf();
        String rabbitMqHost 	= config.getString(TConf.RABBITMQ_HOST);
        String rabbitMqUsername = config.getString(TConf.RABBITMQ_USERNAME);
        String rabbitMqPassword	= config.getString(TConf.RABBITMQ_PASSWORD);

        if (args.length != 2) {
            System.err.println("Usage: java -cp <jar class path> ExternalConsumer <query number> <window size>");
        }
        try {
            queryNumber = Integer.parseInt(args[0]);
            windowSize = Integer.parseInt(args[1]);
        } catch (NumberFormatException e) {
            e.printStackTrace();
            System.exit(1);
        }
        if (queryNumber != 1 && queryNumber != 2) {
            System.err.println("Error: query number must be 1 or 2.");
            System.exit(1);
        }
        if (windowSize != 1 && windowSize != 2 && windowSize != 3) {
            System.err.println("Error: window size must be 1, 2 or 3.");
            System.exit(1);
        }

        if (queryNumber == 1) {
            header = "ts, artID_1, nCmnt_1, artID_2, nCmnt_2, artID_3, nCmnt_3";
            switch (windowSize) {
                case 1:
                    rabbitMQQueue = Variable.RABBITMQ_QUEUE_Q1_1HOUR;
                    outputFile = Variable.OUTPUT_FILE_Q1_1HOUR;
                    break;
                case 2:
                    rabbitMQQueue = Variable.RABBITMQ_QUEUE_Q1_24HOUR;
                    outputFile = Variable.OUTPUT_FILE_Q1_24HOUR;
                    break;
                case 3:
                    rabbitMQQueue = Variable.RABBITMQ_QUEUE_Q1_7DAY;
                    outputFile = Variable.OUTPUT_FILE_Q1_7DAY;
                    break;
            }
        } else {
            header = "ts ,count_h00, count_h02, count_h04, count_h06, count_h08, count_h10, count_h12, count_h14, " +
                    "count_h16, count_h18, count_h20, count_h22";
            switch (windowSize) {
                case 1:
                    rabbitMQQueue = Variable.RABBITMQ_QUEUE_Q2_24HOUR;
                    outputFile = Variable.OUTPUT_FILE_Q2_24HOUR;
                    break;
                case 2:
                    rabbitMQQueue = Variable.RABBITMQ_QUEUE_Q2_7DAY;
                    outputFile = Variable.OUTPUT_FILE_Q2_7DAY;
                    break;
                case 3:
                    rabbitMQQueue = Variable.RABBITMQ_QUEUE_Q2_1MONTH;
                    outputFile = Variable.OUTPUT_FILE_Q2_1MONTH;
                    break;
            }
        }

        System.out.println("===================================================== ");
        System.out.println("Variable:");
        System.out.println("RabbitMQ: " + rabbitMqHost + " (user: " + rabbitMqUsername + ", " + rabbitMqPassword + ")");
        System.out.println("===================================================== ");

        RabbitMQManager rmq = new RabbitMQManager(rabbitMqHost, rabbitMqUsername, rabbitMqPassword, rabbitMQQueue);
        rmq.createDetachedReaderOnFile(rabbitMQQueue, outputFile, header);	//RabbitMQ reader

        while(true){
            Thread.sleep(500);
        }

    }

}