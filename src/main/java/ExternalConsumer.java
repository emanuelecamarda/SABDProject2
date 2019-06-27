import org.apache.storm.shade.org.apache.commons.io.output.FileWriterWithEncoding;
import org.apache.storm.tuple.Values;
import utils.RabbitMQManager;
import utils.Variable;

import java.io.FileWriter;
import java.io.IOException;

public class ExternalConsumer {

    public static void main(String[] args) throws InterruptedException {
        /*
         * Usage:
         * java -cp <jar class path> ExternalConsumer [query number] [window size]
         */

        int queryNumber = 0, windowSize = 0;
        String rabbitMQQueue = "", outputFile = "", header;

        if (args.length != 2) {
            System.err.println("Usage: java -cp <jar class path> ExternalConsumer [query number] [window size]");
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

        // TODO output file
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

        RabbitMQManager rmq = new RabbitMQManager(Variable.RABBITMQ_HOST, Variable.RABBITMQ_USER,
                Variable.RABBITMQ_PASS, rabbitMQQueue);

        try {
            FileWriter writer = new FileWriter(outputFile);
            writer.append(header).append(System.getProperty("line.separator"));
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        rmq.createDetachedReaderOnFile(rabbitMQQueue, outputFile);	//RabbitMQ reader

        while(true){
            Thread.sleep(500);
        }

    }

}