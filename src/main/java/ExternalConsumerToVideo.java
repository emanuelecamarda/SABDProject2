import utils.RabbitMQManager;
import utils.TConf;
import utils.Variable;

public class ExternalConsumerToVideo {

    public static void main(String[] args) throws InterruptedException {

        TConf conf = new TConf();
        String rabbitMQ = conf.getString(TConf.RABBITMQ_HOST);
        String rabbitMQUsername = conf.getString(TConf.RABBITMQ_USERNAME);
        String rabbitMQPassword = conf.getString(TConf.RABBITMQ_PASSWORD);
        String rabbitMQQueue = Variable.RABBITMQ_QUEUE_Q1_1HOUR;

        if (args.length > 0) { rabbitMQ = args[0]; }
        if (args.length > 1) { rabbitMQUsername	 = args[1]; }
        if (args.length > 2) { rabbitMQPassword = args[2]; }
        if (args.length > 3) { rabbitMQQueue = args[3]; }

        RabbitMQManager rmq = new RabbitMQManager(rabbitMQ, rabbitMQUsername, rabbitMQPassword, rabbitMQQueue);

        rmq.createDetachedReader(rabbitMQQueue);	//RabbitMQ reader

        while(true){
            Thread.sleep(500);
        }

    }

}
