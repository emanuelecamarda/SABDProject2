import com.google.gson.Gson;
import org.apache.storm.utils.Time;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;
import utils.TConf;
import utils.Variable;
import utils.LinesBatch;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class DataSource implements Runnable {

    /*
     * This Thread had the objective to simulate a real stream of data, reading data from
     * csv file and save it on Redis. This are then consumed by a spout.
     *
     * To accelerate the process, data are reading from file with a granularity of TIMESPAN (minutes).
     *
     * It is possible to accelerate real time event throw SPEEDUP, that is a constant that
     * expressed the rapports between real time and process time; for example if SPEEDUP is 60,
     * a minute in real time corresponding to an hour in process time.
     *
     * As event time is been chosen the create time of a comment.
     */

    private static final int TIMESPAN = 15;     // expressed in mins
    private static final int SPEEDUP = 800;

    private Jedis jedis;
    private String filename;
    private int redisTimeout = 60000;
    private Gson gson;
    private Boolean hasHead;

    public DataSource(String filename, String redisUrl, int redisPort, Boolean hasHead){

        this.filename = filename;
        this.jedis = new Jedis(redisUrl, redisPort, redisTimeout);
        this.gson = new Gson();
        this.hasHead = hasHead;

        initialize();
    }

    private void initialize(){
        jedis.del(Variable.REDIS_CONSUMED);
        jedis.del(Variable.REDIS_DATA);
    }

    @Override
    public void run() {

        BufferedReader br = null;
        LinesBatch linesBatch;

        try {
            br = new BufferedReader(new FileReader(filename));

            String line = br.readLine();
            if (this.hasHead)       // skipping header
                line = br.readLine();
            linesBatch = new LinesBatch();
            long batchFinalTime 	= computeBatchFinalTime(getEventTime(line));
            long firstSendingTime 	= System.currentTimeMillis();
            linesBatch.addLine(line);

            while ((line = br.readLine()) != null) {

                long eventTime = getEventTime(line);

                if (eventTime < batchFinalTime){
                    linesBatch.addLine(line);
                    continue;
                }

                System.out.println("Sending " + linesBatch.getLines().size() + " lines");

                /* batch is concluded and has to be sent */
                if (linesBatch.getLines().size() != 0)
                    send(linesBatch);

                /* sleep if needed */
                long sendingTime = System.currentTimeMillis() - firstSendingTime;
                if (sendingTime < (TIMESPAN * 60 * 1000) / SPEEDUP) {
                    long timeToSleep = (TIMESPAN * 60 * 1000) / SPEEDUP - sendingTime;
                    try {
                        System.out.println("Go to sleep for " + timeToSleep + " ms");
                        Thread.sleep(timeToSleep);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                } else {
                    System.err.println("Error: SPEEDUP parameter to high, cannot simulate real data stream!");
                }

                /* update batch */
                linesBatch = new LinesBatch();
                batchFinalTime = computeBatchFinalTime(batchFinalTime);
                firstSendingTime = System.currentTimeMillis();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("Finished sending data.");
        if (br != null){  //TODO == o != ?
            try {
                br.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    private void send(LinesBatch linesBatch) throws JedisConnectionException {

        String consumed = jedis.get(Variable.REDIS_CONSUMED);
        String data = jedis.get(Variable.REDIS_DATA);

        /* Check erroneous situation */
        if (data != null && consumed != null){

            jedis.del(Variable.REDIS_CONSUMED);
            jedis.del(Variable.REDIS_DATA);

        }

        /* Wait if the consumer is still reading data */
        if (data != null && consumed == null){

            while (consumed == null){

                try {
                    Thread.sleep(Variable.SHORT_SLEEP);
                } catch (InterruptedException e) { }

                consumed = jedis.get(Variable.REDIS_CONSUMED);

            }

        }

        /* Remove lock from Redis */
        jedis.del(Variable.REDIS_CONSUMED);

        /* Send data */
        String serializedBatch = gson.toJson(linesBatch);
        jedis.set(Variable.REDIS_DATA, serializedBatch);

    }

    private long getEventTime(String line){

        long ts	= 0;
        try {
            ts = Long.parseLong(line.split(",")[5]); // Use createTime as event time
        } catch (NumberFormatException e) {
            e.printStackTrace();
        }
        return ts;
    }

    private long computeBatchFinalTime(long initialTime){
        return initialTime + TIMESPAN * 60; // time is in seconds
    }

    public static void main(String[] args) {
        /*
         * Usage:
         * java -jar SABDProject2-1.0.jar DataSource [dataset] [redis host] [boolean hasHeader]
         */

        String file = Variable.FILE;
        TConf config = new TConf();
        String redisUrl	= config.getString(TConf.REDIS_URL);
        int redisPort = config.getInteger(TConf.REDIS_PORT);
        Boolean hasHeader = Boolean.TRUE;

        if (args.length > 2) {
            file = args[0];
            redisUrl = args[1];
            try {
                hasHeader = Boolean.parseBoolean(args[2]);
            } catch (NumberFormatException e) {
                e.printStackTrace();
            }
        }

		DataSource fill = new DataSource(file, redisUrl, redisPort, hasHeader);
        Thread th1 = new Thread(fill);
        th1.start();
    }

}