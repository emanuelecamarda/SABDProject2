import com.google.gson.Gson;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;
import utils.LinesBatch;
import utils.TConf;
import utils.Variable;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class Test implements Runnable{

    Jedis jedis;
    Gson gson;

    public Test() {
        this.jedis = new Jedis("172.18.0.10", 6379, 60000);
        this.gson = new Gson();
    }

    @Override
    public void run() {

        LinesBatch linesBatch = new LinesBatch();
        linesBatch.addLine(",prova1,,,comment,1525132800,,,");
        send(linesBatch);
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        linesBatch = new LinesBatch();
        linesBatch.addLine(",prova2,,,comment,1530403200,,,");
        send(linesBatch);
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        linesBatch = new LinesBatch();
        linesBatch.addLine(",prova3,,,comment,1535760000,,,");
        send(linesBatch);
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        linesBatch = new LinesBatch();
        linesBatch.addLine(",prova4,,,comment,1541030400,,,");
        send(linesBatch);
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        linesBatch = new LinesBatch();
        linesBatch.addLine(",prova5,,,comment,1546030400,,,");
        send(linesBatch);
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        linesBatch = new LinesBatch();
        linesBatch.addLine(",prova6,,,comment,1551030400,,,");
        send(linesBatch);
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        linesBatch = new LinesBatch();
        linesBatch.addLine(",prova7,,,comment,1556030400,,,");
        send(linesBatch);
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        linesBatch = new LinesBatch();
        linesBatch.addLine(",prova8,,,comment,1561030400,,,");
        send(linesBatch);



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

    public static void main(String[] args) {

        Test fill = new Test();
        Thread th1 = new Thread(fill);
        th1.start();
    }

}
