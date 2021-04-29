package LRB;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class LRBWorkloadGenerator implements Runnable {

    // System parameters
    private final Producer<byte[], byte[]> kafkaProducer;
    private final String kafkaTopic;
    private final String fileName;
    // Experiment parameters
    private final int throughput;

    public LRBWorkloadGenerator(Producer<byte[], byte[]> kafkaProducer, String kafkaTopic, String fileName, int throughput) {
        // System parameters
        this.kafkaProducer = kafkaProducer;
        this.kafkaTopic = kafkaTopic;
        this.fileName = fileName;
        // Experiment parameters
        this.throughput = throughput;
    }

    boolean emitThroughput(BufferedReader br, Random random, long currTimeInMsec, double numOfEventsPerMS) throws IOException {
        // Transform timestamp to seconds
        long now = System.currentTimeMillis();

        if (currTimeInMsec > now) {
            try {
                Thread.sleep(currTimeInMsec - now);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        if (numOfEventsPerMS < 1) {
            if (random.nextDouble() < numOfEventsPerMS) {
                numOfEventsPerMS = 1;
            } else {
                numOfEventsPerMS = 0;
            }
        }

        int numOfEventsPerMsInt = (int) Math.ceil(numOfEventsPerMS);
        boolean ret = true;
        for (int i = 0; i < numOfEventsPerMsInt; i++) {
            String line = br.readLine();
            if (line == null) {
                ret = false;
                break;
            }
            kafkaProducer.send(new ProducerRecord<>(kafkaTopic, line.getBytes(), line.getBytes()));
        }

        return ret;
    }

    public void run() {
        System.out.println("Emitting " + throughput + " tuples per second to " + kafkaTopic);

        try {
            BufferedReader br = new BufferedReader(new FileReader(fileName));
            Random random = new Random();
            double numOfEventsPerMS = throughput / 1000.0;
            long originalTimestamp = System.currentTimeMillis(); // current time in milliseconds

            while (emitThroughput(br, random, originalTimestamp, numOfEventsPerMS)) {
                originalTimestamp += 1;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
