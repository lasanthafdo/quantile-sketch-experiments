package Power;

import NYT.NYTConstants;
import org.apache.commons.math3.distribution.*;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.commons.math3.distribution.ExponentialDistribution;
import org.apache.commons.math3.distribution.GammaDistribution;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.distribution.PoissonDistribution;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.json.JsonObjectDecoder;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import NYT.NYTConstants;
import scala.util.parsing.json.JSON;

public class PowerWorkloadGenerator implements Runnable {

    // System parameters
    private final Producer<byte[], byte[]> kafkaProducer;
    private final String kafkaTopic;
    private final String fileName;
    // Experiment parameters
    private final int throughput;
    ExponentialDistribution eD = new ExponentialDistribution(150);
    PoissonDistribution pD = new PoissonDistribution(250);
    GammaDistribution gD = new GammaDistribution(60, 4);

    NormalDistribution nD = new NormalDistribution(150.0, 15.0);
    NormalDistribution paretoNormal = new NormalDistribution(1, 0.05);
    NormalDistribution uniformNormal = new NormalDistribution(150, 25);
    NormalDistribution uniformNormal2 = new NormalDistribution(1000, 50);

    NormalDistribution randomizedNormalMean = new NormalDistribution(150, 20);
    NormalDistribution randomizedNormalStd = new NormalDistribution(20, 4);

    // Values
    ParetoDistribution ptoD = new ParetoDistribution(1, 1);
    UniformRealDistribution uD = new UniformRealDistribution(50, 10000);
    NormalDistribution valnD = new NormalDistribution(100, 15);

    public PowerWorkloadGenerator(Producer<byte[], byte[]> kafkaProducer, String kafkaTopic, String fileName, int throughput) {
        // System parameters
        this.kafkaProducer = kafkaProducer;
        this.kafkaTopic = kafkaTopic;
        this.fileName = fileName;
        // Experiment parameters
        this.throughput = throughput;
        System.out.println("NetworkDelayExponential Mean:" + Double.toString(eD.getMean()));
        System.out.println("NetworkDelayNormal Mean:" + Double.toString(nD.getMean()));
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
        //TODO inter-event generation delay here in Milliseconds

        int numOfEventsPerMsInt = (int) Math.ceil(numOfEventsPerMS);
        boolean ret = true;
        for (int i = 0; i < numOfEventsPerMsInt; i++) {
            String line = br.readLine();
            if (line == null) {
                ret = false;
                break;
            }
            String[] a =  line.split(";");
            Map<String, String> eventMap = new HashMap<>();

            eventMap.put(PowerConstants.HEADERS.get(2), a[2]); // "Global_active_power" is 3rd slot
            // Network Delay
            int randomNum = ThreadLocalRandom.current().nextInt(0, 150);
            //int sampled_value = (int) nD.sample();
            int sampled_value = (int) eD.sample();
            //int sampled_value = (int) pD.sample();
            //int sampled_value = (int) gD.sample();
            long eventTime = currTimeInMsec; //  - sampled_value; //- randomNum; // Normal Distribution Lateness, mean 100msec, sd: 20ms
            eventMap.put("event_time", Long.toString(eventTime));
            String kafkaOutput = new JSONObject(eventMap).toString();
            kafkaProducer.send(new ProducerRecord<>(kafkaTopic, kafkaOutput.getBytes(), kafkaOutput.getBytes()));
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
