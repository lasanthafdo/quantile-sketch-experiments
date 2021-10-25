package Synthetic;

import org.apache.commons.math3.distribution.*;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONObject;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.concurrent.ThreadLocalRandom;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class SyntheticWorkloadGenerator implements Runnable {

    // System parameters
    private final Producer<byte[], byte[]> kafkaProducer;
    private final String kafkaTopic;
    // Experiment parameters
    private final int throughput;

    NormalDistribution nD = new NormalDistribution(100.0, 15.0);

    // Network Delays
    ExponentialDistribution eD = new ExponentialDistribution(250);
    PoissonDistribution pD = new PoissonDistribution(250);
    GammaDistribution gD = new GammaDistribution(60, 4);
    ZipfDistribution zD = new ZipfDistribution(100, 0.2);
    BinomialDistribution bD = new BinomialDistribution(20, 0.5);

    // Values
    ParetoDistribution ptoD = new ParetoDistribution(1, 1);
    UniformRealDistribution uD = new UniformRealDistribution(1, 10000);
    NormalDistribution valnD = new NormalDistribution(100, 15);

    public SyntheticWorkloadGenerator(Producer<byte[], byte[]> kafkaProducer, String kafkaTopic, int throughput) {
        // System parameters
        this.kafkaProducer = kafkaProducer;
        this.kafkaTopic = kafkaTopic;
        // Experiment parameters
        this.throughput = throughput;
        System.out.println("Pareto Mean: " + pD.getMean());
        System.out.println("Uniform Mean: " + uD.getNumericalMean());
        System.out.println("Normal Mean: " +valnD.getMean());
    }

    boolean emitThroughput(Random random, long currTimeInMsec, double numOfEventsPerMS) {
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
            double b = round(ptoD.sample(), 3);
            double b2 = round(uD.sample(), 3);
            double b3 = round(valnD.sample(), 3);

            Map<String, String> eventMap = new HashMap<>();

            eventMap.put("pareto_value", String.valueOf(b));
            eventMap.put("uniform_value", String.valueOf(b2));
            eventMap.put("normal_value", String.valueOf(b3));
            // Network Delay
            int randomNum = ThreadLocalRandom.current().nextInt(0, 150);
            //int sampled_value = (int) nD.sample();
            //int sampled_value = (int) eD.sample();
            //int sampled_value = (int) zD.sample();
            //int sampled_value = (int) pD.sample();
            //int sampled_value = (int) gD.sample();
            int sampled_value = bD.sample();
            long eventTime = currTimeInMsec - sampled_value; //- randomNum; // Normal Distribution Lateness, mean 100msec, sd: 20ms
            eventMap.put("event_time", Long.toString(eventTime));
            String kafkaOutput = new JSONObject(eventMap).toString();
            kafkaProducer.send(new ProducerRecord<>(kafkaTopic, kafkaOutput.getBytes(), kafkaOutput.getBytes()));
        }

        return ret;
    }

    private double round(double value, int places) {
        if (places < 0) throw new IllegalArgumentException();

        BigDecimal bd = new BigDecimal(Double.toString(value));
        bd = bd.setScale(places, RoundingMode.HALF_UP);
        return bd.doubleValue();
    }

    public void run() {
        System.out.println("Emitting " + throughput + " tuples per second to " + kafkaTopic);

        Random random = new Random();
        double numOfEventsPerMS = throughput / 1000.0;
        long originalTimestamp = System.currentTimeMillis(); // current time in milliseconds

        while (emitThroughput(random, originalTimestamp, numOfEventsPerMS)) {
            originalTimestamp += 1;
        }
    }
}

