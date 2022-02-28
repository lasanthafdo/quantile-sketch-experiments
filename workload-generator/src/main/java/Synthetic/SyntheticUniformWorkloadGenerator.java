package Synthetic;

import org.apache.commons.math3.distribution.BinomialDistribution;
import org.apache.commons.math3.distribution.ExponentialDistribution;
import org.apache.commons.math3.distribution.GammaDistribution;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.distribution.ParetoDistribution;
import org.apache.commons.math3.distribution.PoissonDistribution;
import org.apache.commons.math3.distribution.UniformRealDistribution;
import org.apache.commons.math3.distribution.ZipfDistribution;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONObject;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public class SyntheticUniformWorkloadGenerator implements Runnable {

    // System parameters
    private final Producer<byte[], byte[]> kafkaProducer;
    private final String kafkaTopic;
    // Experiment parameters
    private final int throughput;

    NormalDistribution nD = new NormalDistribution(150.0, 15.0);
    NormalDistribution paretoNormal = new NormalDistribution(1, 0.05);
    NormalDistribution uniformNormal = new NormalDistribution(100, 25);
    NormalDistribution uniformNormal2 = new NormalDistribution(1000, 100);

    NormalDistribution randomizedNormalMean = new NormalDistribution(150, 20);
    NormalDistribution randomizedNormalStd = new NormalDistribution(20, 4);

    // Network Delays
    ExponentialDistribution eD = new ExponentialDistribution(150);
    PoissonDistribution pD = new PoissonDistribution(250);
    GammaDistribution gD = new GammaDistribution(60, 4);
    ZipfDistribution zD = new ZipfDistribution(100, 0.2);
    BinomialDistribution bD = new BinomialDistribution(20, 0.5);

    // Values
    ParetoDistribution ptoD = new ParetoDistribution(1, 1);
    UniformRealDistribution uD = new UniformRealDistribution(1, 1000);
    NormalDistribution valnD = new NormalDistribution(100, 15);

    public SyntheticUniformWorkloadGenerator(Producer<byte[], byte[]> kafkaProducer, String kafkaTopic,
                                             int throughput) {
        // System parameters
        this.kafkaProducer = kafkaProducer;
        this.kafkaTopic = kafkaTopic;
        // Experiment parameters
        this.throughput = throughput;
        System.out.println("Uniform Mean: " + uD.getNumericalMean());
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

        double shapeParam = paretoNormal.sample();
        while (shapeParam < 0.01) {
            shapeParam = paretoNormal.sample();
        }

        ptoD = new ParetoDistribution(shapeParam, shapeParam);

        double std = randomizedNormalStd.sample();
        while (std < 0) {
            std = randomizedNormalStd.sample();
        }

        uD = new UniformRealDistribution(uniformNormal.sample(), uniformNormal2.sample());

        valnD = new NormalDistribution(randomizedNormalMean.sample(), std);

        int numOfEventsPerMsInt = (int) Math.ceil(numOfEventsPerMS);
        boolean ret = true;
        for (int i = 0; i < numOfEventsPerMsInt; i++) {

            Map<String, String> eventMap = new HashMap<>();

            eventMap.put("uniform_value", String.valueOf(round(uD.sample(), 4)));
            // Network Delay
            int randomNum = ThreadLocalRandom.current().nextInt(0, 150);
            //int sampled_value = (int) nD.sample();
            //int sampled_value = (int) eD.sample();
            //int sampled_value = (int) zD.sample();
            //int sampled_value = (int) pD.sample();
            //int sampled_value = (int) gD.sample();
            int sampled_value = (int) eD.sample();
            long eventTime =
                currTimeInMsec; // - sampled_value; //- randomNum; // Normal Distribution Lateness, mean 100msec, sd: 20ms
            eventMap.put("event_time", Long.toString(eventTime));
            String kafkaOutput = new JSONObject(eventMap).toString();
            kafkaProducer.send(new ProducerRecord<>(kafkaTopic, kafkaOutput.getBytes(), kafkaOutput.getBytes()));
        }

        return ret;
    }

    private double round(double value, int places) {
        if (places < 0) {
            throw new IllegalArgumentException();
        }

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

