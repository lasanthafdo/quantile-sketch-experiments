package Synthetic;

import eventtime.generator.EventTimeGenerator;
import eventtime.generator.ExponentialOffsetEventTimeGenerator;
import eventtime.generator.NoOffsetEventTimeGenerator;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.distribution.ParetoDistribution;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONObject;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static Synthetic.SyntheticConstants.HEADER_PARETO;

public class SyntheticParetoWorkloadGenerator implements Runnable {

    // System parameters
    private final Producer<byte[], byte[]> kafkaProducer;
    private final String kafkaTopic;
    // Experiment parameters
    private final int throughput;
    private final EventTimeGenerator eventTimeGenerator;

    NormalDistribution paretoNormal = new NormalDistribution(1, 0.05);

    // Values
    ParetoDistribution ptoD = new ParetoDistribution(1, 1);

    public SyntheticParetoWorkloadGenerator(Producer<byte[], byte[]> kafkaProducer, String kafkaTopic, int throughput,
                                            boolean missingData) {
        // System parameters
        this.kafkaProducer = kafkaProducer;
        this.kafkaTopic = kafkaTopic;
        // Experiment parameters
        this.throughput = throughput;
        if (missingData) {
            eventTimeGenerator = new ExponentialOffsetEventTimeGenerator();
        } else {
            eventTimeGenerator = new NoOffsetEventTimeGenerator();
        }
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

        double shapeParam = paretoNormal.sample();
        while (shapeParam < 0.01) {
            shapeParam = paretoNormal.sample();
        }
        ptoD = new ParetoDistribution(shapeParam, shapeParam);

        int numOfEventsPerMsInt = (int) Math.ceil(numOfEventsPerMS);
        boolean ret = true;
        for (int i = 0; i < numOfEventsPerMsInt; i++) {

            Map<String, String> eventMap = new HashMap<>();

            eventMap.put(HEADER_PARETO, String.valueOf(round(ptoD.sample(), 4)));
            long eventTime = eventTimeGenerator.getEventTimeMillis(currTimeInMsec);
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

