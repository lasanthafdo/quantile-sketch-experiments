package org.uwaterloo.streaming.benchmarks.workload.generator;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.uwaterloo.streaming.benchmarks.workload.generator.nyt.NYTWorkloadGenerator;
import org.uwaterloo.streaming.benchmarks.workload.generator.power.PowerWorkloadGenerator;
import org.uwaterloo.streaming.benchmarks.workload.generator.synthetic.SyntheticParetoWorkloadGenerator;
import org.uwaterloo.streaming.benchmarks.workload.generator.synthetic.SyntheticUniformWorkloadGenerator;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import static org.uwaterloo.streaming.benchmarks.workload.generator.nyt.NYTConstants.NYT_KAFKA_TOPIC_PREFIX;
import static org.uwaterloo.streaming.benchmarks.workload.generator.power.PowerConstants.POWER_KAFKA_TOPIC_PREFIX;
import static org.uwaterloo.streaming.benchmarks.workload.generator.synthetic.SyntheticConstants.SYN_PARETO_KAFKA_TOPIC_PREFIX;
import static org.uwaterloo.streaming.benchmarks.workload.generator.synthetic.SyntheticConstants.SYN_UNIFORM_KAFKA_TOPIC_PREFIX;

public class WorkloadGeneratorEntryPoint {

    public static void main(String[] args) {
        String setupFile = null;
        String expFile = null;

        /* Extract input: setup and experiment files */
        for (int i = 0; i < args.length; i++) {
            if (args[i].equalsIgnoreCase("-s")) {
                setupFile = args[++i];
            } else if (args[i].equalsIgnoreCase("-e")) {
                expFile = args[++i];
            }
        }

        /* WorkloadGenerator require both files. Exit if they were not provided */
        if (setupFile == null) {
            System.err.println("Missing setup file (-s)");
            System.exit(-1);
        } else if (expFile == null) {
            System.err.println("Missing experiment file (-e)");
            System.exit(-1);
        }

        /* Transform configuration files to maps */
        Map setupMap = null;
        Map benchMap = null;

        try {
            setupMap = Utils.yamlToMap(setupFile);
            benchMap = Utils.yamlToMap(expFile);
        } catch (IOException e) {
            System.err.println("Invalid setup or experiment conf files. You must use -s and -e correctly.");
            System.exit(-1);
        }

        /* Identify workload type */
        String workloadType = (String) benchMap.get("workload_type");

        /* Execute! */
        if (workloadType.equalsIgnoreCase("nyt")) {
            runNYTWorkloadGenerator(setupMap, benchMap);
        } else if (workloadType.equalsIgnoreCase("syn") || workloadType.equalsIgnoreCase("synp")) {
            runSyntheticParetoWorkloadGenerator(setupMap, benchMap);
        } else if (workloadType.equalsIgnoreCase("synu")) {
            runSyntheticUniformWorkloadGenerator(setupMap, benchMap);
        } else if (workloadType.equalsIgnoreCase("power")) {
            runPowerWorkloadGenerator(setupMap, benchMap);
        }
    }

    private static void runNYTWorkloadGenerator(Map setupMap, Map benchMap) {
        /* Create Kafka Producer */
        Properties props = new Properties();
        props.put("bootstrap.servers", Utils.getKafkaBrokers(setupMap));

        Producer<byte[], byte[]> kafkaProducer =
            new KafkaProducer<>(props,
                new ByteArraySerializer(),
                new ByteArraySerializer());

        /* Get Benchmark properties */
        int throughput = (Integer) benchMap.get("throughput");
        boolean missingData = (boolean) benchMap.get("missing_data");
        String currentUsersHomeDir = System.getProperty("user.home");
        String dataFile = currentUsersHomeDir + File.separator + "flink-benchmarks" + File.separator + "nyt-data.csv";
        System.out.println(dataFile);

        new Thread(new NYTWorkloadGenerator(kafkaProducer, NYT_KAFKA_TOPIC_PREFIX, dataFile, throughput, missingData))
            .start();
    }

    private static void runPowerWorkloadGenerator(Map setupMap, Map benchMap) {
        /* Create Kafka Producer */
        Properties props = new Properties();
        props.put("bootstrap.servers", Utils.getKafkaBrokers(setupMap));

        Producer<byte[], byte[]> kafkaProducer =
            new KafkaProducer<>(props,
                new ByteArraySerializer(),
                new ByteArraySerializer());

        /* Get Benchmark properties */
        int throughput = (Integer) benchMap.get("throughput");
        boolean missingData = (boolean) benchMap.get("missing_data");
        String currentUsersHomeDir = System.getProperty("user.home");
        String dataFile = currentUsersHomeDir + File.separator + "flink-benchmarks" + File.separator +
            "household_power_consumption.txt";
        System.out.println(dataFile);

        new Thread(
            new PowerWorkloadGenerator(kafkaProducer, POWER_KAFKA_TOPIC_PREFIX, dataFile, throughput, missingData))
            .start();
    }

    private static void runSyntheticParetoWorkloadGenerator(Map setupMap, Map benchMap) {
        /* Create Kafka Producer */
        Properties props = new Properties();
        props.put("bootstrap.servers", Utils.getKafkaBrokers(setupMap));

        Producer<byte[], byte[]> kafkaProducer =
            new KafkaProducer<>(props,
                new ByteArraySerializer(),
                new ByteArraySerializer());

        /* Get Benchmark properties */
        int throughput = (Integer) benchMap.get("throughput");
        boolean missingData = (boolean) benchMap.get("missing_data");
        new Thread(new SyntheticParetoWorkloadGenerator(kafkaProducer, SYN_PARETO_KAFKA_TOPIC_PREFIX, throughput, missingData))
            .start();
    }

    private static void runSyntheticUniformWorkloadGenerator(Map setupMap, Map benchMap) {
        /* Create Kafka Producer */
        Properties props = new Properties();
        props.put("bootstrap.servers", Utils.getKafkaBrokers(setupMap));

        Producer<byte[], byte[]> kafkaProducer =
            new KafkaProducer<>(props,
                new ByteArraySerializer(),
                new ByteArraySerializer());

        /* Get Benchmark properties */
        int throughput = (Integer) benchMap.get("throughput");
        boolean missingData = (boolean) benchMap.get("missing_data");
        new Thread(new SyntheticUniformWorkloadGenerator(kafkaProducer, SYN_UNIFORM_KAFKA_TOPIC_PREFIX, throughput, missingData))
            .start();
    }

}
