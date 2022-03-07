import LRB.LRBWorkloadGenerator;
import NYT.NYTWorkloadGenerator;
import Power.PowerWorkloadGenerator;
import Synthetic.SyntheticParetoWorkloadGenerator;
import Synthetic.SyntheticUniformWorkloadGenerator;
import YSB.YSBWorkloadGenerator;
import YSB.YSBWorkloadSetup;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import redis.clients.jedis.Jedis;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import static LRB.LRBConstants.LRB_KAFKA_TOPIC_PREFIX;
import static NYT.NYTConstants.NYT_KAFKA_TOPIC_PREFIX;
import static Power.PowerConstants.POWER_KAFKA_TOPIC_PREFIX;
import static YSB.YSBConstants.KAFKA_TOPIC_PREFIX;

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

        /* Identify execution mode */
        boolean setupExecution = false;
        for (String arg : args) {
            if (arg.equalsIgnoreCase("-n")) {
                setupExecution = true;
            } else if (arg.equalsIgnoreCase("-r")) {
                setupExecution = false;
            }
        }

        /* Identify workload type */
        String workloadType = (String) benchMap.get("workload_type");

        /* Execute! */
        if (workloadType.equalsIgnoreCase("ysb")) {
            if (setupExecution) {
                runYSBWorkloadSetup(setupMap);
            } else {
                runYSBWorkloadGenerator(setupMap, benchMap);
            }
        } else if (workloadType.equalsIgnoreCase("lrb")) {
            runLRBWorkloadGenerator(setupMap, benchMap);
        } else if (workloadType.equalsIgnoreCase("nyt")) {
            runNYTWorkloadGenerator(setupMap, benchMap);
        } else if (workloadType.equalsIgnoreCase("syn") || workloadType.equalsIgnoreCase("synp")) {
            runSyntheticParetoWorkloadGenerator(setupMap, benchMap);
        } else if (workloadType.equalsIgnoreCase("synu")) {
            runSyntheticUniformWorkloadGenerator(setupMap, benchMap);
        } else if (workloadType.equalsIgnoreCase("power")) {
            runPowerWorkloadGenerator(setupMap, benchMap);
        }
    }

    private static void runYSBWorkloadSetup(Map setupMap) {
        String jedisServerName = (String) setupMap.get("redis.host");
        new YSBWorkloadSetup(new Jedis(jedisServerName)).run();
    }

    private static void runYSBWorkloadGenerator(Map setupMap, Map benchMap) {
        /* Create Kafka Producer */
        Properties props = new Properties();
        props.put("bootstrap.servers", Utils.getKafkaBrokers(setupMap));

        Producer<byte[], byte[]> kafkaProducer =
            new KafkaProducer<>(props,
                new ByteArraySerializer(),
                new ByteArraySerializer());

        /* Create Redis instance */
        String jedisServerName = (String) setupMap.get("redis.host");
        Jedis jedis = new Jedis(jedisServerName);

        /* Get Benchmark properties */
        int numOfInstances = (Integer) benchMap.get("num_instances");
        int throughput = (Integer) benchMap.get("throughput");

        for (int i = 1; i <= numOfInstances; i++) {
            try {
                new Thread(
                    new YSBWorkloadGenerator(
                        kafkaProducer, KAFKA_TOPIC_PREFIX + "-" + i, jedis, throughput))
                    .start();
                Thread.sleep(2000);
            } catch (Exception e) {
                e.printStackTrace();
            }
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
        boolean missingData = (boolean) setupMap.get("missing.data");
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
        boolean missingData = (boolean) setupMap.get("missing.data");
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
        boolean missingData = (boolean) setupMap.get("missing.data");
        new Thread(new SyntheticParetoWorkloadGenerator(kafkaProducer, "synp-events", throughput, missingData))
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
        boolean missingData = (boolean) setupMap.get("missing.data");
        new Thread(new SyntheticUniformWorkloadGenerator(kafkaProducer, "synu-events", throughput, missingData))
            .start();
    }

    private static void runLRBWorkloadGenerator(Map setupMap, Map benchMap) {
        /* Create Kafka Producer */
        Properties props = new Properties();
        props.put("bootstrap.servers", Utils.getKafkaBrokers(setupMap));

        Producer<byte[], byte[]> kafkaProducer =
            new KafkaProducer<>(props,
                new ByteArraySerializer(),
                new ByteArraySerializer());

        /* Create Redis instance */
        String jedisServerName = (String) setupMap.get("redis.host");
        Jedis jedis = new Jedis(jedisServerName);

        /* Get Benchmark properties */
        int numOfInstances = (Integer) benchMap.get("num_instances");
        int throughput = (Integer) benchMap.get("throughput");
        int watermarkFrequency = (Integer) benchMap.get("watermark_frequency");

        try {
            new Thread(
                new LRBWorkloadGenerator(
                    kafkaProducer, LRB_KAFKA_TOPIC_PREFIX + "-" + 1, "car.dat", throughput))
                .start();
            Thread.sleep(2000);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
