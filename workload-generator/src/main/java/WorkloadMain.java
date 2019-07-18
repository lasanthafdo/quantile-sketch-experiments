import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

public class WorkloadMain {

    public static void main(String[] args) {
        String setupFile = null;
        String expFile = null;

        /* Extract input */
        for (int i = 0; i < args.length; i++) {
            if (args[i].equalsIgnoreCase("-s")) {
                setupFile = args[++i];
            } else if (args[i].equalsIgnoreCase("-e")) {
                expFile = args[++i];
            }
        }

        if (setupFile == null || expFile == null) {
            System.err.println("Missing -s and -e");
            System.exit(-1);
        }

        Map setupMap = null;
        Map benchMap = null;

        try {
            setupMap = Utils.yamlToMap(setupFile);
            benchMap = Utils.yamlToMap(expFile);
        } catch (IOException e) {
            System.err.println("Invalid conf files. You must use -s and -e correctly.");
            System.exit(-1);
        }

        /* Execution Mode */
        for (String arg : args) {
            if (arg.equalsIgnoreCase("-n")) {
                createNewSetupGeneratorInstance(setupMap);
            } else if (arg.equalsIgnoreCase("-r")) {
                createTupleGeneratorInstance(setupMap, benchMap);
            }
        }
    }

    private static void createNewSetupGeneratorInstance(Map setupMap) {
        String jedisServerName = (String) setupMap.get("redis.host");
        new NewSetupGenerator(new Jedis(jedisServerName)).setupWorkload();
    }

    private static void createTupleGeneratorInstance(Map setupMap, Map benchMap) {
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

        for (int i = 1; i <= numOfInstances; i++) {
            try {
                TupleGenerator tg =
                        new TupleGenerator(kafkaProducer, "ad-events-" + i, jedis, throughput, watermarkFrequency);
                new Thread(tg).start();
                Thread.sleep(2000);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
