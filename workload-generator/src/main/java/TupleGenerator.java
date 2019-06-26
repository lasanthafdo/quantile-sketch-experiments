import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONObject;
import redis.clients.jedis.Jedis;

import java.util.*;

public class TupleGenerator implements Runnable {

    private static final String[] AD_TYPES = new String[]{"banner", "modal", "sponsored-search", "mail", "mobile"};
    private static final String[] EVENT_TYPES = new String[]{"view", "click", "purchase"};

    // System parameters
    private final Producer<byte[], byte[]> kafkaProducer;
    private final String kafkaTopic;
    private final Jedis jedis;
    // benchmark parameters
    private final int throughput;
    private final int watermarkFrequency;


    TupleGenerator(Producer<byte[], byte[]> kafkaProducer, String kafkaTopic, Jedis jedis, int throughput, int watermarkFrequency) {
        this.kafkaProducer = kafkaProducer;
        this.kafkaTopic = kafkaTopic;
        this.jedis = jedis;

        this.throughput = throughput;
        this.watermarkFrequency = watermarkFrequency;
    }

    List<String> generateAds() {
        List<String> campaigns = new ArrayList<>(jedis.smembers("campaigns"));
        if (campaigns.size() != NewSetupGenerator.NUM_CAMPAIGNS) {
            System.err.println("Setup not generated. Please rerun the program with -n option");
            System.exit(-1);
        }
        List<String> ads = Utils.generateIds(NewSetupGenerator.NUM_CAMPAIGNS * 10);
        for (int i = 0; i < ads.size(); i++) {
            int campaignIndex = i / 10;
            synchronized (jedis) {
                jedis.sadd(campaigns.get(campaignIndex), ads.get(i));
            }
        }
        return ads;
    }

    JSONObject createKafkaEvent(String userId, String pageId, String adId, String adType, String eventType, long eventTimeInMsec, boolean hasWatermark) {
        Map<String, String> eventMap = new HashMap<>();
        eventMap.put("user_id", userId);
        eventMap.put("page_id", pageId);
        eventMap.put("ad_id", adId);
        eventMap.put("ad_type", adType);
        eventMap.put("event_type", eventType);
        eventMap.put("event_time", String.valueOf(eventTimeInMsec));
        if (hasWatermark) {
            long watermarkTime = eventTimeInMsec - (5 * 1000); // 5 seconds before
            eventMap.put("watermark_time", String.valueOf(watermarkTime));
        } else {
            eventMap.put("watermark_time", String.valueOf(0));
        }

        eventMap.put("ip_address", "1.2.3.4");
        return new JSONObject(eventMap);
    }

    long emitThroughput(List<String> adIds, List<String> userIds, List<String> pageIds, Random random, long currTimeInMsec, double numOfEventsPerMS, long tuplesUntilWatermark) {
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
        for (int i = 0; i < numOfEventsPerMsInt; i++) {
            String userId = userIds.get(random.nextInt(userIds.size()));
            String pageId = pageIds.get(random.nextInt(pageIds.size()));
            String adId = adIds.get(random.nextInt(adIds.size()));
            String adType = AD_TYPES[random.nextInt(AD_TYPES.length)];
            String eventType = EVENT_TYPES[random.nextInt(EVENT_TYPES.length)];
            long eventTime = currTimeInMsec - random.nextInt(100); // Lateness of 100msec
            boolean hasWatermark = false;
            if (tuplesUntilWatermark == 0) {
                hasWatermark = true;
                tuplesUntilWatermark = watermarkFrequency;
            }
            JSONObject kafkaEvent = createKafkaEvent(userId, pageId, adId, adType, eventType, eventTime, hasWatermark);
            kafkaProducer.send(new ProducerRecord<>(kafkaTopic, kafkaEvent.toString().getBytes(), kafkaEvent.toString().getBytes()));
            tuplesUntilWatermark--;
        }

        return tuplesUntilWatermark;
    }

    public void run() {
        System.out.println("Emitting " + throughput + " tuples per second to " + kafkaTopic);

        List<String> adIds = generateAds();
        List<String> userIds = Utils.generateIds(NewSetupGenerator.NUM_CAMPAIGNS);
        List<String> pageIds = Utils.generateIds(NewSetupGenerator.NUM_CAMPAIGNS);
        Random random = new Random();

        double numOfEventsPerMS = throughput / 1000.0;

        long originalTimestamp = System.currentTimeMillis(); // current time in milliseconds
        long tuplesUntilWatermark = watermarkFrequency;

        while (true) {
            originalTimestamp += 1;
            tuplesUntilWatermark = emitThroughput(adIds, userIds, pageIds, random, originalTimestamp, numOfEventsPerMS, tuplesUntilWatermark);
        }
    }
}
