package YSB;

import org.apache.commons.math3.distribution.ExponentialDistribution;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.distribution.PoissonDistribution;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONObject;
import redis.clients.jedis.Jedis;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

import static YSB.YSBConstants.NUM_CAMPAIGNS;

public class YSBWorkloadGenerator implements Runnable {

    private static final String[] AD_TYPES = new String[]{"banner", "modal", "sponsored-search", "mail", "mobile"};
    private static final String[] EVENT_TYPES = new String[]{"view", "click", "purchase"};

    // System parameters
    private final Producer<byte[], byte[]> kafkaProducer;
    private final String kafkaTopic;
    private final Jedis jedis;
    // Experiment parameters
    private final int throughput;


    public YSBWorkloadGenerator(Producer<byte[], byte[]> kafkaProducer, String kafkaTopic, Jedis jedis, int throughput) {
        // System parameters
        this.kafkaProducer = kafkaProducer;
        this.kafkaTopic = kafkaTopic;
        this.jedis = jedis;
        // Experiment parameters
        this.throughput = throughput;
    }

    List<String> generateAds() {
        List<String> campaigns = new ArrayList<>(jedis.smembers("campaigns"));
        if (campaigns.size() != NUM_CAMPAIGNS) {
            System.err.println("Setup not generated. Please rerun the program with -n option");
            System.exit(-1);
        }
        List<String> ads = YSBUtils.generateIds(NUM_CAMPAIGNS * 10);
        for (int i = 0; i < ads.size(); i++) {
            int campaignIndex = i / 10;
            synchronized (jedis) {
                //jedis.sadd(campaigns.get(campaignIndex), ads.get(i));
                jedis.set(ads.get(i), campaigns.get(campaignIndex));
            }
        }
        return ads;
    }

    JSONObject createKafkaEvent(String userId, String pageId, String adId, String adType, String eventType, long eventTimeInMsec) {
        Map<String, String> eventMap = new HashMap<>();
        eventMap.put("user_id", userId);
        eventMap.put("page_id", pageId);
        eventMap.put("ad_id", adId);
        eventMap.put("ad_type", adType);
        eventMap.put("event_type", eventType);
        eventMap.put("event_time", String.valueOf(eventTimeInMsec));
        eventMap.put("ip_address", "1.2.3.4");
        return new JSONObject(eventMap);
    }

    long emitThroughput(List<String> adIds, List<String> userIds, List<String> pageIds, Random random, long currTimeInMsec, double numOfEventsPerMS) {
        // Transform timestamp to seconds
        long now = System.currentTimeMillis();

        if (currTimeInMsec > now) {
            try {
                Thread.sleep(currTimeInMsec - now);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        //TODO add network and inter-event generation delay here in Milliseconds
        NormalDistribution normalDistribution = new NormalDistribution(100.0, 50.0);
        ExponentialDistribution ed = new ExponentialDistribution(100);
        PoissonDistribution pd = new PoissonDistribution(100);

        if (numOfEventsPerMS < 1) {
            if (random.nextDouble() < numOfEventsPerMS) {
                numOfEventsPerMS = 1;
            } else {
                numOfEventsPerMS = 0;
            }
        }


        int numOfEventsPerMsInt = (int) Math.ceil(numOfEventsPerMS);
        for (int i = 0; i < numOfEventsPerMsInt; i++) {

            // create Kafka Event
            String userId = userIds.get(random.nextInt(userIds.size()));
            String pageId = pageIds.get(random.nextInt(pageIds.size()));
            String adId = adIds.get(random.nextInt(adIds.size()));
            String adType = AD_TYPES[random.nextInt(AD_TYPES.length)];
            String eventType = EVENT_TYPES[random.nextInt(EVENT_TYPES.length)];
            // Network Delay
            int randomNum = ThreadLocalRandom.current().nextInt(0, 150);
            //int sampled_value = (int) normalDistribution.sample();
            //int sampled_value = (int) ed.sample();
            int sampled_value = (int) pd.sample();
            long eventTime = currTimeInMsec - sampled_value; //- randomNum; // Normal Distribution Lateness, mean 100msec, sd: 20ms
            JSONObject kafkaEvent = createKafkaEvent(userId, pageId, adId, adType, eventType, eventTime);

            kafkaProducer.send(new ProducerRecord<>(kafkaTopic, kafkaEvent.toString().getBytes(), kafkaEvent.toString().getBytes()));

        }

        return 1;
    }

    public void run() {
        System.out.println("Emitting " + throughput + " tuples per second to " + kafkaTopic);

        List<String> adIds = generateAds();
        List<String> userIds = YSBUtils.generateIds(NUM_CAMPAIGNS);
        List<String> pageIds = YSBUtils.generateIds(NUM_CAMPAIGNS);
        Random random = new Random();

        double numOfEventsPerMS = throughput / 1000.0;

        long originalTimestamp = System.currentTimeMillis(); // current time in milliseconds

        while (true) {
            originalTimestamp += 1;
            emitThroughput(adIds, userIds, pageIds, random, originalTimestamp, numOfEventsPerMS);
        }
    }
}
