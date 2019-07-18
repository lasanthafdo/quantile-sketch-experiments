import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONObject;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import redis.clients.jedis.Jedis;

import java.util.*;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

public class TupleGeneratorTest {

    private final String kafkaTopic = "ad-events";
    @Rule
    public MockitoRule mockitoRule = MockitoJUnit.rule();
    @Mock
    Jedis jedis;
    @Mock
    Producer<byte[], byte[]> kafkaProducer;
    @Mock
    Random random;

    @Test
    public void testGenerateAds() {
        // SETUP
        Set<String> campaignsSet = new HashSet<>();
        for (int i = 0; i < 100; i++)
            campaignsSet.add("campaign-" + i);
        when(jedis.smembers("campaigns")).thenReturn(campaignsSet);

        // RUN
        TupleGenerator tupleGenerator = new TupleGenerator(kafkaProducer, kafkaTopic, jedis, 1000, 1000);
        tupleGenerator.generateAds();

        // VERIFY
        for (String s : campaignsSet) {
            verify(jedis, times(10)).sadd(eq(s), any(String.class));
        }
    }

    @Test
    public void testSubmitToKafkaWithWatermark() {
        // SETUP
        long currTime = System.currentTimeMillis();

        // RUN
        TupleGenerator tupleGenerator = new TupleGenerator(kafkaProducer, kafkaTopic, jedis, 1000, 1);
        JSONObject jsonObject = tupleGenerator.createKafkaEvent("x", "y", "z", "l", "m", currTime, true);

        // VERIFY
        assert jsonObject.getLong("watermark_time") + 5 * 1000 == currTime;
    }


    @Test
    public void testSubmitToKafkaWithNoWatermark() {
        // SETUP
        long currTime = System.currentTimeMillis();

        // RUN
        TupleGenerator tupleGenerator = new TupleGenerator(kafkaProducer, kafkaTopic, jedis, 1000, 1);
        JSONObject jsonObject = tupleGenerator.createKafkaEvent("x", "y", "z", "l", "m", currTime, false);

        // VERIFY
        assert jsonObject.getLong("watermark_time") == 0;
    }

    @Test
    public void testEmitThroughputWithFixedRandom() {
        // SETUP
        long currTimeInMS = System.currentTimeMillis();
        List<String> adIds = new ArrayList<>(100);
        List<String> userIds = new ArrayList<>(100);
        List<String> pageIds = new ArrayList<>(100);
        for (int i = 0; i < 100; i++) {
            adIds.add("ad-id-" + i);
            userIds.add("user-id-" + i);
            pageIds.add("page-id-" + i);
        }
        when(random.nextInt(eq(100))).thenReturn(3);
        when(random.nextInt(eq(1000))).thenReturn(300);

        // RUN
        TupleGenerator tupleGenerator = new TupleGenerator(kafkaProducer, kafkaTopic, jedis, 1000, 100);
        tupleGenerator.emitThroughput(adIds, userIds, pageIds, random, currTimeInMS, 1000, 100);

        // VERIFY
        ArgumentCaptor<ProducerRecord> argumentCaptor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafkaProducer, times(1000)).send(argumentCaptor.capture());

        List<ProducerRecord> producerRecords = argumentCaptor.getAllValues();
        for (int i = 0; i < producerRecords.size(); i++) {
            byte[] jsonBytes = (byte[]) producerRecords.get(i).key();

            JSONObject jsonObject = new JSONObject(new String(jsonBytes));
            assert jsonObject.getString("user_id").equalsIgnoreCase("user-id-3");
            assert jsonObject.getString("ad_id").equalsIgnoreCase("ad-id-3");
            assert jsonObject.getString("page_id").equalsIgnoreCase("page-id-3");

            long eventTime = jsonObject.getLong("event_time");
            if (i != 0 && i % 100 == 0) {
                assert jsonObject.getLong("watermark_time") == eventTime - 5 * 1000;
            } else {
                assert jsonObject.getLong("watermark_time") == 0;
            }
            assert eventTime == currTimeInMS - 3;
        }
    }

    @Test
    public void testEmitThroughputWithoutFixedRandom() {
        // SETUP
        long currTimeInMS = System.currentTimeMillis();
        List<String> adIds = new ArrayList<>(100);
        List<String> userIds = new ArrayList<>(100);
        List<String> pageIds = new ArrayList<>(100);
        for (int i = 0; i < 100; i++) {
            adIds.add("ad-id-" + i);
            userIds.add("user-id-" + i);
            pageIds.add("page-id-" + i);
        }

        // RUN
        TupleGenerator tupleGenerator = new TupleGenerator(kafkaProducer, kafkaTopic, jedis, 1000, 100);
        long ret = tupleGenerator.emitThroughput(adIds, userIds, pageIds, new Random(), currTimeInMS, 1000, 100);

        // VERIFY
        assert ret == 0;
        ArgumentCaptor<ProducerRecord> argumentCaptor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafkaProducer, times(1000)).send(argumentCaptor.capture());

        List<ProducerRecord> producerRecords = argumentCaptor.getAllValues();
        for (int i = 0; i < producerRecords.size(); i++) {
            byte[] jsonBytes = (byte[]) producerRecords.get(i).key();

            JSONObject jsonObject = new JSONObject(new String(jsonBytes));

            long eventTime = jsonObject.getLong("event_time");
            if (i != 0 && i % 100 == 0) {
                assert jsonObject.getLong("watermark_time") == eventTime - 5 * 1000;
            } else {
                assert jsonObject.getLong("watermark_time") == 0;
            }
            assert eventTime <= currTimeInMS;
            assert eventTime >= currTimeInMS - 100;
        }
    }
}

