package YSB;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
//import org.apache.flink.streaming.runtime.tasks.scheduler.StreamTaskSchedulerPolicy;
import org.json.JSONObject;
import redis.clients.jedis.Jedis;

import javax.annotation.Nullable;

public class AdvertisingQuery implements Runnable {

    /* The Kafka topic the source operators are pulling the results from */
    private final String KAFKA_PREFIX_TOPIC = "ad-events";
    /* The Job Parameters */
    //This class provides simple utility methods for reading and parsing program
    // arguments from different sources.
    // Only single value parameter could be supported in args.
    private final ParameterTool setupParams;
    /* The scheduler policy */
//    private final StreamTaskSchedulerPolicy schedulerPolicy;
    /* The num of queries */
    private final int numQueries;
    /* The window size */
    private final int windowSize;

    public AdvertisingQuery(
            ParameterTool setupParams, int numQueries, int windowSize) {
        this.setupParams = setupParams;
//        this.schedulerPolicy = schedulerPolicy;
        this.numQueries = numQueries;
        this.windowSize = windowSize;
    }


    @Override
    public void run() {
        // Setup Flink
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        env.setStreamTaskSchedulerPolicy(schedulerPolicy);
        env.getConfig().setGlobalJobParameters(setupParams);

        // Add queries
        for (int i = 1; i <= numQueries; i++) {
            addQuery(env, i);
        }

        // Execute queries
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void addQuery(StreamExecutionEnvironment env, int queryInstance) {
        DataStream<String> messageStream = env.addSource(
                // Every source is a Kafka Consumer
                new FlinkKafkaConsumer<>(
                        // Different topics for different query queryInstances
                        KAFKA_PREFIX_TOPIC + "-" + queryInstance,
                        new SimpleStringSchema(),
                        setupParams.getProperties()).setStartFromEarliest())
                .name("Source (" + queryInstance + ")")
                // Chain all operators before a watermark is emitted.
                .disableChaining();

        messageStream
                // Parse the JSON string from Kafka as an ad
                .map(new DeserializeAdsFromkafka())
                .name("DeserializeInput (" + queryInstance + ")")
                .disableChaining()
                // Filter ads
                .filter(new FilterAds())
                //.<Tuple2<String, String>>project(2,5)
                // Assign timestamps and watermarks
                //.assignTimestampsAndWatermarks(new AdsWatermarkAndTimeStampAssigner())
                //.name("TimeStamp (" + queryInstance + ")")
                //.disableChaining()
                // perform join with redis data
                .map(new JoinAdWithRedis())
                .name("JoinWithRedis (" + queryInstance + ")")
                .disableChaining()
                // Collect aggregates in an event window
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(windowSize))).aggregate(new WindowAdsAggregator())
                .name("Window (" + queryInstance + ")")
                .disableChaining()
                // sink function
                .addSink(new PrintCampaignAdClicks())
                .name("Sink(" + queryInstance + ")")
                .disableChaining();
    }


    private class DeserializeAdsFromkafka implements MapFunction<String, Tuple9<String, String, String, String, String, String, String, String, String>> {

        @Override
        public Tuple9<String, String, String, String, String, String, String, String, String> map(String input) {
            JSONObject obj = new JSONObject(input);
            return new Tuple9<>(
                    obj.getString("user_id"),
                    obj.getString("page_id"),
                    obj.getString("ad_id"),
                    obj.getString("ad_type"),
                    obj.getString("event_type"),
                    obj.getString("event_time"),
                    obj.getString("ip_address"),
                    obj.getString("event_time"),
                    String.valueOf(System.currentTimeMillis())); // ingestion time
        }
    }

    private class FilterAds implements FilterFunction<Tuple9<String, String, String, String, String, String, String, String, String>> {

        @Override
        public boolean filter(Tuple9<String, String, String, String, String, String, String, String, String> ad) {
            return ad.getField(4).equals("view");
        }
    }

    private class AdsWatermarkAndTimeStampAssigner implements AssignerWithPunctuatedWatermarks<Tuple9<String, String, String, String, String, String, String, String, String>> {
        @Nullable
        @Override
        public Watermark checkAndGetNextWatermark(Tuple9<String, String, String, String, String, String, String, String, String> lastElement, long extractedTimestamp) {
            long watermarkTime = Long.valueOf(lastElement.getField(7));
            return watermarkTime != 0 ? new Watermark(watermarkTime) : null;
        }

        @Override
        public long extractTimestamp(Tuple9<String, String, String, String, String, String, String, String, String> element, long previousElementTimestamp) {
            return Long.valueOf(element.getField(5));
        }
    }

    private class JoinAdWithRedis extends RichMapFunction<Tuple9<String, String, String, String, String, String, String, String, String>, Tuple5<String, String, String, String, String>> {

        private RedisAdCampaignCache redisAdCampaignCache;

        @Override
        public void open(Configuration parameters) {
            //initialize jedis
            ParameterTool parameterTool = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
            this.redisAdCampaignCache = new RedisAdCampaignCache(new Jedis(parameterTool.get("jedis_server")));
        }

        @Override
        public Tuple5<String, String, String, String, String> map(Tuple9<String, String, String, String, String, String, String, String, String> input) {
            String userId = input.getField(0);
            String adId = input.getField(2);
            return new Tuple5<>(
                    redisAdCampaignCache.execute(adId),
                    userId + "," + adId,
                    input.getField(5), // event time
                    input.getField(7), // watermark
                    input.getField(8) // ingestion
            );
        }
    }

    private class NameToLowerCase extends RichMapFunction<Tuple5<String, String, String, String, String>, Tuple5<String, String, String, String, String>> {

        @Override
        public Tuple5<String, String, String, String, String> map(Tuple5<String, String, String, String, String> input) throws Exception {
            return new Tuple5<>(
                    input.f0,
                    input.f1,
                    input.f2, // event time
                    input.f3, // watermark
                    input.f4 // ingestion
            );
        }
    }

    private class WindowAdsAggregator implements AggregateFunction<Tuple5<String, String, String, String, String>, Tuple2<String, Integer>, Tuple2<String, Integer>> {

        @Override
        public Tuple2<String, Integer> createAccumulator() {
            return new Tuple2<>("", 0);
        }

        @Override
        public Tuple2<String, Integer> add(Tuple5<String, String, String, String, String> value, Tuple2<String, Integer> accumulator) {
            accumulator.f0 = value.f0;
            accumulator.f1 += 1;
            return accumulator;
        }

        @Override
        public Tuple2<String, Integer> getResult(Tuple2<String, Integer> accumulator) {
            return accumulator;
        }

        @Override
        public Tuple2<String, Integer> merge(Tuple2<String, Integer> acc0, Tuple2<String, Integer> acc1) {
            assert acc0.f0.equalsIgnoreCase(acc1.f0);
            return new Tuple2<>(acc0.f0, acc0.f1 + acc1.f1);
        }
    }

    private class PrintCampaignAdClicks implements SinkFunction<Tuple2<String, Integer>> {

        @Override
        public void invoke(Tuple2<String, Integer> value, Context context) {
        }
    }
}
