package YSB;

import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import org.json.JSONObject;
import redis.clients.jedis.Jedis;

import javax.annotation.Nullable;
import java.time.Duration;

public class AdvertisingQuery implements Runnable {

    /* The Kafka topic the source operators are pulling the results from */
    private final String KAFKA_PREFIX_TOPIC = "ad-events-1";
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
    private JSONObject js;

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

        WatermarkStrategy<String> wt = WatermarkStrategy.<String>noWatermarks()
                .withTimestampAssigner((event, timestamp) -> Long.parseLong(new JSONObject(event).getString("event_time")));

        DataStream<String> messageStream = env.addSource(
                // Every source is a Kafka Consumer
                new FlinkKafkaConsumer<>(
                        // Different topics for different query queryInstances
                        KAFKA_PREFIX_TOPIC,
                        new SimpleStringSchema(),
                        setupParams.getProperties()).assignTimestampsAndWatermarks(wt).setStartFromEarliest())
                .name("Source (" + queryInstance + ")").setParallelism(1);
                // Chain all operators before a watermark is emitted.
                //.disableChaining();

        messageStream
                // Parse the JSON string from Kafka as an ad
                .map(new DeserializeAdsFromkafka())
                .name("DeserializeInput (" + queryInstance + ")")
                .disableChaining()
                // Filter ads
                .filter(new FilterAds())
                .name("FilterAds (" + queryInstance + ")")
                //.disableChaining()
                .<Tuple5<String, String, String, Boolean, String>>project(2, 5, 7, 8, 9)
                .name("project (" + queryInstance + ")")
                //.disableChaining()
                // perform join with redis data
                .map(new JoinAdWithRedis())
                .name("JoinWithRedis (" + queryInstance + ")")
                //.disableChaining()
                //.keyBy(0)
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(windowSize)))
                .aggregate(new WindowAdsAggregator())
                .name("Window (" + queryInstance + ")")
                .writeAsText("results-ysb.txt", FileSystem.WriteMode.OVERWRITE);
                // sink function
                //.addSink(new PrintCampaignAdClicks())
                //.name("Sink(" + queryInstance + ")");
    }


    private class DeserializeAdsFromkafka implements MapFunction<String, Tuple10<String, String, String, String, String, String, String, String, Boolean, String>> {

        @Override
        public Tuple10<String, String, String, String, String, String, String, String, Boolean, String> map(String input) {
            JSONObject obj = new JSONObject(input);
            Boolean fake = false;
            if (obj.has("fake")){
                fake = true;
            }
            return new Tuple10<>(
                    obj.getString("user_id"),
                    obj.getString("page_id"),
                    obj.getString("ad_id"),
                    obj.getString("ad_type"),
                    obj.getString("event_type"),
                    obj.getString("event_time"),
                    obj.getString("ip_address"),
                    String.valueOf(System.currentTimeMillis()),
                    fake,
                    obj.getString("uniqueId")); // ingestion time

        }
    }

    private class FilterAds implements FilterFunction<Tuple10<String, String, String, String, String, String, String, String, Boolean, String>> {

        @Override
        public boolean filter(Tuple10<String, String, String, String, String, String, String, String, Boolean, String> ad) {
            return ad.getField(4).equals("view");
        }
    }

    private class JoinAdWithRedis extends RichMapFunction<Tuple5<String, String, String, Boolean, String>, Tuple5<String, String, String, Boolean, String>> {

        private RedisAdCampaignCache redisAdCampaignCache;

        @Override
        public void open(Configuration parameters) {
            //initialize jedis
            ParameterTool parameterTool = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
            this.redisAdCampaignCache = new RedisAdCampaignCache(new Jedis(parameterTool.get("jedis_server")));
        }

        @Override
        public Tuple5<String, String, String, Boolean, String> map(Tuple5<String, String, String, Boolean, String> input) {
            //String userId = input.getField(0);
            String adId = input.getField(0);
            return new Tuple5<>(
                    redisAdCampaignCache.execute(adId),
                    input.getField(1), // event time
                    input.getField(2), // ingestion time
                    input.getField(3), // fake?
                    input.getField(4) // uniqueId?
            );
        }
    }

    private class WindowAdsAggregator implements AggregateFunction<Tuple5<String, String, String, Boolean, String>, Tuple3<Long, Integer, Integer>, Tuple3<Long, Integer, Integer>> {

        @Override
        public Tuple3<Long, Integer, Integer> createAccumulator() {
            return new Tuple3<>((long)1, 0, 0);
        }

        @Override
        public Tuple3<Long, Integer, Integer> add(Tuple5<String, String, String, Boolean, String> value, Tuple3<Long, Integer, Integer> accumulator) {
            //windowSize
            accumulator.f0 = Long.parseLong(value.f1)/(3 * 1000);
            accumulator.f1 += 1;
            if (!value.f3){
                accumulator.f2 += 1;
            }
            return accumulator;
        }

        @Override
        public Tuple3<Long, Integer, Integer> getResult(Tuple3<Long, Integer, Integer> accumulator) {
            return accumulator;
        }

        @Override
        public Tuple3<Long, Integer, Integer> merge(Tuple3<Long, Integer, Integer> acc0, Tuple3<Long, Integer, Integer> acc1) {
            //assert acc0.f0.equals(acc1.f0);
            return new Tuple3<>(acc0.f0, acc0.f1 + acc1.f1, acc0.f2 + acc1.f2);
        }
    }

}
